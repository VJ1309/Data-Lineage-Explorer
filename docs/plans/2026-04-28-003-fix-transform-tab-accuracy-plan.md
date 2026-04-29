---
title: "fix: Repair temp-view expression inheritance and wildcard over-expansion"
type: fix
status: active
date: 2026-04-28
origin: docs/brainstorms/2026-04-28-transform-tab-accuracy-requirements.md
---

# fix: Repair temp-view expression inheritance and wildcard over-expansion

## Overview

Two bugs in `backend/parsers/sql.py` cause the Transform tab and lineage tree to show inaccurate data for columns in `uc_dc_dev.scru_em.ship_ordr_mlstn`:

1. **Expression inheritance (Bug 1):** When `_resolve_temp_views` collapses a chain of temp view hops into a direct `LineageEdge`, it always copies `transform_type` and `expression` from the consumer (final-hop) edge — discarding any richer intermediate expressions (COALESCE, GROUP BY, window functions). The resolved edge in `lineage_graph` ends up with a trivial `SELECT * FROM last_view` rather than the meaningful transformation.

2. **Wildcard over-expansion (Bug 2):** The `_lookup()` wildcard fallback fires when a named temp-view column has no exact entry in `tv_sources` (e.g., `src_match_rnk.mlstn_event_cd`). It looks up the matching wildcard entry (`tv_sources["src_match_rnk.*"]`) and returns all its sources. After the chain-resolution loop expands that wildcard entry from `["src_match_rnk.*"]` to named columns like `["delv_line.mlstn_event_cd", "delv_line.src_sys_cd"]`, the fallback returns all of them for any column lookup — creating incorrect cross-column edges (`delv_line.src_sys_cd → ship_ordr_mlstn.mlstn_event_cd`). Empirical testing confirmed this failure mode produces wrong source columns, not missing source columns.

Both fixes are scoped to `_resolve_temp_views` and `_lookup` in `backend/parsers/sql.py`.

---

## Problem Frame

The lineage tree's inline expression display (`lineage-tree.tsx:94`) and the catalog column drill-down (`routes.py:320`) read `expression` from resolved `LineageEdge` objects in `lineage_graph`. These edges are produced by `_resolve_temp_views()` in `parsers/sql.py`, which collapses multi-hop temp-view chains into direct source→target edges.

The current implementation discards intermediate expressions during chain collapse and over-expands wildcard resolution after chain-reduction loop rewrites. Both errors are silent — no exceptions, wrong data, correct-looking counts — exactly the class of bug documented in `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md`.

> **Note on brainstorm hypothesis:** The origin document hypothesized that a `_parse_select_node` bug in the `not col_refs` branch caused `SELECT TABLE.*` to emit wrong wildcard edges. Research disproved this: SQLGlot parses `SELECT ML_FM_LM_UPDT.*` as `Column(this=Star(), table='ML_FM_LM_UPDT')`, which has a non-empty `col_refs` list, so the `for col_ref in col_refs:` loop correctly emits `ml_fm_lm_updt.* → target.*`. No fix is needed in `_parse_select_node`. (see origin: docs/brainstorms/2026-04-28-transform-tab-accuracy-requirements.md)

---

## Requirements Trace

- R1. Resolved `LineageEdge` objects in `lineage_graph` carry the most semantically meaningful `(transform_type, expression)` found anywhere in the temp-view chain, not the consumer-hop expression.
- R2. Named-column lookups through a wildcard chain produce exactly one resolved source per column — the correct matching column, never cross-column edges.
- R3. No regressions in the existing backend test suite (`python -m pytest tests/ -v`).
- R4. Columns with already-correct non-passthrough expressions (aggregations, windows, etc.) are unaffected.

---

## Scope Boundaries

- Fix is scoped to `backend/parsers/sql.py` — `_resolve_temp_views` and `_lookup`.
- No frontend changes required; `lineage-tree.tsx` and `column-inspector.tsx` already render whatever `expression` they receive.
- No changes to `_parse_select_node`, `_classify_transform`, or the `LineageEdge` data model.
- PySpark parser is out of scope.
- Re-introducing the raw-graph path view in the Transform tab is out of scope (separate initiative).

---

## Context & Research

### Relevant Code and Patterns

- `backend/parsers/sql.py:695–840` — `_resolve_temp_views()`: Phase 1 builds `tv_sources: dict[str, list[str]]`, Phase 2 runs the chain-resolution loop, Phase 3 emits resolved edges inheriting consumer `transform_type`/`expression`.
- `backend/parsers/sql.py:727–761` — `_lookup()`: exact-match path, wildcard collect path, named-column fallback path (lines 754–761 are the Bug 2 site).
- `backend/parsers/sql.py:35–59` — `_classify_transform()`: implicit transform priority — `window > cast > aggregation > expression > passthrough`. `_best_expression` should follow this order.
- `backend/lineage/engine.py:127` — `build_graph_with_warnings()`: `raw_graph` stores pre-resolution edges (all intermediate expressions intact); `lineage_graph` stores resolved edges (where Bug 1 manifests).
- `backend/tests/test_sql_parser.py:179, 200, 491, 514` — existing temp-view resolution tests (no expression assertions; no over-expansion case).
- `frontend/components/lineage-tree.tsx:94` — consumes `node.edge.expression` from `lineage_graph`.

### Institutional Learnings

- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md` — direct structural predecessor: when collapsing multi-hop chains, fields that vary per edge must be aggregated/scored across all hops, not read from a single representative. The analogous fix here: score expressions from all edges in the chain, not just copy from the consumer. Also: use ordered-deduplication patterns (`list` not `set`) to preserve determinism.

### External References

- None required — local patterns are sufficient.

---

## Key Technical Decisions

- **Reverse `edge_lookup` dict (not modifying `tv_sources`):** Build a `dict[str, list[LineageEdge]]` mapping `target_col.lower() → [edges]` from the pre-resolution edge list at the top of `_resolve_temp_views`. This keeps `tv_sources` and the chain-resolution loop unchanged; only the emission step (Phase 3) gains access to intermediate expressions. Changing `tv_sources` to carry tuples would require updating both `_lookup`'s return type and the Phase 2 loop — higher blast radius.

- **`_best_expression` as a local helper inside `_resolve_temp_views`:** The helper walks from a consumer temp-view column back through `edge_lookup`, collecting `(transform_type, expression)` pairs until it reaches a non-temp-view source. It returns the highest-priority pair using `_classify_transform`'s order: `window > cast > aggregation > expression > passthrough`. Among ties, the expression closest to the original source (deepest in the chain) is preferred — that is where meaningful transformations like COALESCE typically appear. Falls back to the consumer edge's expression if the entire chain is passthrough. **Wildcard-aware lookup:** since pre-resolution edges use wildcard target keys (e.g., `src_match_rnk.*` not `src_match_rnk.mlstn_event_cd`), the walk must check both `edge_lookup[col]` (exact) and `edge_lookup[tbl + ".*"]` (wildcard base) at each step — mirroring `_lookup`'s two-stage fallback.

- **`_lookup` named-column filter (2-line change):** In the wildcard fallback path (`sql.py:754–761`), add a filter before returning: when a source from `wildcard_sources` does NOT end in `.*` (i.e., it is a named column), only include it if its column name matches the requested `col_name`. Sources ending in `.*` are still handled by the existing substitution logic. This preserves all existing wildcard-to-wildcard expansion behavior.

- **No change to `_parse_select_node`:** Research confirmed table-qualified stars (`SELECT TABLE.*`) parse correctly and enter the `col_refs` loop, not the `not col_refs` branch. The brainstorm hypothesis is wrong; no parser fix is needed.

---

## Open Questions

### Resolved During Planning

- **Was `_parse_select_node` a bug site for `SELECT TABLE.*`?** No — planning-phase research traced the code path: SQLGlot parses `SELECT T.*` as `Column(this=Star(), table='T')`, which has a non-empty `col_refs` list (the `Column` node itself is found by `find_all(exp.Column)`), so the `for col_ref in col_refs:` loop is entered and the wildcard edge `t.* → target.*` is emitted correctly. The brainstorm listed this as a "likely cause" to investigate — investigation disproved it. The real Bug 2 root cause is entirely in `_lookup`'s named-column fallback.
- **Does the chain walk need to handle cycles?** No — temp views are defined as a DAG by construction (SQLGlot would error on circular CTEs); the iteration is bounded.
- **Should `_best_expression` use raw_graph?** No — `raw_graph` is not available inside `parsers/sql.py`. The `edge_lookup` reverse dict built from the pre-resolution `edges` list already contains all intermediate expressions.

### Deferred to Implementation

- **Exact iteration strategy in `_best_expression`:** Whether to implement as depth-first traversal or to collect all edges by iterating the full `edge_lookup` for each temp-view column in the chain — determine at implementation time based on what is clearest.
- **Whether the `_best_expression` walk needs to handle multi-predecessor chains (many sources converging into one temp view column):** This case should be straightforward (pick best across all), but confirm the iteration handles it correctly.
- **Whether any `mlstn_event_cd`-style columns have no wildcard entry at all in `tv_sources` (not just over-expanded):** Empirical testing confirmed the over-expansion failure mode. However, the brainstorm also describes the symptom as "temp view name left as source," which could indicate a second failure mode where `_lookup` returns `None` (no exact AND no wildcard entry). If U1 does not fully resolve the `mlstn_event_cd` symptom after deployment, this warrants a follow-up investigation of the specific CTE chain in the notebook.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
Pre-resolution edges (edges list)
       │
       ├──► tv_sources: dict[str, list[str]]          (existing — chain resolution, unchanged)
       │         └── Phase 2 loop rewrites tv_sources
       │
       └──► edge_lookup: dict[str, list[LineageEdge]]  (NEW — built once at start of _resolve_temp_views)
                 └── keyed by target_col.lower()

Phase 3 — emit resolved edges:
  for each consumer edge e where e.source_col is a temp view:
      upstream_sources = _lookup(e.source_col)     ← Bug 2 fix: filter named sources by col_name
      for upstream_col in upstream_sources:
          best = _best_expression(               ← Bug 1 fix: walk edge_lookup chain
                     start=e.source_col,
                     edge_lookup=edge_lookup,
                     temp_views=temp_views_lower,
                     fallback=(e.transform_type, e.expression)
                 )
          emit LineageEdge(source_col=upstream_col, target_col=e.target_col,
                           transform_type=best.type, expression=best.expr, ...)
```

```
_best_expression walk (conceptual):
  current = start_col (e.g., "src_match_rnk.mlstn_event_cd" — a temp view col)
  candidates = []
  visited = set()
  while current in temp_views and current not in visited:
      visited.add(current)
      tbl = current.rsplit(".", 1)[0]
      # Wildcard-aware: pre-resolution edges use wildcard target keys (e.g. "src_match_rnk.*"),
      # not named column keys, so check both exact and wildcard base.
      edges_to_current = edge_lookup.get(current) or edge_lookup.get(tbl + ".*") or []
      candidates.extend([(e.transform_type, e.expression) for e in edges_to_current])
      if edges_to_current:
          current = edges_to_current[0].source_col   (follow one path up; multi-predecessor: collect all)
      else:
          break
  return highest_priority(candidates) or fallback
```

---

## Implementation Units

- U1. **Fix `_lookup` wildcard over-expansion (Bug 2)**

**Goal:** Named-column lookups through a wildcard chain return only the source whose column name matches, preventing cross-column edges.

**Requirements:** R2, R3, R4

**Dependencies:** None

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

**Approach:**
- In `_lookup()` at the named-column fallback path (the `return [...]` list comprehension around line 758), add a filter clause: when a source entry does NOT end in `.*`, only include it if `source_col.rsplit(".", 1)[-1].lower() == col_name.lower()`.
- Sources that DO end in `.*` continue to use the existing `s[:-1] + col_name` substitution — no change.
- This is the only code change for Bug 2. No change to `tv_sources` building or the chain-resolution loop.

**Execution note:** Write a characterization test that demonstrates the over-expansion failure before applying the fix — confirm the test fails, then apply the one-line filter change and confirm it passes.

**Patterns to follow:**
- `backend/parsers/sql.py:727–761` — existing `_lookup` wildcard substitution logic; add filter alongside, do not replace.

**Test scenarios:**
- Happy path: Wildcard chain where `tv_sources["tv1.*"] = ["base.col_a", "base.col_b"]` (named columns after chain expansion). `_lookup("tv1.col_a")` returns `["base.col_a"]` only — not `["base.col_a", "base.col_b"]`.
- Happy path: Same wildcard chain. `_lookup("tv1.col_b")` returns `["base.col_b"]` only.
- Edge case: `tv_sources["tv1.*"] = ["base2.*"]` (still a wildcard source). `_lookup("tv1.col_a")` returns `["base2.col_a"]` — existing substitution behavior preserved.
- Edge case: Mixed wildcard entry `["base2.*", "base.col_a"]`. `_lookup("tv1.col_a")` returns `["base2.col_a", "base.col_a"]` — wildcard expands, named column matches.
- Integration: Multi-cell Databricks notebook with `SELECT TV1.*` pass-through — resolved edges for `mlstn_event_cd` and `src_sys_cd` each resolve to exactly one base-table source column, not two.
- Regression: All existing `test_temp_view_wildcard_*` tests continue to pass.

**Verification:**
- `_lookup("tv1.col_a")` given a mixed-name wildcard entry returns exactly the sources whose column names match `col_a`.
- No cross-column edges appear in a resolved graph where a wildcard chain contains multiple named columns.
- `python -m pytest tests/test_sql_parser.py -v` passes.

---

- U2. **Add `_best_expression` helper and fix expression inheritance (Bug 1)**

**Goal:** Resolved `LineageEdge` objects in `lineage_graph` carry the most semantically meaningful expression from the full temp-view chain, not the consumer-hop expression.

**Requirements:** R1, R3, R4

**Dependencies:** None (independent of U1, though both may land in the same commit)

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

**Approach:**
- At the start of `_resolve_temp_views`, build `edge_lookup: dict[str, list[LineageEdge]]` — a reverse mapping from `target_col.lower()` to all edges that produce that column. Build from the same `edges` input list used to build `tv_sources`. This dict does NOT change during the chain-resolution loop. Keys are the original target columns as they appear in pre-resolution edges — for wildcard SELECT edges these keys will be `tbl.*`, not named columns.
- Add a `_best_expression` helper (local or module-level) that accepts: a starting temp-view column, `edge_lookup`, `temp_views_lower`, and a fallback `(transform_type, expression)` tuple. It walks backward through `edge_lookup` collecting `(transform_type, expression)` from all edges until it exits the temp-view set. **Wildcard-aware:** at each step, check `edge_lookup[current]` first; if empty, fall back to `edge_lookup[tbl + ".*"]` (where `tbl = current.rsplit(".", 1)[0]`). This mirrors `_lookup`'s two-stage strategy. Include a `visited` set to guard against revisiting. Returns the highest-priority pair using `_classify_transform`'s order: `window > cast > aggregation > expression > passthrough`. Ties prefer the entry closest to the original source (deepest).
- In Phase 3 of `_resolve_temp_views` (the emission loop, ~lines 784–838), replace the current `transform_type=e.transform_type, expression=e.expression` with the result of `_best_expression(e.source_col, edge_lookup, temp_views_lower, fallback=(e.transform_type, e.expression))`.
- Edges where `e.source_col` is NOT a temp view (i.e., already a direct source) are unaffected — `_best_expression` returns the fallback immediately.

**Patterns to follow:**
- `backend/parsers/sql.py:35–59` — `_classify_transform()` priority order: `window > cast > aggregation > expression > passthrough`.
- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md` — collect all, score, pick one; never blindly inherit from one representative edge.
- `backend/parsers/sql.py:108–117` — `_resolve_ctes` iterative chain loop pattern (same max_iterations guard).

**Test scenarios:**
- Happy path: Two-hop chain — `base.src_sys_cd → tv1.src_sys_cd [COALESCE expression, transform_type="expression"] → final.src_sys_cd [SELECT * FROM tv1, transform_type="passthrough"]`. Resolved edge `base.src_sys_cd → final.src_sys_cd` should have `transform_type="expression"` and `expression` containing `COALESCE`.
- Happy path: Intermediate hop has `transform_type="aggregation"` (GROUP BY). Resolved edge carries the aggregation expression, not the consumer passthrough.
- Happy path: Intermediate hop has `transform_type="window"`. `window` beats `aggregation` — resolved edge carries the window expression.
- Edge case: All hops in chain are passthrough. Resolved edge carries the consumer's passthrough expression (unchanged behavior).
- Edge case: Consumer hop has `transform_type="expression"` (non-passthrough) and intermediate hop is passthrough. Resolved edge carries the consumer's non-passthrough expression.
- Edge case: Consumer hop has `transform_type="aggregation"` and intermediate hop has `transform_type="window"`. `window` wins — resolved edge carries window expression from the intermediate hop.
- Regression: Edges where `source_col` is already a real base-table column (not a temp view) are emitted with their original `transform_type` and `expression` unchanged.
- Covers AE-equivalent: Clicking `ship_ordr_mlstn.src_sys_cd` — the `src_sys_cd` column has a COALESCE at the intermediate CTE level; resolved edge `expression` should contain `COALESCE`, not `SELECT * FROM SRC_MATCH_RNK`.

**Verification:**
- A resolved edge through a chain containing a COALESCE intermediate carries `expression` with COALESCE content, not `SELECT * FROM view`.
- A resolved edge through an all-passthrough chain carries the consumer's expression (unchanged).
- Existing edges with non-passthrough `transform_type` on the consumer hop are unaffected (do not silently downgrade to passthrough).
- `python -m pytest tests/test_sql_parser.py -v` passes.

---

## System-Wide Impact

- **Interaction graph:** Only `_resolve_temp_views` and `_lookup` are modified. All callers of `parse_sql()` receive the same `LineageEdge` structure — only the `expression` and `transform_type` field values change on edges that pass through temp-view chains.
- **Error propagation:** No new exception paths. `_best_expression` receives a fallback and returns it if no chain is found — gracefully degrades to current behavior.
- **State lifecycle risks:** `edge_lookup` is a function-local dict, built and discarded within `_resolve_temp_views`. No persistent state changes.
- **API surface parity:** `/tables/{table}/columns` (catalog drill-down) and `/lineage` (tree) both consume `lineage_graph` edges — both will benefit automatically from the expression fix. `/lineage/paths` uses `raw_graph` (unaffected).
- **Integration coverage:** The full parse → graph build → API → frontend chain is the integration test; the unit tests in `test_sql_parser.py` cover parser output. Manual verification against the deployed `ship_ordr_mlstn` data is the end-to-end check.
- **Unchanged invariants:** `LineageEdge` data model fields are unchanged. `tv_sources` build and chain-resolution loop are unchanged. `_parse_select_node` is unchanged. The `raw_graph` / `lineage_graph` split in `engine.py` is unchanged.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `_best_expression` walk loops infinitely on a temp-view cycle | Temp views form a DAG by construction; the `visited` guard in the walk prevents revisiting regardless |
| Chain walk visits a column with no `edge_lookup` entry (e.g., column from a `CREATE TABLE AS SELECT` that was not captured as a temp view edge) | Fallback to consumer expression when `edge_lookup[col]` is empty or missing |
| Bug 2 filter hides a legitimate multi-source wildcard result (e.g., a column that genuinely comes from two base tables via UNION) | Only the named-column filter is new; wildcard-to-wildcard expansion is unchanged. UNION-sourced columns produce multiple edges with matching column names, so all are returned correctly |
| Existing `test_temp_view_wildcard_named_chain` passes but over-expansion was masked by test structure | Write new over-expansion regression test first, verify it fails before fix, then apply fix (characterization-first posture for U1) |

---

## Documentation / Operational Notes

- No API contract changes — `LineageEdge` field names and types are unchanged; only values improve.
- Manual smoke test after deploy: open the live app, upload the `uc_dc_dev.scru_em` source, click `ship_ordr_mlstn.src_sys_cd` in the lineage graph → verify Transform tab and tree show COALESCE expression. Click `ship_ordr_mlstn.mlstn_event_cd` → verify source is a real base-table column, not `src_match_rnk.mlstn_event_cd`.

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-28-transform-tab-accuracy-requirements.md](docs/brainstorms/2026-04-28-transform-tab-accuracy-requirements.md)
- Related code: `backend/parsers/sql.py:695–840` (`_resolve_temp_views`), `backend/parsers/sql.py:727–761` (`_lookup`)
- Related learning: `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md`
- Frontend consumption: `frontend/components/lineage-tree.tsx:94`
