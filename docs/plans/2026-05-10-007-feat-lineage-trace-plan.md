---
title: "feat: Lineage Trace — per-column upstream filter & join investigation view"
type: feat
status: draft
date: 2026-05-10
origin: grilling session 2026-05-10 (no requirements doc — interactive design)
revisions:
  - 2026-05-10: v0.7 scope reduction — U1 (statement_id) and U2 (PySpark predicate emission) deferred until fixture evidence demands them; see docs/brainstorms/2026-05-10-lineage-trace-plan-evaluation.md. v1 ships U3-U6 against existing parser output, grouping Trace Steps by source-table boundary.
---

# feat: Lineage Trace (v0.7)

## Overview

Business analysts investigating data issues currently leave the app to read SQL/PySpark by hand because the column inspector shows transforms but not the filters and joins that constrained a value. We add a multi-hop **Lineage Trace** rendered in the column inspector: clicking a column expands a tree of **Trace Steps**, each one a source-table-bounded group of writes plus the `WHERE` / `HAVING` / `QUALIFY` predicates and the `JOIN ... ON` keys that bound it. The trace walks the existing `raw_graph` upstream, collapses temp-view / CTE hops, and rolls their predicates onto the consuming step (annotated `via temp view <name>`).

Public-API responses for existing endpoints are byte-identical. The new endpoint is `GET /lineage/trace?table=X&column=Y`, returning the immediate Trace Step(s) plus the upstream column IDs needed to lazy-expand the tree. The frontend renders a collapsible tree, fetching one hop per expand. **Four implementation units; ships in 1–2 PRs.** No parser changes — v0.7 surfaces existing `__filter__`/`__joinkey__`/`__qualify__`/`__having__` synthetic-column edges that the SQL parser already emits.

---

## Problem Frame

The existing `ColumnInspector` shows, for a clicked column: the column's expression, a "Column Transformations" table (one row per source column with its transform type and expression), and source file + line. It does not show:

1. The `WHERE` / `HAVING` / `QUALIFY` predicates that filtered the rows the column was aggregated/computed over.
2. The `JOIN ... ON` keys that determined which rows survived the join.
3. The same information at upstream hops — when a value is NULL because an upstream view's `WHERE` dropped the row, the BA has to navigate one column at a time, expression-only, with no scope grouping.
4. Anything for PySpark `.filter()` / `.where()` / `.join(...)` calls — `parsers/pyspark.py:286-291` treats them as pass-through and never emits filter or join-key edges.

Items 1–3 are partly already in the data model: `parsers/sql.py:472-517` emits `__filter__`, `__joinkey__`, `__qualify__`, `__having__` synthetic-column edges. The graph view (`frontend/components/lineage-graph.tsx:155-242`) already renders them as ⚑/⚷ pseudo-columns. The inspector simply does not pull them in, because they target sibling synthetic columns rather than the real column being inspected.

End-user behaviour today: BA clicks `revenue.amount`, sees the SUM expression, leaves the app to grep the repo for "INSERT INTO revenue" to find the surrounding `WHERE`. Adding a Lineage Trace closes that loop in-app.

---

## Requirements Trace

- **~~R1.~~ (deferred)** ~~Add `statement_id: str` to `LineageEdge`.~~ Deferred to follow-up plan. v0.7 groups Trace Steps by source-table boundary using the existing `LineageEdge` shape. Trigger to revisit: a fixture or bug report where two distinct INSERT statements into the same target table produce a Trace Step that mixes their predicates incorrectly.
- **~~R2.~~ (deferred)** ~~PySpark parser emits `__filter__` and `__joinkey__` edges.~~ Deferred to follow-up plan. v0.7 surfaces PySpark JOIN keys (already partially captured via `_join_sources` in the existing parser path that emits joinkey edges through `spark.sql`) and is honest in the UI when no `.filter()` data is available. Trigger to revisit: a user-uploaded PySpark codebase where a `.filter()` / `.where()` call is missing from the Trace.
- **R3.** Engine exposes `lineage_trace(graph, raw_graph, table, column) -> list[TraceStep]`. Walks `raw_graph` upstream from the column's immediate writers; groups sibling edges by source-table boundary into Trace Steps; collapses temp-view / CTE writers and rolls their `__filter__`/`__joinkey__`/`__qualify__`/`__having__` edges onto the consuming Trace Step with `via_temp_views`.
- **R4.** New endpoint `GET /lineage/trace?table=X&column=Y` returns the immediate Trace Step(s) plus `upstream_columns` IDs for lazy expansion. 404 when the column does not exist in `lineage_graph`. Empty `steps: []` when the column is a source-table column with no writers.
- **R5.** Frontend `ColumnInspector` gains a Lineage Trace section. New `lineage-trace.tsx` renders the collapsible tree; React Query hook `useLineageTrace(column)` caches per-column responses; expanding a Trace Step's upstream chip fetches the next hop on demand.
- **R6.** Documentation: `docs/ARCHITECTURE.md` updated with the new endpoint and the per-column Trace pattern; `CLAUDE.md` route summary refreshed; `CONTEXT.md` and `docs/adr/0001-lineage-trace-walks-raw-graph.md` already in place.

---

## Scope Boundaries

- No changes to public response shapes for `/lineage`, `/lineage/paths`, `/tables/*`, `/impact`, `/search`, `/warnings`. `statement_id` is internal — never appears in their JSON.
- No source-file content cache and no inline source SQL block view — deferred to v2 (the inspector links to `file:line`; the BA opens the file in their editor for full context).
- No GROUP BY surfacing as a distinct Trace Step element — implicit in existing aggregation edges (`transform_type="aggregation"` with the `SUM(...)` expression). Defer to v2.
- No semantic / glossary search for "data discovery" — separate plan.
- No frontend changes outside the inspector and its supporting hooks/types.
- No state-shape changes beyond the `LineageEdge` field addition.

### Deferred to Follow-Up Work

- Inline source SQL block + `/sources/{id}/files/{path}?lines=42-78` endpoint with a per-source file-content cache (v2).
- GROUP BY rendered as its own Trace Step element (v2).
- Glossary mapping ("monthly_revenue" → `mart.revenue.amount`) for the discovery branch (separate plan).
- PySpark `.distinct()` / `.dropDuplicates()` / `.limit()` semantics in Trace Steps — current treatment is pass-through; revisit if BAs flag false-negative investigations.
- **Single-source CTE predicate visibility (discovered during U3 implementation 2026-05-10).** `parsers/sql.py::_resolve_ctes` routes single-source CTEs (the common pattern: `WITH x AS (SELECT … FROM t WHERE …)`) into `simple_map` as alias-only entries — their `WHERE` / `HAVING` / `QUALIFY` predicates are dropped before any edge is emitted. The naive fix (route CTEs-with-predicates through `multi_map`) doesn't reach `raw_graph`: `_parse_single_statement` calls `resolve_temp_views` internally, dropping the synthetic `__filter__` edges before `parse_sql` snapshots `raw_edges`. Real fix requires plumbing pre-resolution edges through the per-statement boundary, with chain-resolution updates so CTE references inside outer SELECTs still resolve to underlying physical tables. Out of scope for v0.7. **Effect on v1 BA experience:** Trace Steps for columns whose only filter lives in a single-source CTE will show writes + JOINs but no filter. Regular INSERT WHEREs (e.g. `orders_agg.sql::agg_revenue.__filter__`) and JOIN ON clauses are surfaced correctly. Trigger to ship the fix: a user reports a column whose Trace is missing a filter that exists in a CTE.

---

## Context & Research

### Relevant Code and Patterns

- `backend/lineage/models.py:52-65` — `LineageEdge` dataclass. R1 adds `statement_id: str = ""` here. The default makes the field back-compatible with edges constructed in tests that don't care about scoping.
- `backend/lineage/models.py:36-48` — `ColumnMeta`, the canonical "engine returns a typed result, route reshapes for HTTP" precedent that R3's `TraceStep` mirrors.
- `backend/lineage/engine.py:250-326` — `engine.trace_paths()`, the canonical engine-owned traversal. R3's `lineage_trace()` is its natural sibling.
- `backend/lineage/engine.py:147-201` — `build_graph_with_warnings()` returning `GraphResult`. New traversals consume `GraphResult.graph` and `GraphResult.raw_graph`.
- `backend/parsers/sql.py:438-517` — column lineage extraction including `__joinkey__` (line 472) and `_emit_predicate_edges` for `__filter__`/`__qualify__`/`__having__` (lines 515-517). R1 enriches these edges with `statement_id`. The SELECT/INSERT AST node carries `meta.line` for the start position.
- `backend/parsers/sql.py:1090-1097` — `resolve_temp_views()` drops every edge targeting a temp view. Drives the ADR-0001 decision: Trace traversal walks `raw_graph` (pre-resolution) to keep temp-view predicates.
- `backend/parsers/pyspark.py:96-300` — `_DataFrameTracker`. `visit_Assign` handles `.filter()` / `.where()` at lines 286-291 as pure pass-through today; R2 mutates this branch to record predicates. Edges emit at `visit_Expr` (line 302) when a `.write` is encountered — R2 hooks the predicate flush there.
- `backend/parsers/pyspark.py:243-275` — existing JOIN handling. `join_keys` are tracked as a list per variable; R2 will reuse the same per-variable accumulator pattern for filters.
- `backend/api/routes.py:233-238` — `list_columns` using `engine.column_metadata()`. R4's `/lineage/trace` endpoint follows the identical "engine returns dataclass, route reshapes via `_*_to_dict`" template.
- `backend/api/routes.py:243-271` — `/lineage`, `/lineage/paths`, `/impact`. R4's endpoint sits alongside; same column-id construction (`f"{table}.{column}"`).
- `backend/api/routes.py:169-176` — `_is_synthetic_table` helper. R3 uses an analogous synthetic-column predicate when filtering edges into the right Trace Step bucket (writes vs filters vs joins).
- `frontend/components/column-inspector.tsx:29-164` — the entire current inspector. R5 extends it: a new section after "Column Transformations" mounts `<LineageTrace colId={colId} />`.
- `frontend/components/lineage-graph.tsx:126-242` — existing handling of `__filter__` / `__joinkey__` pseudo-columns in the graph view. Useful prior art: the ⚑/⚷ glyphs and the toggle-to-hide pattern. R5 reuses these glyphs in Trace Step cards.
- `frontend/lib/api.ts`, `frontend/lib/hooks.ts` — typed fetch wrappers and React Query hooks. R5 adds `getLineageTrace()` and `useLineageTrace(table, column)`. No `invalidateLineageData()` change needed (Trace is read-only and refetches naturally on column change).
- `backend/state.py` — module-level globals. No new global needed; the trace is computed on demand from existing `raw_graph`.

### Institutional Learnings

- `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md` — §1 "Graph traversal belongs in `lineage/engine.py`, not in `api/routes.py`" mandates the placement of `lineage_trace()`. R3 extends the canonical-examples list (`upstream`, `downstream`, `trace_paths`, `column_metadata`, **`lineage_trace`**).
- `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md` — §3 "ParseResult is the parser return contract"; R1/R2 don't change the contract, only enrich the edges inside it. §4 "engine/parser dataclasses live in `lineage/models.py`; API dataclasses in `api/models.py`" — `TraceStep` is engine-layer (lives in `lineage/models.py`); the route shapes it via `_trace_step_to_dict` (no `dataclasses.asdict()` per the documented footgun).
- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md` — multi-predecessor expression aggregation pattern. R3's Trace Step assembly preserves all distinct predicate expressions sharing the same `statement_id`, deduped by `(expression, source_line)` to avoid double-counting predicates that survive the parser unchanged.
- `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md` — 4-part column ID. R3 uses `lineage.ids.split_column_id()` for every table/column extraction; never inline `rsplit`.
- `backend/AGENTS.md` — "Two graphs, kept in sync" (R3 reads `raw_graph` + `lineage_graph` consistently); "Column IDs are 4-part" (R3 honors); "Don't introduce new state globals" (no new global; Trace is computed on each request from existing graphs).
- `frontend/AGENTS.md` — "This is NOT the Next.js you know"; before adding the React Query hook in R5, read the current Next.js docs for any deprecation in `useQuery` integration patterns.
- `docs/adr/0001-lineage-trace-walks-raw-graph.md` — codifies the temp-view rollup decision driving R3.

### External References

External research not run. The local patterns are recent (2026-04-25, 2026-05-01) and the Lineage Graph / Raw Graph dual-graph contract is already documented in `docs/ARCHITECTURE.md`. The dataclass-vs-dict separation precedents (`ColumnMeta`, `GraphResult`, `StoredWarning`) cover every shape this plan introduces.

---

## Key Technical Decisions

- **`statement_id` is a structural identifier, not a UUID.** Format `{file}:{cell|0}:{start_line}`. Rationale: stable across re-parses of unchanged source (a SELECT at line 42 of foo.sql stays "foo.sql:0:42"); cheap to compute (already in the AST node `meta.line`); human-readable in debug output. Hashes and UUIDs were rejected: a fresh hash per parse would make Trace Steps non-cacheable across refreshes; a UUID on the parser side adds no value for this internal identity.
- **`statement_id: str = ""` not `Optional[str] = None`.** Empty string means "edge not assignable to a statement" (e.g., a synthetic cycle-detection warning). Trace Step assembly filters out empty-statement edges. `Optional[str]` would force every consumer to handle `None` for no upside.
- **Temp views collapse, predicates roll up.** See ADR-0001. The `via_temp_views` field on a `TraceStep` lists the names of every collapsed view whose predicates were merged in. Hard requirement for BA orientation: without the breadcrumb, they lose the connection back to the file.
- **Per-hop API, not full-tree.** `GET /lineage/trace?table=X&column=Y` returns one column's Trace Step(s) plus `upstream_columns` IDs. Frontend recurses on expand. Bounds response size; aligns with React Query's natural per-key caching; identical request shape regardless of depth.
- **Multi-writer columns return multiple Trace Steps in the same response.** When `revenue.amount` is written by two distinct statements, the single API call returns both. The UI branches the tree at that node. Alternative — pick one as "primary" — would silently hide the others.
- **Write-statement scoping is per `statement_id`, not per file.** Two SELECTs in the same file get distinct Trace Steps; one SELECT spanning many lines stays one Trace Step. Mirrors the SQL/PySpark source structure rather than the file system.
- **PySpark `.filter()` predicates are captured via `ast.unparse()`.** The predicate AST node is unparsed to a Python source string (`"F.col('status') == F.lit('paid')"`). Faithful enough for BA inspection; lossless round-trip is not a goal. Where a predicate is a plain string (`df.where("status = 'paid'")`), the literal is kept as-is.
- **PySpark predicate accumulator lives on `_DataFrameTracker`, keyed by variable name.** `self._filters: dict[str, list[tuple[str, int]]]` mirrors the existing `self._join_sources` pattern. Propagates across `.select()` / `.withColumn()` / `.join()` / etc. Flushed at `.write` along with column edges.
- **`TraceStep` lives in `lineage/models.py`.** Engine-layer dataclass. Route reshapes via `_trace_step_to_dict`, never `dataclasses.asdict()` (per the documented `architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md` §4 footgun).
- **Per-unit verification on a representative fixture.** `sample_data/sample_lineage.zip` plus a new fixture (`sample_data/lineage_trace_fixture.zip` — to be added in U3) covering: multi-writer column, temp view rollup, JOIN-only step, source-table column with no writers, PySpark write with `.filter()` and `.join()`. Each unit asserts byte-identical responses for unchanged endpoints, plus structural assertions for the new endpoint.

---

## Open Questions

### Resolved During Grilling

- **Filter scoping** (per-write-statement vs per-file vs all-table): per-write-statement. Drives R1's `statement_id` data model.
- **Trace depth** (immediate write vs multi-hop vs configurable): multi-hop, all upstream. Drives R3's recursive walk and R5's tree UI.
- **Statement identity** (new field vs positional tuple vs re-parse): new `statement_id: str` field on `LineageEdge`. Drives R1/R2.
- **Temp view rendering** (collapse with rollup vs render vs hide): collapse with rollup. Codified in ADR-0001.
- **PySpark statement boundary** (write-call vs per-assignment vs defer): bound at `.write` / `.saveAsTable` / `.insertInto`. Drives R2's accumulator-and-flush pattern.
- **API granularity** (per-hop vs full tree to depth N vs unbounded): per-hop. Drives R4's response shape.
- **Inline source SQL block** (v1 vs deferred): deferred. Snippets + jump-to-`file:line` for v1.
- **Terminology** (Lineage Trace / Trace Step vs Investigation vs Provenance): Lineage Trace / Trace Step. Codified in `CONTEXT.md`.

### Deferred to Implementation

- **Whether `TraceStep.kind` is `"sql" | "pyspark"` derived from the source-file extension, or `"sql_select" | "sql_insert" | "pyspark_write" | "pyspark_sql"` reflecting the AST construct.** The latter is more useful for UI iconography; the former is cheaper. Pick at U3 implementation time based on what the inspector will actually render.
- **Whether `upstream_columns` on a Trace Step deduplicates synthetic columns.** A `__filter__` predicate that references `staging.transactions.status` should produce an `upstream_columns` entry for `staging.transactions.status` (so the BA can click into that column's trace), but never an entry for `staging.transactions.__filter__` (synthetic columns are not user-clickable). R3 must filter synthetics out at assembly time. Verify with a fixture.
- **Whether two `__filter__` edges in the same statement that produce identical `(expression, source_line)` tuples are deduped at the engine layer or the route layer.** Either is correct; pick the one that keeps the engine signature cleaner.
- **Whether the React Query cache key for `useLineageTrace(table, column)` invalidates on source register/refresh/delete.** It must — `frontend/lib/hooks.ts::invalidateLineageData()` already invalidates several lineage queries; add this one to that list. Verify in U5.
- **Whether Trace Step rendering shows the column-write expressions inline or only on demand.** A statement that writes 30 columns shouldn't dump 30 expressions when the BA is investigating one. Default: show only the traced column's expression; "show all writes" expander reveals the rest. Decide at U5 based on the live render.

---

## Implementation Units

> v0.7 ships U3, U4, U5, U6 (four units, 1–2 PRs). U1 and U2 are documented below for posterity but **deferred** — see Requirements Trace for the triggers that would unblock them. Skip directly to U3 unless one of those triggers fires.

### Deferred (kept for traceability)

- ~~U1.~~ **`statement_id` on `LineageEdge` + SQL parser populates (R1)**

**Goal:** Add `statement_id: str = ""` to `LineageEdge`. Update the SQL parser so every emitted edge carries the statement identifier of the SELECT/INSERT/CTE/spark-sql node that produced it.

**Requirements:** R1

**Dependencies:** None. Lands first.

**Files:**
- Modify: `backend/lineage/models.py:52-65` — add `statement_id: str = ""` to `LineageEdge`. Document the format `{file}:{cell|0}:{start_line}` in the docstring.
- Modify: `backend/parsers/sql.py` — at every `LineageEdge(...)` construction site (line 472 for `__joinkey__`, the predicate emit helpers near 515-517, the column-edge emit sites in `_emit_column_edges` and adjacent), thread the statement's start line through and assign `statement_id=f"{source_file}:{source_cell or 0}:{statement_start_line}"`. The statement start line comes from the AST node's `meta.line` (sqlglot's `Expression.meta`). Add a small helper `_make_statement_id(source_file, source_cell, node)` to centralize the format.
- Modify: `backend/parsers/sql.py::_emit_predicate_edges` and the JOIN handler — accept the SELECT node and derive `statement_id` once per statement, pass to every edge construction.
- Modify: `backend/tests/test_sql_parser.py` — add a focused test `test_statement_id_grouping` that parses two SELECTs in one file and asserts (a) all edges from the first SELECT share one `statement_id`, (b) edges from the second SELECT share a different one, (c) `__filter__` and `__joinkey__` edges from the same SELECT share its `statement_id`. No existing tests should break — the new field has a default and is additive.

**Approach:**
- Define the helper `_make_statement_id(source_file, source_cell, statement_node)` first; verify it produces stable output for the same input across re-parses.
- Walk every SELECT-handling code path in `parsers/sql.py` and thread the statement node through. Most paths already receive `select_node` or equivalent — pass `_make_statement_id(...)` result as `statement_id=` kwarg to each `LineageEdge(...)` call.
- For CTEs that get parsed as virtual subqueries (line 927-944 area), derive the `statement_id` from the CTE's AST node, not the outer SELECT. CTE filters belong to the CTE's scope.
- For `spark.sql("...")` calls invoked from `parsers/pyspark.py` via `_parse_sql`, the `source_line` argument already carries the SQL string's location in the Python file; reuse it as the SELECT's start line.
- Run the full test suite. Existing tests should pass without modification — the field is additive with a safe default.

**Patterns to follow:**
- Existing field additions to `LineageEdge` (e.g., `qualified: bool = True` already in the dataclass) — same default-value, additive style.
- `parsers/sql.py:_emit_predicate_edges` signature precedent — accept the parent node, derive contextual fields from it.

**Test scenarios:**
- Happy path: two SELECTs in one file produce edges with two distinct `statement_id`s. CTE filters belong to the CTE's `statement_id`, not the outer SELECT.
- Happy path: `__filter__`, `__joinkey__`, `__qualify__`, `__having__` edges from a single SELECT all share that SELECT's `statement_id` with the column-write edges.
- Happy path: a `spark.sql("SELECT ...")` invocation inside a `.py` file produces edges whose `statement_id` is `{py_file}:0:{spark_sql_call_line}` — i.e., the Python location, not a synthetic SQL-only identifier.
- Edge case: a temp view CREATE produces edges whose `statement_id` differs from the consumer SELECT that reads it.
- Integration: every parser test in `tests/test_sql_parser.py` passes unchanged — the new field is additive.

**Verification:**
- `python -m pytest tests/` from `backend/` is green.
- `grep -nE "LineageEdge\(\s*$|LineageEdge\([^)]*statement_id=" backend/parsers/sql.py` — every multi-line `LineageEdge(...)` block sets `statement_id`. (Visual scan; or use a stricter test: parse a fixture and assert no edge has `statement_id == ""`.)
- New test `tests/test_sql_parser.py::test_statement_id_grouping` passes.

---

- ~~U2.~~ **PySpark parser emits `__filter__` and `__joinkey__` edges + populates `statement_id` (R2)**

**Goal:** Stop dropping `.filter()` / `.where()` / `.join()` predicates on the floor. Accumulate predicate text per DataFrame variable through the AST walk; flush as `__filter__` and `__joinkey__` edges at the `.write` call, with `statement_id` set to the write call's line.

**Requirements:** R2

**Dependencies:** U1 — relies on the `statement_id` field existing on `LineageEdge`.

**Files:**
- Modify: `backend/parsers/pyspark.py:96-103` — add two per-variable accumulators on `_DataFrameTracker.__init__`: `self._filters: dict[str, list[tuple[str, int]]]` (variable → [(predicate_text, lineno), ...]) and `self._joins: dict[str, list[tuple[str, str, int, list[str]]]]` (variable → [(left_expr, right_expr, lineno, joined_tables), ...]). The existing `self._join_sources` stays — it tracks which tables are joined; the new accumulator tracks the ON-clause text and source columns referenced.
- Modify: `backend/parsers/pyspark.py:286-291` — replace the pass-through branch for `("filter", "where")` with: capture `value.args[0]` (the predicate AST node), unparse it via `ast.unparse(value.args[0])`, append `(predicate_text, node.lineno)` to `self._filters[var]`. Inherit existing parent filters: `self._filters[var] = list(self._filters.get(src_var, [])) + [...]`. Other operations in the tuple (`dropDuplicates`, `drop`, `limit`, `orderBy`, `sort`, `distinct`, `repartition`, `cache`) remain pass-through but propagate the existing `_filters`/`_joins` accumulators.
- Modify: `backend/parsers/pyspark.py:243-275` — when capturing `join_keys`, also record the ON-clause expression text + the joined tables onto `self._joins[var]`. Inherit through subsequent transforms via `_propagate_join_sources`-style copy.
- Modify: `backend/parsers/pyspark.py:302-359` (`visit_Expr`) — at the write boundary, after emitting the column edges, also emit `__filter__` and `__joinkey__` edges:
  - For each `(predicate_text, lineno)` in `self._filters.get(src_var, [])`: emit one `LineageEdge` per source column referenced in the predicate (use a heuristic: parse the predicate string for `F.col("name")` / `df["name"]` patterns; fall back to a single edge from `f"{src_table}.__filter_expr__"` if column extraction fails). `target_col=f"{target_table}.__filter__"`, `transform_type="filter"`, `expression=predicate_text`, `statement_id=f"{source_file}:0:{call_lineno}"`.
  - For each join entry in `self._joins.get(src_var, [])`: emit `LineageEdge` for each ON column on each side. `target_col=f"{target_table}.__joinkey__"`, `transform_type="join_key"`, `expression=on_expression_text`, `statement_id=f"{source_file}:0:{call_lineno}"`.
  - All real-column edges emitted at the write also get `statement_id=f"{source_file}:0:{call_lineno}"` (uniform per-write statement_id).
- Modify: `backend/tests/test_pyspark_parser.py` — add tests:
  - `test_pyspark_filter_emits_filter_edges`: `df.read.table("a").filter(F.col("x") > 5).write.saveAsTable("b")` produces a `b.__filter__` edge with `transform_type="filter"` and the predicate text in `expression`.
  - `test_pyspark_join_emits_joinkey_edges`: `df1.join(df2, "id").write.saveAsTable("b")` produces `b.__joinkey__` edges from both sides.
  - `test_pyspark_filter_chain_inherits_through_select`: filter at line 4, select at line 5, write at line 7 → filter edge survives, statement_id is the write line.
  - `test_pyspark_statement_id_uniform_per_write`: every edge from one write has the same `statement_id`.

**Approach:**
- Add the two accumulators and a `_propagate_filters_joins(target_var, source_var)` helper that mirrors `_propagate_join_sources`. Call it from every transform branch that currently calls `_propagate_join_sources`.
- Refactor the write-boundary section into two phases: phase 1 emits column edges (existing logic, now with `statement_id`); phase 2 emits the new `__filter__` / `__joinkey__` edges.
- The "extract source columns from predicate text" heuristic is intentionally simple. If a predicate doesn't yield any source columns, emit a single edge from a fallback synthetic source (`f"{src_table}.__predicate_{lineno}__"`) so the predicate text is still surfaced in the Trace; or skip the edge and emit a parser warning. Pick at implementation time.
- Run the existing PySpark suite first; nothing should break (the new branches add edges, never remove). Add the new tests after.

**Patterns to follow:**
- Existing `self._join_sources` accumulator (line 102) — same shape, parallel field.
- `parsers/sql.py:_emit_predicate_edges` — pattern of "extract column references from a predicate, emit one edge per reference, share the predicate text on every edge."
- U1's `_make_statement_id` helper — reuse for the `f"{source_file}:0:{call_lineno}"` construction.

**Test scenarios:**
- Happy path: `df.filter(F.col("status") == F.lit("paid")).write.saveAsTable("revenue")` produces a `revenue.__filter__` edge with the predicate text in `expression` and `transform_type="filter"`.
- Happy path: `df1.join(df2, "id").write.saveAsTable("merged")` produces `merged.__joinkey__` edges from both `df1.id` and `df2.id`.
- Happy path: a chain of `.filter()` → `.select()` → `.filter()` → `.write` produces two `__filter__` edges, both with `statement_id` set to the write call's line.
- Edge case: `.where("status = 'paid'")` (string-form predicate) produces a `__filter__` edge whose `expression` is the literal string, with `transform_type="filter"`.
- Edge case: a write whose source DataFrame has no filters and no joins produces zero `__filter__` / `__joinkey__` edges (no false positives).
- Edge case: `dropDuplicates` / `distinct` / `limit` — currently pass-through; do not emit `__filter__` edges in v1. (Documented in Scope Boundaries deferral.)
- Integration: the existing PySpark suite passes unchanged.

**Verification:**
- `python -m pytest tests/test_pyspark_parser.py` is green, including the four new tests.
- `python -m pytest tests/` from `backend/` is green.
- `grep -nE "transform_type=\"(filter|join_key)\"" backend/parsers/pyspark.py` returns at least two matches (was zero before).

---

### Active units (v0.7)

- U3. **`engine.lineage_trace()` + `TraceStep` dataclass (R3)**

**Goal:** Add `engine.lineage_trace(graph, raw_graph, table, column) -> list[TraceStep]` returning the immediate Trace Step(s) for one column. Walks `raw_graph`, groups edges by source-table boundary, separates writes from filter/join synthetics, collapses temp views / CTEs, and rolls up their predicates onto the consuming step with `via_temp_views`. **No parser changes.**

**Requirements:** R3

**Dependencies:** None for v0.7. Reads existing `raw_graph` content emitted by `parsers/sql.py`. Honest about PySpark `.filter()` gap in the UI (handled in U5).

**Files:**
- Modify: `backend/lineage/models.py` — add `TraceStep` dataclass next to `ColumnMeta`. Fields:
  - `kind: Literal["sql", "pyspark"]` — derived from `source_file` extension at assembly time
  - `source_file: str`
  - `source_cell: int | None`
  - `source_line: int` — line of the immediate write that produced this step (min of writes' source_lines)
  - `target_table: str` — the table the writes land on (or temp-view name)
  - `writes: list[TraceStepWrite]` — `(column_id, expression, transform_type, source_line)`
  - `filters: list[TraceStepPredicate]` — `(kind: "where"|"having"|"qualify", expression, source_columns: list[str], source_line)`
  - `joins: list[TraceStepJoin]` — `(expression, source_columns: list[str], source_line)`
  - `via_temp_views: list[str]` — names of CTEs / temp views whose predicates were rolled up
  - `upstream_columns: list[str]` — deduped real-column source IDs (synthetics excluded; self-references excluded)
- Modify: `backend/lineage/engine.py` — add `lineage_trace(graph: nx.DiGraph, raw_graph: nx.DiGraph, table: str, column: str, max_steps: int = 50) -> list[TraceStep]`. Algorithm:
  1. Build `col_id = f"{table}.{column}"`. If `col_id` not in `graph`, return `[]` (route raises 404).
  2. Find immediate-writer edges in `raw_graph`: every `(u, v, data)` where `v == col_id`. If none, return `[]` (source-table column).
  3. Group writer edges by **source-table boundary**: `(source_table, source_file)`. Each group becomes one candidate Trace Step.
  4. For each group, fetch the synthetic-column edges that target the same writer's target table (`{target_table}.__filter__|__qualify__|__having__|__joinkey__`). Partition into `filters` and `joins`.
  5. For each writer's `source_col` whose source table is a temp-view-or-CTE node (detected by the helper `_is_temp_view_node`): recursively pull that node's synthetic edges and append them to the consuming step's `filters`/`joins`; append the view name to `via_temp_views`. Walk through chains of temp views (cap depth = 16 to avoid unbounded recursion).
  6. Compute `upstream_columns`: deduped real-column source IDs from the step's `writes` (synthetics filtered via `_is_synthetic_column`; self-references excluded).
  7. Return up to `max_steps` `TraceStep`s, sorted by `(source_file, source_line)`.
- Modify: `backend/lineage/engine.py` — export `lineage_trace` (mirroring how `column_metadata`, `trace_paths`, `upstream`, `downstream` are exported).
- Test: `backend/tests/test_engine.py` — add `test_lineage_trace_*` cases:
  - `test_lineage_trace_immediate_step_with_filter`
  - `test_lineage_trace_multi_writer_returns_multiple_steps` (hand-built nx.DiGraph)
  - `test_lineage_trace_cte_filter_rolls_up_with_via_annotation` (uses real `finance_mart.sql` fixture)
  - `test_lineage_trace_source_column_returns_empty`
  - `test_lineage_trace_unknown_column_returns_empty`
  - `test_lineage_trace_upstream_columns_excludes_synthetics`
  - `test_lineage_trace_join_step_has_joinkey` (uses `finance_mart.sql`)
  - `test_lineage_trace_max_steps_truncation`

**Approach:**
- Define `TraceStep` and sub-dataclasses first.
- Implement `lineage_trace` and drive it against integration tests using `parse_sql` on the real fixture files (`sample_data/orders_agg.sql`, `sample_data/finance_mart.sql`). The CTE rollup is the load-bearing case — write that test first.
- Use `lineage.ids.split_column_id` for every column-id parse. Never inline `rsplit`.
- Iterate `raw_graph.edges(data=True)` once per call, bucketing into `dict[target_col, list[edge]]` and `dict[source_col, list[edge]]` indexes. Avoid repeated graph scans.
- Detect temp-view-or-CTE nodes: a table name `t` is "temp-view-like" if `t` appears as the table portion of an edge's `target_col` AND `t` is also the source table of some other edge AND `t` is not in the resolved `lineage_graph` as a "real" terminal table. Simpler heuristic: any non-terminal table reachable in `raw_graph` whose name does not appear as a top-level INSERT target. Pin the detection in a private helper `_is_temp_view_node(raw_graph, table_name)` so it can be replaced if it proves too coarse.

**Patterns to follow:**
- `engine.column_metadata()` — typed return, route reshapes.
- `engine.trace_paths()` — BFS-style walk over a graph with synthetic-column awareness. Reuse the truncation discipline (don't return more than N paths; document the limit).
- `_remove_source_files` (`api/routes.py:33-48`) — extracted-helper template.

**Test scenarios:**
- Happy path: column written by one SELECT with one `WHERE` (`orders_agg.sql::agg_revenue.total_revenue`). Single TraceStep with one `filter`, zero `joins`, six `writes` entries (the SELECT writes six columns), `upstream_columns` populated.
- Happy path: column written from a CTE chain whose intermediate CTE has a `WHERE` (`finance_mart.sql::mart_finance.revenue`). Single TraceStep with the JOIN keys, plus the CTE's `total_revenue > 0` rolled up; `via_temp_views` includes `customer_revenue`.
- Edge case: multi-step temp-view chain (hand-built fixture using `CREATE TEMP VIEW` — NOT CTEs, since single-source CTEs lose their predicates upstream of `raw_graph`). All collapsed views' filters roll up; `via_temp_views` lists each.
- Edge case: column with zero writers (a source-table column like `raw_orders.amount`). Returns `[]`.
- Edge case: column not in `lineage_graph`. Returns `[]` (route translates to 404).
- Edge case: a write whose source column references a synthetic column. The synthetic must NOT appear in `upstream_columns`.
- Edge case: `max_steps=1` truncates a 3-writer column to one step deterministically (sorted by `(source_file, source_line)`).

**Verification:**
- `python -m pytest tests/test_engine.py` is green, including the new `test_lineage_trace_*` cases.
- `python -m pytest tests/` from `backend/` is green.
- `grep -nE "for .* in .*\.edges\(data=True\)" backend/api/routes.py` does not increase (the new traversal stays in the engine).

---

- U4. **`GET /lineage/trace` endpoint (R4)**

**Goal:** Surface `engine.lineage_trace()` over HTTP. Reshape `TraceStep` into a JSON-friendly dict; return 404 on unknown column; return `{steps: []}` for source-table columns with no writers.

**Requirements:** R4

**Dependencies:** U3.

**Files:**
- Modify: `backend/api/routes.py` — add `_trace_step_to_dict(step: TraceStep) -> dict` helper next to `_column_meta_to_dict`. Explicit field-by-field shape — never `dataclasses.asdict()` (per `architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md` §4 footgun).
- Modify: `backend/api/routes.py` — add the route:
  ```py
  @router.get("/lineage/trace")
  def get_lineage_trace(table: str, column: str):
      col_id = f"{table}.{column}"
      if col_id not in state.lineage_graph:
          raise HTTPException(status_code=404, detail=f"Column '{col_id}' not found")
      steps = engine.lineage_trace(state.lineage_graph, state.raw_graph, table, column)
      return {"target": col_id, "steps": [_trace_step_to_dict(s) for s in steps]}
  ```
- Test: `backend/tests/test_routes.py` — add HTTP-level tests for each U3 scenario, plus:
  - `test_lineage_trace_unknown_column_returns_404`
  - `test_lineage_trace_source_column_returns_empty_steps`
  - `test_lineage_trace_existing_endpoints_unchanged`: parses a fixture, calls `/lineage`, `/lineage/paths`, `/tables`, `/tables/{t}/columns`, `/impact`, `/search`, `/warnings`. Asserts none of those response bodies contain `statement_id`, `__filter__`, `__joinkey__`, `__qualify__`, `__having__`, `via_temp_views`, or other Trace-only field names. Smaller and more durable than full-body snapshots.

**Approach:**
- Wire the route as the last endpoint in the `Lineage / Impact` section of `routes.py`. Mirror the existing `get_lineage` shape.
- The 404 check must reference `state.lineage_graph` (resolved), not `raw_graph`, so the externally-visible "this column exists" contract stays consistent with `/lineage`.
- The `_trace_step_to_dict` helper is the only place `TraceStep` shapes into JSON. If a follow-up wants different shapes (e.g. an MCP variant), it adds a sibling helper, not a parameter.

**Patterns to follow:**
- `_column_meta_to_dict` (lines 218-230) — exact template for "engine dataclass → API dict" reshaping.
- `get_lineage` / `get_impact` — same column-id construction, same response envelope structure.

**Test scenarios:**
- Happy path: GET `/lineage/trace?table=mart_finance&column=revenue` returns 200 with a `steps` array. Each step has the documented shape (`kind`, `source_file`, `source_cell`, `source_line`, `target_table`, `writes`, `filters`, `joins`, `via_temp_views`, `upstream_columns`).
- Edge case: GET on an unknown column returns 404 with a descriptive message.
- Edge case: GET on a source-table column returns 200 with `steps: []`.
- Integration: existing endpoints contain no Trace-only field names in their response bodies.

**Verification:**
- `python -m pytest tests/test_routes.py` is green.
- `python -m pytest tests/` from `backend/` is green.
- Manual: hit the new endpoint via curl against `sample_data/sample_lineage.zip` and confirm the shape.
- The targeted "no Trace-only fields in existing endpoints" test passes.

---

- U5. **Frontend Lineage Trace tree in the column inspector (R5)**

**Goal:** Extend `ColumnInspector` with a Lineage Trace section that renders the lazy-expanded tree. Each Trace Step is a card showing filters, joins, and the columns it wrote; expanding a step's upstream chip fetches the next hop on demand.

**Requirements:** R5

**Dependencies:** U4 (the endpoint must exist).

**Files:**
- Modify: `frontend/lib/api.ts` — add the typed fetch wrapper:
  ```ts
  export async function getLineageTrace(table: string, column: string): Promise<LineageTraceResponse> { ... }
  ```
  Plus the `LineageTrace`, `TraceStep`, `TraceStepWrite`, `TraceStepPredicate`, `TraceStepJoin` types matching the backend JSON shape.
- Modify: `frontend/lib/hooks.ts` — add `useLineageTrace(table, column)` React Query hook. Cache key: `["lineage-trace", table, column]`. Add the trace key to `invalidateLineageData()` so source register/refresh/delete clears stale traces.
- Add: `frontend/components/lineage-trace.tsx` — the renderer. Top-level component takes `colId`. Loads the column's immediate Trace Step(s). Each step is a card with three sub-sections (Writes — collapsed by default unless the traced column is the only write; Filters; Joins) plus an "Upstream" footer with one chip per `upstream_columns` entry. Clicking a chip expands an inline child `<LineageTrace colId={upstreamColId} />`. Default-expand the topmost step; deeper steps stay collapsed until the user opens them.
- Modify: `frontend/components/column-inspector.tsx` — mount `<LineageTrace colId={colId} />` after the existing "Column Transformations" section. Section heading: "Lineage Trace". Don't render at all when `colId` is null (the existing empty state covers this).
- Modify: `frontend/components/column-inspector.tsx` — add a small interactive `file:line` chip that links to the source file (no inline view in v1; just visually communicates jumpability — actual jumping is out-of-app).

**Approach:**
- Build the API + hook + types first; verify with the React Query devtools that one click = one network call.
- Build `lineage-trace.tsx` bottom-up: a `<TraceStepCard step={...} />` that takes one step and renders all three sub-sections. Then a recursive `<LineageTrace colId={...} />` that fetches and renders all steps for one column, with an "expand upstream" UI.
- Default-collapsed deep hops, default-expanded immediate write. Visual nesting via indent or a vertical guide line; cap left-indent so deep chains stay readable.
- Render filter and join glyphs (⚑ and ⚷) consistent with `lineage-graph.tsx`.
- Loading and error states: per-card skeleton on expand, an error chip with retry on fetch failure. Empty state for source-table columns: "Source column — end of trace."
- Accessibility: the expand chip is a `<button>`, focus-ringed. The card is an `<article>` with semantic section headings. Keyboard: Enter/Space toggles expand.
- Read `node_modules/next/dist/docs/` before adding any new file — the AGENTS.md note in `frontend/` is explicit that the Next.js shape may differ from training data.
- Verify the live UX: `npm run dev`, click a known column with known filters, expand at least three hops, and confirm the trace renders end-to-end without jank.

**Patterns to follow:**
- `frontend/components/column-inspector.tsx` — section structure (`<section>` with uppercase tracking-widest label), syntax highlighting for SQL via `react-syntax-highlighter`.
- `frontend/components/lineage-tree.tsx` — column ID handling, table-vs-column split via `splitColumnId`.
- `frontend/components/lineage-graph.tsx:230-242` — synthetic-column glyphs (⚑/⚷); reuse the pattern for filter/join Trace Step badges.
- `frontend/lib/hooks.ts::invalidateLineageData()` — invalidation pattern for cross-cutting cache resets after source mutations.

**Test scenarios:**
- Manual happy path: register `sample_data/sample_lineage.zip`, click a column with a known WHERE clause, see the filter rendered in the Lineage Trace card with file:line.
- Manual happy path: a column with two writers shows two Trace Step cards under one column; both expand independently.
- Manual edge case: a column whose lineage passes through a temp view shows the rolled-up filter with the `via temp view <name>` annotation.
- Manual edge case: clicking an upstream chip on a deep hop (3+ levels) loads only that branch's data; sibling branches stay unloaded.
- Manual edge case: source-table column shows the empty state ("Source column — end of trace.")
- Visual: the section reads cleanly on a dense column (10+ filters/joins) without overwhelming the existing transformations table above it.
- Build verification: `npm run build` and `npm run lint` from `frontend/` are green.

**Verification:**
- `npm run build` and `npm run lint` from `frontend/` are green.
- Manual interaction in the browser against `sample_data/sample_lineage.zip` covers each happy-path and edge-case scenario above.
- React Query devtools: one column click triggers one `/lineage/trace?table=...&column=...` request; expanding an upstream chip triggers exactly one additional request; no waterfall on initial render.

---

- U6. **Documentation updates (R6)**

**Goal:** Surface the new architecture and endpoint in the standing reference docs.

**Requirements:** R6

**Dependencies:** U1–U5 lands first (so doc claims match shipped behaviour).

**Files:**
- Modify: `docs/ARCHITECTURE.md` — add to "Key invariants": **Statement ID is structural and stable** (`{file}:{cell|0}:{start_line}`). Add to the REST-surface table: `GET /lineage/trace`. Add to "Two graphs, not one": a sentence noting that filters in temp views are visible only via `/lineage/trace` because the resolved Lineage Graph drops them.
- Modify: `CLAUDE.md` — append `/lineage/trace` to the route summary in the Backend section. Add a one-line mention of the Trace pattern under Backend Architecture: "Lineage Trace (`engine.lineage_trace`) walks `raw_graph` to build per-column Trace Steps; collapses temp views with predicate rollup."
- Append: `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md` — under §1 "Existing examples of the correct pattern", add `engine.lineage_trace`. The doc explicitly maintains this list.
- Verify: `CONTEXT.md` (already written 2026-05-10) and `docs/adr/0001-lineage-trace-walks-raw-graph.md` (already written 2026-05-10) match the shipped behaviour. Update if anything diverged during implementation.

**Approach:**
- Doc updates land in the same PR as the unit they describe, or in a final cleanup PR if the work split makes that cleaner.
- Cross-check that every term in `CONTEXT.md` matches its usage in the shipped code. If `TraceStep.kind` ends up as `"sql_select" | "sql_insert" | ...` (per the deferred-to-implementation question), update the CONTEXT entry for **Statement** to call out the four kinds.

**Test scenarios:** N/A (documentation).

**Verification:**
- `grep -n "/lineage/trace" docs/ARCHITECTURE.md CLAUDE.md` finds the new entry in both.
- `CONTEXT.md` reads consistently against the final shipped names.
- The ADR-0001 rationale still matches what the code does.

---

## System-Wide Impact

- **Interaction graph:** `lineage_trace` is called only from `routes.get_lineage_trace` (production) and `tests/test_engine.py`. `statement_id` is read by `engine.lineage_trace`; written by every `LineageEdge` constructor in `parsers/sql.py` and `parsers/pyspark.py`. The frontend's `useLineageTrace` is consumed only by the new `lineage-trace.tsx`. No mutation of `state.lineage_graph` / `state.raw_graph`.
- **Error propagation:** Unchanged. `lineage_trace` returns `[]` for unknown / source columns; the route translates the unknown case to 404 via `lineage_graph` membership. No new `HTTPException` shapes.
- **State lifecycle risks:** No new state globals. The Trace is computed per-request from existing `raw_graph` / `lineage_graph`. `state.parse_warnings.clear()` etc. are unaffected. Test reset hooks need no changes.
- **API surface parity:** Hard requirement. `/lineage`, `/lineage/paths`, `/tables/*`, `/impact`, `/search`, `/warnings`, `/sources/*` are byte-identical pre/post-merge — `statement_id` does not appear in any of their JSON responses. U4's snapshot test pins this.
- **Frontend coupling:** The new section sits inside `ColumnInspector`. No changes to navigation, no new pages. `invalidateLineageData()` gains the trace key so source register/refresh/delete clears stale traces.
- **Performance:** Each Trace request walks `raw_graph` once and groups edges by `statement_id`. For a typical column on `sample_data/sample_lineage.zip`, the response is small (1–10 steps). Per-hop API design bounds the worst case. No frontend pre-fetch — every fetch is user-initiated.
- **Unchanged invariants:**
  - `lineage_graph` ↔ `raw_graph` parallel mutation (`backend/AGENTS.md`). Trace reads both; mutates neither.
  - 4-part column ID. Every column-id parse uses `split_column_id`.
  - `ParseResult` / `ParseWarning` / `GraphResult` / `StoredWarning` / `ColumnMeta` shapes are unchanged.
  - Module-level state in `state.py` stays as module-level globals.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `statement_id` accidentally leaks into existing endpoint responses (e.g., `/lineage` graph nodes pick it up via NetworkX edge data). | U4's byte-identicality snapshot test covers `/lineage`, `/lineage/paths`, `/tables/*`, `/impact`, `/search`, `/warnings` against a recorded baseline. |
| The PySpark predicate-text extraction heuristic loses meaningful column references (e.g., `F.col("status")` works but a wrapped `F.expr("status")` does not). | Document the heuristic limits in U2; emit a parser warning for predicates that yield zero source columns; the predicate text is preserved in `expression` even if column attribution is partial. |
| Temp-view rollup over a deep chain (4+ views) produces a Trace Step with too many `via_temp_views` to render cleanly. | UI truncates `via_temp_views` to first 3 + "… and N more" when rendering. The full list stays in the API response. |
| Multi-writer columns surprise BAs by branching the tree on what they thought was a single column. | The first time the tree branches, render an info badge ("Two writers — both scopes shown"). Rare in practice but explicit on first encounter. |
| Statement boundaries differ between SQL CTEs and INSERT ... SELECT. | U1 derives `statement_id` from the AST node that physically wrote the column — the CTE definition for CTE writes, the outer SELECT for the consuming SELECT. Test covers both cases. |
| PySpark filters propagated through `.select()` get attached to the wrong write when a single tracker handles multiple writes. | U2's accumulator is keyed by variable name; each write reads only its own source variable's accumulators. The existing `_join_sources` follows the same pattern and has no observed bugs. |
| Frontend lazy expand creates an N+1 render storm if a user expands every node. | React Query caches per `(table, column)` key, so expanding the same column twice hits the cache. Document this; do not optimize prematurely. |
| Snapshot drift breaks the byte-identicality test on legitimate, unrelated future changes. | Snapshot file is committed to the repo with a clear comment ("DO NOT regenerate without intent"). Updates require a deliberate `--update-snapshots` invocation. |

---

## Documentation / Operational Notes

- After landing, run `/ce-compound` to capture: (a) the per-variable predicate accumulator pattern in `_DataFrameTracker`, (b) the temp-view rollup discipline (predicates roll up on collapse; via_temp_views is load-bearing for orientation, not cosmetic).
- No rollout, monitoring, or feature-flag concerns. In-memory backend; new endpoint and field land and work on next deploy. Users re-upload data on each Railway redeploy regardless (per `CLAUDE.md`), so no migration of stored state.
- No frontend coordination beyond this plan. The new endpoint is additive; nothing else consumes `/lineage/trace`.
- Future v2 (file-content cache + inline SQL block view): the `source_file:source_line` already on every Trace Step is sufficient — the v2 work is purely additive and won't require a Trace Step shape change.

---

## Sources & References

- **Origin:** grilling session 2026-05-10 (no requirements doc — interactive design dialogue captured in this plan and `CONTEXT.md`).
- **ADR:** `docs/adr/0001-lineage-trace-walks-raw-graph.md`.
- **Glossary:** `CONTEXT.md` (Lineage Trace, Trace Step, Statement, Statement ID, Synthetic Column, Lineage Graph, Raw Graph).
- Related code:
  - `backend/lineage/models.py:36-65, 90-105` (`ColumnMeta`, `LineageEdge`, `GraphResult`)
  - `backend/lineage/engine.py:147-326` (`build_graph_with_warnings`, `column_metadata`, `trace_paths`)
  - `backend/parsers/sql.py:438-517, 1090-1097` (column lineage, predicate emit, temp-view resolver)
  - `backend/parsers/pyspark.py:96-359` (`_DataFrameTracker`)
  - `backend/api/routes.py:218-271` (`_column_meta_to_dict`, `list_columns`, `get_lineage`, `get_impact`)
  - `frontend/components/column-inspector.tsx`, `frontend/components/lineage-graph.tsx:126-242`, `frontend/lib/hooks.ts`
- Related institutional learnings:
  - `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md`
  - `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md`
  - `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md`
  - `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md`
- Repo conventions: `backend/AGENTS.md`, `frontend/AGENTS.md`, `CLAUDE.md`, `.claude/CLAUDE.md`.
