---
title: "refactor: Backend Code Quality — Column ID Helper, ParseResult, Dispatch Separation, SourceEntry"
type: refactor
status: active
date: 2026-04-30
origin: docs/brainstorms/2026-04-30-backend-code-quality-refactors-requirements.md
---

# refactor: Backend Code Quality — Column ID Helper, ParseResult, Dispatch Separation, SourceEntry

## Overview

Four internal refactors to reduce duplication and improve clarity in the DataLineage Explorer backend and frontend. No API surface or behavioral changes. Each unit is independently atomic.

1. **Column ID helper** — extract a shared `split_column_id` (backend) and `splitColumnId` (frontend) utility to replace 12+ inline split patterns that are a documented bug source.
2. **Dispatch separation** — remove Databricks `.sql` notebook format detection from `parse_sql()` and move it to `engine._parse_file()`, where format concerns already live.
3. **Structured parser output** — replace `_warnings` and `_raw_out` mutable output parameters across all three parsers (`parse_sql`, `parse_notebook`, `parse_pyspark`) with a `ParseResult` dataclass return.
4. **SourceEntry dataclass** — replace plain `dict` source metadata (with underscore-prefixed private keys filtered by string matching) with a typed `SourceEntry` dataclass.

---

## Problem Frame

The DataLineage Explorer has accumulated four structural problems that raise the cost of future changes:

- Column ID splitting is implemented inline at 10+ backend sites and 4 frontend components. CLAUDE.md documents this as a known bug source; a past incident traced a UI bug to exactly this pattern.
- `parse_sql()` detects the Databricks `.sql` notebook format, splits on `-- COMMAND ----------`, and recursively calls itself — format-dispatch responsibilities that belong in the engine layer.
- Parsers receive pre-allocated mutable lists as output channels (`_warnings`, `_raw_out`), hiding side effects from callers and making unit testing awkward.
- Source metadata lives in plain dicts with `_underscore` private keys filtered out by string pattern at every read site.

None of these affect end-user behavior. All carry cost on every future change.

(see origin: `docs/brainstorms/2026-04-30-backend-code-quality-refactors-requirements.md`)

---

## Requirements Trace

- R1. `split_column_id(col_id)` backend helper using `rsplit(".", 1)`. All backend split sites use it.
- R2. `splitColumnId(id)` frontend helper in `lib/utils.ts`. All frontend split sites use it.
- R3. No inline split patterns remain for column-ID splitting.
- R4. `parse_sql()` does not detect `-- COMMAND ----------` or call itself recursively.
- R5. Format detection and cell dispatch moved to `engine._parse_file()`.
- R6. `_resolve_views=False` remains usable when dispatching cells from the engine.
- R7. `parse_sql()` and `parse_notebook()` no longer accept `_warnings` or `_raw_out` mutable parameters. Return `ParseResult`.
- R8. `engine._parse_file()` collects warnings and raw edges from `ParseResult`, not from pre-allocated lists.
- R9. All call sites updated accordingly.
- R10. Source metadata stored as `SourceEntry` dataclass with explicit public and private fields.
- R11. `SourceEntry` exposes a `to_public_dict()` method. Manual `startswith("_")` filter replaced.
- R12. `state.source_registry` typed as `dict[str, SourceEntry]`. All routes use typed fields.

---

## Scope Boundaries

- No behavioral or API surface changes — same edges, same response shapes, same error handling.
- `parse_pyspark` is included in the `ParseResult` migration (see Key Technical Decisions) even though R7 does not name it explicitly, because `engine._parse_file` must treat all three parsers symmetrically.
- Frontend scope expanded to all 4 components with inline splitting: `lineage-graph.tsx`, `lineage-tree.tsx`, `column-inspector.tsx`, and `transform-inspector.tsx`. Partial migration would leave the documented bug source intact.
- `_parse_copy`, `_parse_merge`, `_parse_command_fallback`, `_process_subquery` — private helpers that currently receive `_warnings` — keep threading a local list owned by `parse_sql`. Their internal signatures are not changed; the public interface change is what matters.
- `SourceConfig` in `backend/lineage/models.py` is dead code (confirmed: unused since commit `38ae74c`). Removal is a separate cleanup; do not reuse it for `SourceEntry`.
- Global state encapsulation (`AppState` class) is explicitly deferred and not in scope.
- New tests are not required as deliverables, but two existing warning-collection tests must be updated to read from `ParseResult.warnings` after U4.
- The pre-existing `test_routes.py:reset_state` gap has been fixed: route tests now reset `state.raw_graph` along with `source_registry`, `lineage_graph`, and `parse_warnings`.

---

## Context & Research

### Relevant Code and Patterns

- `backend/lineage/models.py` — all domain dataclasses (`FileRecord`, `LineageEdge`, `ParseWarning`, etc.) using `@dataclass` and `from __future__ import annotations`. Canonical pattern for `ParseResult` and `SourceEntry`.
- `backend/parsers/sql.py:1089` — `parse_sql()` current signature with `_warnings`/`_raw_out`/`_resolve_views`.
- `backend/parsers/sql.py:1060–1064` — `_DATABRICKS_SQL_SEP` constant and `_split_databricks_sql()` private function to promote.
- `backend/parsers/sql.py:1106–1114` — detection + split + recursive-call block to remove in U3.
- `backend/lineage/engine.py:10–38` — `_parse_file()` current implementation; receives `raw_edges_out` mutable list.
- `backend/api/routes.py:66–71` — `list_sources()` with manual `startswith("_")` filter.
- `backend/api/routes.py:82–105` — source dict construction with private keys (`_token`, `_records`, `_parsed_files`, `_file_stats`, `_error_files`).
- `frontend/lib/utils.ts` — exists, exports only `cn()`. Right home for `splitColumnId`.
- `frontend/components/lineage-graph.tsx:33`, `lineage-tree.tsx:8` — local `splitColumnId` helpers to replace with shared import.
- `frontend/components/column-inspector.tsx:51,120`, `transform-inspector.tsx:49` — inline `lastIndexOf(".")` sites to replace.

### Institutional Learnings

- `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md` — documented bug from inline splitting. Explicitly recommends extracting a shared `splitColumnId` utility. **Test with 4-part names only** — 2-part names pass even with the wrong `split(".", 1)` implementation.
- `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md` — three-tier function placement rule. Databricks format detection belongs in the engine layer; `parse_sql` should only parse SQL grammar.
- `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md` — `lineage_graph` and `raw_graph` must always be mutated together. `ParseResult.raw_edges` feeds `raw_graph`; preserve this invariant in U4.
- `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md` — `_warnings` appears in internal helpers; `ParseResult` is the natural structured return type.

---

## Key Technical Decisions

- **`split_column_id` location (backend):** New module `backend/lineage/ids.py`. Single-purpose; importable from both `engine.py` and `routes.py` without circular imports. Keeps `models.py` focused on domain types.
- **`splitColumnId` location (frontend):** `frontend/lib/utils.ts`. Extends the existing shared utility file; no new file needed.
- **`ParseResult` type:** `@dataclass` (not `NamedTuple`), consistent with `models.py` conventions. Fields: `edges: list[LineageEdge]`, `raw_edges: list[LineageEdge]`, `warnings: list[str]`. PySpark returns `raw_edges=[]`.
- **`parse_pyspark` included in U4:** `engine._parse_file` calls all three parsers in a uniform loop; leaving pyspark on the old mutable-list contract would require a conditional branch. Include for symmetry.
- **`_split_databricks_sql` promotion:** Rename to `split_databricks_sql` (drop underscore) and export from `parsers/sql.py` so `engine._parse_file` can import it without relying on private symbols. Rename `_DATABRICKS_SQL_SEP` → `DATABRICKS_SQL_SEP` consistently.
- **`SourceEntry` location:** New `backend/api/models.py`. Source entry is an API-layer concern, not a domain lineage type — it does not belong in `lineage/models.py`.
- **`SourceEntry` field naming:** Drop the `_` prefix from private fields (they are dataclass fields, not dict keys). Names: `token`, `records`, `parsed_files`, `file_stats`, `error_files`. Privacy is enforced by `to_public_dict()`, not by naming convention.
- **Inline rsplit sites to exclude:** The ~12 `rsplit(".", 1)` calls inside `_resolve_temp_views` and `_best_expression` in `parsers/sql.py` operate on internal lowercased strings within resolver logic — they are not column-ID splits and should not be migrated. `ingestion/upload.py:20` uses rsplit for file extension extraction — also excluded.

---

## Open Questions

### Resolved During Planning

- **Should `parse_pyspark` be included in ParseResult?** Yes — see Key Technical Decisions.
- **How many frontend components to migrate?** All 4 with inline splitting, not just the 2 named in requirements.
- **Does `ParseResult` need a `raw_edges` field?** Yes — `engine._parse_file` currently captures pre-resolution edges via `_raw_out` to populate `state.raw_graph`; this must survive the migration.
- **Can `engine.py` import private symbols from `sql.py`?** No — `_split_databricks_sql` must be promoted to a public name before U3.

### Deferred to Implementation

- **Exact `ParseResult` field defaults:** `field(default_factory=list)` for `raw_edges` and `warnings`, or explicit init args — implementer picks whichever is cleaner.
- **Whether `raw_edges` defaults to `None` or `[]` for pyspark:** Either is fine; choose the cleaner path in `_parse_file`.
- **`test_routes.py:reset_state` raw_graph gap:** Resolved. Route tests reset `state.raw_graph` before each test.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**ParseResult shape (U4):**

```
ParseResult
  .edges:     list[LineageEdge]   # post-resolution edges (or pyspark edges)
  .raw_edges: list[LineageEdge]   # pre-resolution edges ([] for pyspark)
  .warnings:  list[str]
```

**`engine._parse_file` flow after U3 + U4:**

```
_parse_file(record):
  if record.type == "sql":
    cells = split_databricks_sql(record.content)   # returns [content] if no separator
    per-cell: result = parse_sql(cell, ..., source_cell=idx, _resolve_views=False)
    collect all cells' edges, raw_edges, warnings
    final: edges = _resolve_temp_views(collected_edges, ...)
  elif record.type == "python":
    result = parse_pyspark(content, ...)
  elif record.type == "notebook":
    result = parse_notebook(content, ...)
  return (resolved_edges, raw_edges, warnings)   # feeds build_graph_with_warnings
```

**`SourceEntry` shape (U5):**

```
SourceEntry
  id, source_type, url, status, file_count, warning_count   # public
  token, records, parsed_files, file_stats, error_files     # private (not in to_public_dict)
  .to_public_dict() -> dict                                  # used by all route responses
```

---

## Implementation Units

- U1. **Backend column-ID split helper**

**Goal:** Introduce `split_column_id` in a new `backend/lineage/ids.py` module and replace all 10 inline `rsplit(".", 1)` column-ID split sites in `engine.py` and `routes.py`.

**Requirements:** R1, R3

**Dependencies:** None

**Files:**
- Create: `backend/lineage/ids.py`
- Modify: `backend/lineage/engine.py`
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_ids.py`

**Approach:**
- Define `split_column_id(col_id: str) -> tuple[str, str]` returning `(table, column)` via `col_id.rsplit(".", 1)`.
- Import and use it at all 10 call sites in `engine.py` and `routes.py`.
- Exclude: the ~12 `rsplit(".", 1)` calls inside `parsers/sql.py` resolver logic (internal, not column-ID splits); `ingestion/upload.py:20` (file extension extraction).

**Patterns to follow:**
- `backend/lineage/models.py` — module structure, `from __future__ import annotations`
- Existing `rsplit(".", 1)` call shapes in `engine.py` and `routes.py` for the exact replacements

**Test scenarios:**
- Happy path: `split_column_id("main.raw.orders.amount")` → `("main.raw.orders", "amount")`
- 3-part name: `split_column_id("raw.orders.amount")` → `("raw.orders", "amount")` (partially-qualified name)
- Regression guard: confirm the function uses `rsplit` not `split` — `split(".", 1)` on `"main.raw.orders.amount"` would give `("main", "raw.orders.amount")` which is wrong; use a 4-part name to catch this
- Integration: all existing pytest tests pass after the replacement (no behavior change)

**Verification:**
- `grep -r 'rsplit.*"\.".*1' backend/lineage/ backend/api/` returns no results outside `parsers/sql.py` internals
- All pytest tests pass

---

- U2. **Frontend column-ID split helper**

**Goal:** Add `splitColumnId` to `frontend/lib/utils.ts` and replace all inline `lastIndexOf(".")` split sites across 4 components.

**Requirements:** R2, R3

**Dependencies:** None (independent of U1)

**Files:**
- Modify: `frontend/lib/utils.ts`
- Modify: `frontend/components/lineage-graph.tsx`
- Modify: `frontend/components/lineage-tree.tsx`
- Modify: `frontend/components/column-inspector.tsx`
- Modify: `frontend/components/transform-inspector.tsx`

**Approach:**
- Add `export function splitColumnId(id: string): { table: string; col: string }` using `lastIndexOf(".")` + `slice`. Match the semantics of the local helpers already in `lineage-graph.tsx:33` and `lineage-tree.tsx:8` exactly.
- Replace the local helper definitions in those 2 components with an import from `lib/utils`.
- Replace the 3 inline sites in `column-inspector.tsx` (lines ~51, ~120) and `transform-inspector.tsx` (line ~49) with calls to the imported function.

**Patterns to follow:**
- Existing local `splitColumnId` implementations in `lineage-graph.tsx:33` and `lineage-tree.tsx:8` — the new shared function must match them exactly
- `cn()` export style in `frontend/lib/utils.ts`

**Test scenarios:**
- `splitColumnId("main.raw.orders.amount")` → `{ table: "main.raw.orders", col: "amount" }` (4-part name — must use this, not 2-part)
- 3-part: `splitColumnId("raw.orders.amount")` → `{ table: "raw.orders", col: "amount" }`
- Regression guard: confirm `table` for `"main.raw.orders.amount"` is `"main.raw.orders"` not `"main"` (catches `split(".")` mistake)
- Integration: `npm run build` produces no TypeScript errors; component rendering behavior is unchanged

**Verification:**
- No `lastIndexOf(".")` pattern remains inline inside any component file for column-ID purposes
- `npm run build` succeeds with no errors

---

- U3. **Separate Databricks format dispatch from `parse_sql`**

**Goal:** Remove format detection and cell-splitting from `parse_sql()`, promote `_split_databricks_sql` to a public export, and move dispatch to `engine._parse_file()`. Codebase remains fully working after this unit.

**Requirements:** R4, R5, R6

**Dependencies:** None

**Files:**
- Modify: `backend/parsers/sql.py`
- Modify: `backend/lineage/engine.py`
- Test: `backend/tests/test_sql_parser.py`
- Test: `backend/tests/test_engine.py`

**Approach:**
- In `parsers/sql.py`: rename `_split_databricks_sql` → `split_databricks_sql` and `_DATABRICKS_SQL_SEP` → `DATABRICKS_SQL_SEP` (or keep the constant private and access it only through the split function — either is fine). Remove the detection+split+recursive-call block from `parse_sql()` (lines ~1106–1114). `parse_sql` becomes a function that accepts a single SQL string and returns edges for that string only. The `_warnings` and `_raw_out` parameters remain on `parse_sql` for now (removed in U4).
- In `engine._parse_file`: add `from parsers.sql import split_databricks_sql` (and `DATABRICKS_SQL_SEP` if needed). In the `"sql"` branch, check if content contains the separator; if so, call `split_databricks_sql` and iterate cells, calling `parse_sql(..., source_cell=cell_idx, _resolve_views=False)` per cell, then run `_resolve_temp_views` on collected edges. If no separator, call `parse_sql(record.content, ..., _resolve_views=True)` as before. The `_raw_out` and `_warnings` mutable params are still threaded through `parse_sql` calls here (removed in U4).

**Patterns to follow:**
- Existing `_parse_file` branch structure for `"sql"` / `"python"` / `"notebook"` dispatch in `engine.py`

**Test scenarios:**
- Single-statement `.sql` file: `_parse_file` produces the same edge set as before
- Multi-cell Databricks `.sql` file (content containing `-- COMMAND ----------`): `_parse_file` dispatches cells and produces edges from all cells
- `parse_sql` called directly with a string containing `-- COMMAND ----------` no longer splits — the separator is treated as a SQL token, not a cell boundary
- Single-cell `.sql` content (no separator): takes the non-split path; behavior identical to today
- `_resolve_views=False` per cell: each cell's temp views are not independently resolved; final resolution happens at the collected-edges level

**Verification:**
- `parse_sql` contains no reference to `DATABRICKS_SQL_SEP` or cell-splitting logic
- All pytest tests pass

---

- U4. **ParseResult dataclass and parser return-type migration**

**Goal:** Add `ParseResult` to `lineage/models.py` and migrate `parse_sql`, `parse_pyspark`, and `parse_notebook` to return it instead of accepting mutable output parameters. Update `engine._parse_file` to consume `ParseResult`.

**Requirements:** R7, R8, R9

**Dependencies:** U3 (removes the recursive self-call from `parse_sql`, simplifying `ParseResult` integration)

**Files:**
- Modify: `backend/lineage/models.py`
- Modify: `backend/parsers/sql.py`
- Modify: `backend/parsers/pyspark.py`
- Modify: `backend/parsers/notebook.py`
- Modify: `backend/lineage/engine.py`
- Test: `backend/tests/test_sql_parser.py` (2 warning-collection tests need updating)
- Test: `backend/tests/test_engine.py`
- Test: `backend/tests/test_notebook_parser.py`

**Approach:**
- Add `ParseResult` dataclass to `models.py`: `edges: list[LineageEdge]`, `raw_edges: list[LineageEdge]`, `warnings: list[str]`. Use `field(default_factory=list)` for mutable defaults.
- `parse_sql()`: remove `_warnings` and `_raw_out` parameters. Internally accumulate warnings in a local list. Capture pre-resolution edges before calling `_resolve_temp_views`. Return `ParseResult(edges=resolved, raw_edges=pre_resolution, warnings=local_warnings)`. The `_resolve_views: bool = True` parameter stays — it is an input flag, not an output channel. Internal helpers (`_parse_copy`, `_parse_merge`, etc.) continue threading the local `warnings` list internally; their signatures are not changed.
- `parse_pyspark()`: remove `_warnings` parameter. Return `ParseResult(edges=edges, raw_edges=[], warnings=local_warnings)`.
- `parse_notebook()`: remove `_warnings` and `_raw_out` parameters. Return `ParseResult`.
- `engine._parse_file()`: remove `raw_edges_out` parameter. Collect `result.edges`, `result.raw_edges`, and `result.warnings` from each parser call. Feed `result.raw_edges` into `state.raw_graph` accumulation. Convert accumulated warnings to `ParseWarning` objects (preserving lines 36–37 logic).
- The two existing tests that pass a `warnings_list` (`test_bad_sql_collects_warning`, `test_one_bad_statement_does_not_drop_good_statements`) must be updated to assert on `result.warnings` instead.

**Patterns to follow:**
- `@dataclass` style in `backend/lineage/models.py` (e.g., `ParseWarning`, `LineageEdge`)
- `from __future__ import annotations` at top of `models.py`

**Test scenarios:**
- Happy path: `parse_sql("SELECT a FROM t", source_file="x.sql", source_line=1)` returns a `ParseResult` instance with `edges` populated and `warnings` empty
- Warning path: `parse_sql` with an unparseable statement returns `ParseResult` with `warnings` non-empty and `edges` containing the parseable-statement edges (regression: both fields must be correct)
- `parse_pyspark` returns `ParseResult` with `raw_edges=[]`
- `parse_notebook` returns `ParseResult` with edges and warnings matching prior behavior
- `engine._parse_file` return tuple still produces the same `(list[LineageEdge], list[ParseWarning])` shape used by `build_graph_with_warnings`
- Integration: `build_graph_with_warnings` produces identical `lineage_graph` and `raw_graph` before and after the migration

**Verification:**
- `parse_sql`, `parse_notebook`, `parse_pyspark` signatures contain no `_warnings` or `_raw_out` parameters
- `engine._parse_file` contains no `raw_edges_out` parameter
- All pytest tests pass including the 2 updated warning tests

---

- U5. **SourceEntry dataclass**

**Goal:** Replace plain source metadata dicts with a `SourceEntry` dataclass in a new `backend/api/models.py`. Update `state.py` type annotation and all call sites in `routes.py`.

**Requirements:** R10, R11, R12

**Dependencies:** None (independent of U1–U4)

**Files:**
- Create: `backend/api/models.py`
- Modify: `backend/state.py`
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_routes.py`

**Approach:**
- Define `SourceEntry` in `backend/api/models.py` with explicit typed fields. Public fields: `id`, `source_type`, `url`, `status`, `file_count`, `warning_count`. Private fields (no underscore prefix — privacy enforced by `to_public_dict()`): `token: str`, `records: list[FileRecord]`, `parsed_files: set[str]`, `file_stats: dict[str, dict]`, `error_files: set[str]`. Use `field(default_factory=...)` for collection fields set after construction (`records`, `parsed_files`, `file_stats`, `error_files`).
- `to_public_dict()` method: returns a plain `dict` with the 6 public fields only. This replaces the `{k: v for k, v in entry.items() if not k.startswith("_")}` pattern at all call sites.
- Update `state.py`: `source_registry: dict[str, SourceEntry]`. Add import.
- Update `routes.py`: replace dict construction with `SourceEntry(...)` instantiation; replace `entry.items()` filtering with `entry.to_public_dict()`; replace `entry["_records"]` / `entry["_token"]` etc. with typed attribute access (`entry.records`, `entry.token`).
- `test_routes.py:reset_state` fixture calls `.clear()` on `state.source_registry` — this still works after the type change.
- Do not reference or extend the dead `SourceConfig` in `lineage/models.py`.

**Patterns to follow:**
- `@dataclass` style in `backend/lineage/models.py`
- Existing private-key access patterns in `routes.py` — all become typed attribute access after migration

**Test scenarios:**
- `POST /sources` response contains only the 6 public fields (id, source_type, url, status, file_count, warning_count) — no token or records
- `GET /sources` returns a list where each entry has exactly the 6 public keys
- Round-trip: `POST /sources` followed by `GET /sources` returns matching id, source_type, url, status
- `DELETE /sources/{id}` removes the entry; subsequent `GET /sources` no longer includes it
- Attribute access: a freshly-registered `SourceEntry` has `entry.records` as a list of `FileRecord` (not a dict key lookup)
- Integration: `POST /sources` + `POST /sources/{id}/refresh` populates `entry.parsed_files` correctly

**Verification:**
- No `startswith("_")` filter pattern remains in `routes.py`
- No `entry["_token"]`, `entry["_records"]`, `entry["_parsed_files"]` etc. dict-key access remains in `routes.py`
- All `test_routes.py` tests pass

---

## System-Wide Impact

- **Interaction graph:** `build_graph_with_warnings` and `build_graph` are the only callers of `_parse_file`. No other code calls the parsers directly in production paths (tests call parsers directly, but they test the public interface which improves after this refactor).
- **Error propagation:** Unchanged — parse errors become `ParseWarning` objects in `_parse_file`. `ParseResult.warnings` is the new collection point; the `ParseWarning` construction logic in `_parse_file` (lines 36–37) is preserved.
- **State lifecycle risks:** `state.raw_graph` and `state.lineage_graph` must still be mutated together in `routes.py`'s refresh flow. `ParseResult.raw_edges` feeds `raw_graph` — this invariant must be preserved in U4.
- **API surface parity:** No API response shapes change. `to_public_dict()` returns the same 6 public keys as the current filter.
- **Integration coverage:** The two-graph mutation invariant (`lineage_graph` + `raw_graph` updated together) is not covered by existing tests — this is a pre-existing gap, not introduced here.
- **Unchanged invariants:** `build_graph_with_warnings` public signature (`list[FileRecord] → tuple[nx.DiGraph, list[ParseWarning]]`) does not change. All REST endpoint response shapes remain identical. The `_resolve_views: bool` parameter on `parse_sql` is kept.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Internal helpers (`_parse_copy`, `_parse_merge`, etc.) had `_warnings is not None` guards — removing the external param and using a local list changes the "always accumulate" behavior | Audit all `_warnings is not None` guards in `parsers/sql.py` before removing the parameter; ensure local list is always created unconditionally |
| Tests that import `_split_databricks_sql` by private name will break on rename | Grep tests for `_split_databricks_sql` before renaming; update any direct import |
| `pyspark` parser omitted from initial requirements scope — expanding it could introduce regressions | Run pyspark-specific tests after U4; `test_sql_parser.py` and `test_notebook_parser.py` both depend on the pyspark bridge path |
| Frontend TypeScript type mismatch if `splitColumnId` return type differs from component usage | Run `npm run build` immediately after U2 to catch type errors before moving on |

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-30-backend-code-quality-refactors-requirements.md](docs/brainstorms/2026-04-30-backend-code-quality-refactors-requirements.md)
- Related learning: `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md`
- Related learning: `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md`
- Related learning: `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md`
- Related learning: `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md`

---

## Deferred / Open Questions

### From 2026-04-30 review

- **pyspark raw_edges=[] silently clears raw_graph for all Python files** — Key Technical Decisions / U4 (P0, scope-guardian, feasibility, adversarial, confidence 100)

  `engine.py:21–25` currently copies ALL pyspark edges into `raw_edges_out`, which feeds `state.raw_graph`. The plan instructs `parse_pyspark` to return `raw_edges=[]` and the Open Questions section says "Either is fine; choose the cleaner path" — but `raw_edges=[]` would silently empty raw_graph for every Python file, breaking path tracing at `GET /lineage/paths`. The correct fix is `raw_edges=edges` for pyspark so the raw_graph invariant is preserved.

  <!-- dedup-key: section="key technical decisions  u4" title="pyspark raw_edges silently clears raw_graph for all python files" evidence="PySpark returns raw_edges=[]" -->

- **_detect_temp_views and _resolve_temp_views not promoted in U3** — U3 Approach (P1, adversarial, feasibility, confidence 100)

  After U3 moves the Databricks cell-dispatch loop to `engine._parse_file`, the engine will need to call `_resolve_temp_views` on collected edges from all cells. U3 only promotes `_split_databricks_sql`; `_detect_temp_views` and `_resolve_temp_views` must also be promoted (to `detect_temp_views` and `resolve_temp_views`). Without this, U3 is incomplete and the engine-side temp-view handling will still rely on private symbols — the same problem the plan is trying to solve.

  <!-- dedup-key: section="u3 approach" title="detect_temp_views and resolve_temp_views not promoted in u3" evidence="rename _split_databricks_sql  split_databricks_sql" -->

- **splitColumnId return type contradicts existing implementations** — U2 Approach / Test Scenarios (P2, feasibility, confidence 100)

  U2 specifies `{ table: string; col: string }` as the return type and test scenarios use this object shape. But both existing local `splitColumnId` implementations in `lineage-graph.tsx:33` and `lineage-tree.tsx:8` return `[string, string]` tuples. The plan says to "match the semantics of the local helpers exactly" — which directly contradicts the specified type. Implementing the object shape would require updating all call sites beyond what U2 describes.

  <!-- dedup-key: section="u2 approach  test scenarios" title="splitcolumnid return type contradicts existing implementations" evidence="Add export function splitColumnId(id: string): { table: string; col: string }" -->

- **Column-ID split site count is inconsistent across sections** — Overview / Problem Frame / U1 (P2, coherence, confidence 75)

  The Overview says "12+", the Problem Frame says "10+ backend sites and 4 frontend components" (=14+), and U1 says "all 10 call sites." These numbers are inconsistent. An implementer verifying exhaustive coverage gets a different target depending on which section they read. The consistent framing is "10 backend + 4 frontend = 14 total" per U1's detailed scope.

  <!-- dedup-key: section="overview  problem frame  u1" title="columnid split site count is inconsistent across sections" evidence="replace 12+ inline split patterns that are a documented bug source" -->

- **R2 Requirements Trace doesn't acknowledge 4-component scope expansion** — Requirements Trace (P2, coherence, confidence 75)

  The origin requirements doc named only 2 frontend components; the plan expanded to 4 after discovering additional split sites. Scope Boundaries documents the expansion with rationale, but R2 in Requirements Trace says "All frontend split sites use it" with no cross-reference. A reviewer checking R2 against the origin doc sees a silent scope change with no reconciliation in the traceability section.

  <!-- dedup-key: section="requirements trace" title="r2 requirements trace doesnt acknowledge 4component scope expansion" evidence="splitColumnId(id) frontend helper in libutils.ts. All frontend split sites use it." -->

- **SourceEntry url field construction timing unspecified** — U5 Approach (P2, feasibility, confidence 75)

  The plan lists `url` as a public field on `SourceEntry` but doesn't specify whether it is `Optional[str]` with default `None`, an empty string at construction, or required at construction time. The original dict's `url` value may be set at registration time from the request or populated after file processing. An implementer translating the dict to a dataclass needs this decision to write a correct constructor.

  <!-- dedup-key: section="u5 approach" title="sourceentry url field construction timing unspecified" evidence="Public fields: id, source_type, url, status, file_count, warning_count" -->

- **Test files with rsplit not mentioned in U1 scope** — U1 Scope (P2, scope-guardian, confidence 75)

  U1's scope says "all 10 call sites in engine.py and routes.py" without stating whether test files that may contain `rsplit(".", 1)` column-ID splits are in scope or explicitly excluded. If test files have such splits, leaving them unupdated creates inconsistency; if they don't, an explicit exclusion note would remove ambiguity for the implementer.

  <!-- dedup-key: section="u1 scope" title="test files with rsplit not mentioned in u1 scope" evidence="Import and use it at all 10 call sites in engine.py and routes.py." -->

- **Databricks multi-cell test not listed in U3** — U3 Files (P2, scope-guardian, confidence 75)

  `test_temp_view_with_mixed_case_in_databricks_notebook` calls `parse_sql` with multi-cell Databricks content containing `-- COMMAND ----------`. After U3 removes the splitter from `parse_sql`, this test's input will be treated as a single SQL string (not a multi-cell notebook), changing its behavior. It is not listed in U3's test files, so an implementer may miss it.

  <!-- dedup-key: section="u3 files" title="databricks multicell test not listed in u3" evidence="Test: backend/tests/test_sql_parser.py  backend/tests/test_engine.py" -->

- **ids.py circular-import safety unverified in plan** — Key Technical Decisions (P3, scope-guardian, confidence 75)

  The plan asserts that `split_column_id` in `backend/lineage/ids.py` is "importable from both `engine.py` and `routes.py` without circular imports" but doesn't verify this against the actual import graph. The claim is likely correct since `ids.py` would have no upstream imports, but it's an unverified assertion. An implementer should confirm before creating the module.

  <!-- dedup-key: section="key technical decisions" title="idsspy circularimport safety unverified in plan" evidence="Single-purpose; importable from both engine.py and routes.py without circular imports." -->

- **SourceEntry field renaming enables accidental full serialization** — U5 / Key Technical Decisions (P3, adversarial, confidence 75)

  Dropping underscore prefixes from SourceEntry's private fields means any caller using `dataclasses.asdict(entry)` will accidentally include `token`, `records`, and `parsed_files` in the output. The plan relies on `to_public_dict()` discipline but doesn't document this footgun. Should explicitly note that `to_public_dict()` is the only correct serialization path and that `dataclasses.asdict()` must not be used on `SourceEntry`.

  <!-- dedup-key: section="u5  key technical decisions" title="sourceentry field renaming enables accidental full serialization" evidence="SourceEntry field naming: Drop the _ prefix from private fields... Privacy is enforced by to_public_dict(), not by naming convention." -->
