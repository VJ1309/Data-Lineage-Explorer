---
title: "Backend code quality patterns: ParseResult, SourceEntry, layer separation, and shared ID helpers"
date: 2026-05-01
category: docs/solutions/architecture-patterns
module: backend
problem_type: architecture_pattern
component: service_object
severity: medium
applies_when:
  - Adding a new parser type to the engine
  - Extending source registry fields
  - Adding any column-ID split operation in backend or frontend
  - Moving format-detection logic between layers
  - Reviewing parser function signatures
tags:
  - parsers
  - dataclass
  - ParseResult
  - SourceEntry
  - engine
  - column-id
  - dispatch
  - refactoring
related_components:
  - frontend_stimulus
  - tooling
---

# Backend code quality patterns: ParseResult, SourceEntry, layer separation, and shared ID helpers

## Context

The DataLineage Explorer backend had four structural smells that raised the cost of every future change:

1. **Scattered column-ID splits** — `rsplit(".", 1)` (backend) and `lastIndexOf(".")` (frontend) duplicated at 10 backend sites and 4 frontend components. CLAUDE.md documents this as a known bug source; a past UI bug traced directly to this pattern (see `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md`).
2. **Format detection inside the parser** — `parse_sql()` detected Databricks `.sql` notebook separators, split on `-- COMMAND ----------`, and recursively called itself. Format-dispatch is an engine concern, not a grammar-parsing concern.
3. **Mutable output parameters** — all three parsers accepted pre-allocated lists as output channels (`_warnings`, `_raw_out`), hiding side effects and making unit testing awkward.
4. **Plain source dict with underscore-prefix privacy** — source metadata was stored as a raw dict with `_token`, `_records`, etc., filtered by `startswith("_")` at every read site.

All four were resolved in a coordinated refactor on branch `refactor/backend-code-quality` (commits `4213edb` through `8f2645f`, 2026-04-30 — 2026-05-01). No API surface or behavioral changes.

## Guidance

### 1. Shared column-ID split helpers

**Backend:** `backend/lineage/ids.py` exports `split_column_id(col_id: str) -> tuple[str, str]` using `col_id.rsplit(".", 1)`. All engine and route column-ID splits import from here.

**Frontend:** `frontend/lib/utils.ts` exports `splitColumnId(id: string): [string, string]` using `lastIndexOf(".")` + `slice`. Return type is a tuple `[string, string]`, not `{ table, col }` — this matches the existing local implementations in `lineage-graph.tsx` and `lineage-tree.tsx` that it replaces. All four components (`lineage-graph.tsx`, `lineage-tree.tsx`, `column-inspector.tsx`, `transform-inspector.tsx`) import from here.

**Critical invariant:** Always use `rsplit` / `lastIndexOf`, never `split` / `indexOf`. On `main.raw.orders.amount`, `split(".", 1)` returns `("main", "raw.orders.amount")` — wrong; `rsplit(".", 1)` returns `("main.raw.orders", "amount")` — correct. Tests must use 4-part column IDs to catch this; 2-part names pass with either implementation.

**Exclusion zones:** ~12 `rsplit(".", 1)` calls inside `_resolve_temp_views` and `_best_expression` in `parsers/sql.py` operate on internal lowercased strings within resolver logic, not column IDs — do not migrate. `ingestion/upload.py` uses rsplit for file extension extraction — also excluded.

### 2. Engine owns format dispatch; parsers own grammar

`parse_sql()` accepts a single SQL string and parses it. It does not detect `-- COMMAND ----------`, does not split input, and does not recurse. This is intentional.

`engine._parse_file()` owns all format dispatch:

```python
# engine._parse_file, sql branch
cells = split_databricks_sql(record.content)   # returns [content] if no separator
for idx, cell in enumerate(cells):
    result = parse_sql(cell, ..., source_cell=idx, _resolve_views=False)
    # collect result.edges, result.raw_edges, result.warnings
edges = resolve_temp_views(collected_edges, ...)
```

Public exports required from `parsers/sql.py`: `split_databricks_sql`, `DATABRICKS_SQL_SEP`, `detect_temp_views`, `resolve_temp_views`. When promoting private helpers to enable this separation, promote all four together. Promoting only `_split_databricks_sql` and leaving `_detect_temp_views`/`_resolve_temp_views` private leaves the engine calling private symbols — the same violation the refactor was meant to fix.

### 3. Parsers return `ParseResult`; no mutable output parameters

`ParseResult` is defined in `backend/lineage/models.py`:

```python
@dataclass
class ParseResult:
    edges: list[LineageEdge] = field(default_factory=list)
    raw_edges: list[LineageEdge] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
```

All three parsers return `ParseResult`. The engine consumes:
- `result.edges` → lineage graph
- `result.raw_edges` → `state.raw_graph` (pre-resolution edges)
- `result.warnings` → converted to `ParseWarning` objects

**PySpark raw_edges invariant:** `parse_pyspark` must return `raw_edges=edges` (a copy of the edge list), **not** `raw_edges=[]`. The engine feeds `raw_edges` into `state.raw_graph`. Returning `[]` would silently empty raw_graph for every Python file, breaking `GET /lineage/paths`. The original plan said "either is fine" — this is incorrect; `raw_edges=[]` is wrong.

Internal private helpers inside `parse_sql` (`_parse_copy`, `_parse_merge`, `_parse_command_fallback`, `_process_subquery`) continue threading a local `warnings: list[str]` — only the public function boundary changed; internal signatures are unchanged.

### 4. `SourceEntry` dataclass with explicit public interface

`SourceEntry` is defined in `backend/api/models.py` (API-layer concern, not `lineage/models.py`):

```python
@dataclass
class SourceEntry:
    # Public — included in to_public_dict()
    id: str
    source_type: str
    url: Optional[str]
    status: str
    file_count: int
    warning_count: int
    # Private — excluded from to_public_dict()
    token: str = ""
    records: list[FileRecord] = field(default_factory=list)
    parsed_files: set[str] = field(default_factory=set)
    file_stats: dict[str, dict] = field(default_factory=dict)
    error_files: set[str] = field(default_factory=set)

    def to_public_dict(self) -> dict:
        return {
            "id": self.id, "source_type": self.source_type, "url": self.url,
            "status": self.status, "file_count": self.file_count,
            "warning_count": self.warning_count,
        }
```

**Footgun — `dataclasses.asdict()` is unsafe here:** Private fields have no `_` prefix; privacy is enforced only by `to_public_dict()`. Calling `dataclasses.asdict(entry)` includes `token`, `records`, and `parsed_files` in the output. Always use `entry.to_public_dict()` for serialization. Document this constraint when adding new fields to `SourceEntry`.

`state.source_registry` is typed as `dict[str, SourceEntry]`. All route attribute access (`entry.token`, `entry.records`, etc.) replaces the old dict-key access (`entry["_token"]`, `entry["_records"]`). The `.clear()` call in `test_routes.py:reset_state` still works after the type change.

## Why This Matters

- **Column ID splits:** Each new feature touching column IDs risks adding another split site. The next `split(".", 1)` instead of `rsplit(".", 1)` passes all tests with 2-part names and ships a silent data bug.
- **Parser/engine separation:** When `parse_sql` owned format dispatch, adding a new notebook format required editing the parser. Now it requires editing only the engine's dispatch table.
- **Mutable output params:** Functions receiving pre-allocated output lists are harder to test (callers must allocate), harder to type (`list | None`), and hide the full contract. `ParseResult` makes the return contract explicit and verifiable in tests.
- **Dict-with-underscore-filter:** `startswith("_")` is fragile — a key renamed to not start with `_` leaks to API responses silently. `to_public_dict()` makes the contract explicit and enforced by code rather than convention.

## When to Apply

- **New parser type:** return `ParseResult`; pass through the engine's existing collect loop; ensure `raw_edges` is non-empty if raw_graph should reflect this parser's output
- **New source registry field:** add to `SourceEntry`; update `to_public_dict()` if it should be public; never use `dataclasses.asdict()`
- **New column-ID operation:** import `split_column_id` / `splitColumnId`; never inline
- **New file format detection:** add to the engine's dispatch table in `_parse_file`; do not add detection logic to parser functions
- **Verifying raw_graph health after parser changes:** check that `ParseResult.raw_edges` is populated for all non-notebook parsers

## Examples

**Column ID — before (10 inline backend sites):**
```python
table, col = col_id.rsplit(".", 1)
```

**After:**
```python
from lineage.ids import split_column_id
table, col = split_column_id(col_id)
```

---

**Parser signature — before:**
```python
warnings: list[str] = []
raw_out: list[LineageEdge] = []
edges = parse_sql(sql, source_file=f, source_line=1, _warnings=warnings, _raw_out=raw_out)
```

**After:**
```python
result = parse_sql(sql, source_file=f, source_line=1)
# result.edges, result.raw_edges, result.warnings
```

---

**Source metadata — before:**
```python
public = {k: v for k, v in source_entry.items() if not k.startswith("_")}
token = source_entry["_token"]
```

**After:**
```python
public = source_entry.to_public_dict()
token = source_entry.token
```

## Related

- `docs/solutions/ui-bugs/column-id-split-invariant-2026-04-25.md` — original bug that motivated column-ID helper extraction
- `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md` — three-tier function placement rule (engine / parser / helper)
- `docs/solutions/best-practices/lineage-engine-architecture-patterns-2026-04-25.md` — raw_graph + lineage_graph dual-graph invariant; `ParseResult.raw_edges` preserves this
- `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md` — `_warnings` internal threading pattern now superseded by `ParseResult`
- Plan: `docs/plans/2026-04-30-004-refactor-backend-code-quality-plan.md`
