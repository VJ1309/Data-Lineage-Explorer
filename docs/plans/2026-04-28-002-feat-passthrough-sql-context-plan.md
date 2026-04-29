---
title: "feat: Show full SQL context for passthrough transformations"
type: feat
status: completed
date: 2026-04-28
origin: docs/brainstorms/2026-04-28-passthrough-sql-context-requirements.md
---

# feat: Show full SQL context for passthrough transformations

## Overview

When a user clicks a column node in the lineage graph and the Transform tab opens, passthrough edges currently show "Passed through unchanged — no expression recorded." even though the SQL that produced the column is available at parse time. This plan captures the full SQL query (WITH chain + SELECT body, all clauses preserved) for passthrough edges and surfaces it in the column inspector.

---

## Problem Frame

The `ColumnInspector` SQL Logic section filters out passthrough edges (`e.transform_type !== "passthrough"`, `column-inspector.tsx:43`) and shows a static fallback message. The backend already calls `_classify_transform()` for passthrough columns and stores `expr_str` (the per-column reference, e.g. `customer_id`) — but it is too trivial to be useful. For complete logic analysis, engineers need the full SELECT context including CTEs and filtering clauses.

(see origin: `docs/brainstorms/2026-04-28-passthrough-sql-context-requirements.md`)

---

## Requirements Trace

- R1. Passthrough SQL edges expose the full SELECT query body as `expression` (WITH chain + SELECT, all clauses preserved).
- R2. CTEs are shown in full — all CTE bodies, all clauses.
- R3. Renamed columns (`customer_id AS client_id`) are visible in the displayed SQL.
- R4. Multiple passthrough upstream edges render as stacked scrollable SQL blocks with a height cap.
- R5. Non-passthrough edges, approximate/wildcard edges, and PySpark edges are unaffected.

---

## Scope Boundaries

- PySpark passthrough edges — no SQL SELECT to capture; separate paradigm.
- Approximate/wildcard passthrough edges (`CLONE`, `COPY INTO`, `confidence="approximate"`) — no SELECT body; `expression` stays as-is.
- MERGE statement passthrough assignments (call sites in `_parse_merge`, `sql.py:653` and `sql.py:682`) — individual column assignment expressions, not SELECT-level; not changed.
- Non-passthrough transform types (aggregation, expression, cast, window, filter) — expression handling unchanged.

---

## Context & Research

### Relevant Code and Patterns

- `backend/parsers/sql.py` — `_parse_select_node(select_node: exp.Select, ...)` at line 254 is the primary site. `select_node` is available throughout the function body. `select_node.sql(dialect="databricks", pretty=True)` emits the full query including the WITH clause when one is present.
- Main passthrough edge creation loop: `sql.py:440–552` (`for sel in select_node.selects:` → `transform_type, expr_str = _classify_transform(expr_node)` at line 455 → `LineageEdge(..., expression=expr_str, ...)` at line 546).
- Approximate/wildcard edges are created via `_wildcard_edge()` factory (`sql.py:698`) — separate path, not touched by this change.
- `frontend/components/column-inspector.tsx:42–43` — `withExpression` filter that excludes passthrough.
- `frontend/components/column-inspector.tsx:74` — `div` wrapping each `SyntaxHighlighter` block; no height cap currently.
- `frontend/app/lineage/page.tsx:106` — only `ColumnInspector` is wired; `TransformInspector` was removed.

### Institutional Learnings

- `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md` — compute per-SELECT derived values once before the `for sel in select_node.selects:` loop, not per-column inside it. `full_sql` follows this pattern.

---

## Key Technical Decisions

- **Overwrite `expression` for passthrough rather than adding a new field:** The `expression` field is already the "SQL logic" display field. Per-column passthrough refs (`customer_id`) are trivially useless; the full SELECT is the meaningful value. No schema addition needed; no API contract change.
- **Compute `full_sql` once before the SELECT loop, not per-column:** All passthrough columns from the same SELECT share the same full query. Matches the existing pattern for per-SELECT derived values (see institutional learnings above).
- **Only override `certain` passthrough edges:** Approximate/wildcard edges are created via a separate factory (`_wildcard_edge`) and are not in the `for sel in select_node.selects:` loop, so they are naturally excluded without a guard.
- **All clauses preserved (no stripping of WHERE/HAVING/GROUP BY):** User goal is complete logic analysis; filtering context is part of the logic.
- **Height cap on SQL blocks in `ColumnInspector`:** A column with multiple passthrough upstream sources renders multiple full-query blocks. Cap each at `max-h-48 overflow-y-auto` to prevent the panel from becoming a wall of SQL.

---

## Open Questions

### Resolved During Planning

- **Do MERGE passthrough call sites need updating?** No. Lines 653 and 682 are inside `_parse_merge`, which processes individual column assignments (`target.col = source.col`), not SELECT projections. No `select_node` is in scope there. (see origin)
- **Should WHERE/HAVING/GROUP BY be stripped?** No. Preserved for complete logic analysis. (see origin)
- **Does `TransformInspector` need changes?** No. It was removed from the lineage page in `feat(lineage): click column node in graph to open transform inspector`. Only `ColumnInspector` is active. (see origin)

### Deferred to Implementation

- Whether `select_node.sql()` on very large multi-CTE queries produces output above a size threshold that warrants truncation — observe during manual testing and add a `[:8000]` guard if needed.

---

## Implementation Units

- U1. **Capture full SELECT SQL for passthrough edges in the backend parser**

**Goal:** For each `certain` passthrough edge produced inside `_parse_select_node`, store the full formatted SELECT query (WITH chain + SELECT body, all clauses) as `expression` instead of the per-column reference string.

**Requirements:** R1, R2, R3, R5

**Dependencies:** None

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

**Approach:**
- Before the `for sel in select_node.selects:` loop (line 440), compute once: `full_sql = select_node.sql(dialect="databricks", pretty=True)`.
- Inside the loop, after `_classify_transform` returns `"passthrough"`, use `full_sql` as the `expression` argument instead of `expr_str` when building `LineageEdge`.
- The change is scoped to the column-level edge construction block inside `_parse_select_node`. `_parse_merge` is untouched.
- `_wildcard_edge` calls (approximate edges) are outside this loop and are not affected.

**Patterns to follow:**
- Per-SELECT derived values computed once before the selects loop — see `_tvf_synthetic_name` usage pattern in `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md`.
- `select_node.sql(dialect="databricks", pretty=True)` — same dialect and pretty flag used throughout the file.

**Test scenarios:**
- Happy path: `INSERT INTO tgt SELECT a, b FROM src` → passthrough edge `expression` contains `SELECT\n  a,\n  b\nFROM src` (the full SELECT body, no INSERT wrapper).
- Happy path: renamed column `SELECT customer_id AS client_id FROM raw.orders` → `expression` contains `AS client_id` making the rename visible.
- Happy path: single CTE `WITH base AS (SELECT x FROM t) SELECT x FROM base` → `expression` contains the full `WITH base AS (...)  SELECT x FROM base`.
- Happy path: multi-CTE chain → `expression` contains all CTE definitions in order.
- Happy path: SELECT with WHERE clause → WHERE is preserved in `expression`.
- Happy path: JOINed SELECT → JOIN clause preserved in `expression`.
- Regression: aggregation edge (`SUM(amount) AS total`) — `expression` still contains the per-column aggregation expression, not the full SELECT.
- Regression: expression edge (`amount * 1.1 AS adjusted`) — `expression` still contains the per-column arithmetic expression.
- Regression: approximate/wildcard passthrough (CLONE) — `expression` is unchanged (not the full SELECT).
- Edge case: `SELECT *` passthrough → `expression` contains `SELECT * FROM tbl`.
- Edge case: passthrough through a `CREATE TABLE AS SELECT` — `expression` is the inner SELECT body (no CREATE TABLE wrapper).

**Verification:**
- All existing `test_sql_parser.py` tests pass.
- New tests for the scenarios above pass.
- A manual `parse_sql()` call with a multi-CTE query confirms `expression` on passthrough edges is the full formatted SQL string.

---

- U2. **Display passthrough SQL expressions in `ColumnInspector`**

**Goal:** Remove the frontend filter that hides passthrough edges from the SQL Logic section, and cap each SQL block's height so multiple upstream sources don't produce an unbounded panel.

**Requirements:** R4, R5

**Dependencies:** U1 (passthrough edges must carry a non-null `expression` before the frontend can display it)

**Files:**
- Modify: `frontend/components/column-inspector.tsx`

**Approach:**
- `column-inspector.tsx:43` — remove `e.transform_type !== "passthrough"` from the `withExpression` filter predicate. The remaining two conditions (`e.expression` truthy and `e.expression !== "*"`) are sufficient to exclude approximate/wildcard edges whose expression remains null or `"*"`.
- `column-inspector.tsx:74` — add `max-h-48 overflow-y-auto` to the wrapper `div` around each `SyntaxHighlighter` block so full-query SQL scrolls within a fixed height rather than expanding the panel.

**Patterns to follow:**
- Existing `SyntaxHighlighter` usage in the same file — same `language`, `style`, and `customStyle` props; only the wrapper `div` changes.

**Test scenarios:**
- Happy path: clicking a passthrough column node renders a syntax-highlighted SQL block in the SQL Logic section instead of "Passed through unchanged."
- Happy path: a column with two passthrough upstream sources renders two stacked SQL blocks, each scrollable independently.
- Happy path: a non-passthrough column (aggregation) still shows its per-column expression — no regression.
- Edge case: a passthrough column whose `expression` is null (approximate/wildcard edge) still falls through to the "Passed through unchanged" fallback — the filter continues to exclude it because `e.expression` is falsy.
- Build: `npm run build` passes with no TypeScript errors.
- Lint: `npm run lint` passes.

**Verification:**
- `npm run build` and `npm run lint` pass.
- In the running app: clicking a column node that has passthrough upstream sources shows the full SQL query in the SQL Logic section with a scrollable block.
- Clicking an approximate/wildcard passthrough column still shows the unchanged fallback message.

---

## System-Wide Impact

- **Unchanged invariants:** The `expression` field is not persisted — it is computed fresh on every parse and returned in the `/lineage/paths` and edge-level API responses. No migration required. Non-passthrough expression semantics are unchanged.
- **API surface parity:** `PathStep.expression` and the edge objects in graph responses already accept arbitrary strings; no type change needed on the frontend `api.ts` types.
- **State lifecycle risks:** None. `full_sql` is computed per parse call; no caching or shared state involved.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Very large multi-CTE queries produce extremely long `expression` strings, slowing serialization | Monitor during manual testing; add a character cap (e.g. `[:8000]`) if needed — tracked as a deferred implementation note |
| `select_node.sql()` raises on a malformed AST node | Wrap in the same `try/except` pattern already used in `_classify_transform` (`sql.py:41–43`); fall back to `expr_str` |

---

## Sources & References

- **Origin document:** [`docs/brainstorms/2026-04-28-passthrough-sql-context-requirements.md`](../brainstorms/2026-04-28-passthrough-sql-context-requirements.md)
- Parser entry point: `backend/parsers/sql.py`, `_parse_select_node` (line 254)
- Frontend display: `frontend/components/column-inspector.tsx` (lines 42–89)
- Learnings: `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md`
