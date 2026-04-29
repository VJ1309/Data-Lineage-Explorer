# Requirements: Show Full SQL Context for Passthrough Transformations

**Date:** 2026-04-28
**Status:** Ready for planning

---

## Problem

When a user clicks a column node in the lineage graph, the Transform tab opens `ColumnInspector` and shows "Passed through unchanged — no expression recorded." for passthrough edges. Engineers doing logic analysis or audits cannot see the SQL that produced the column without leaving the app.

## Goal

For passthrough SQL edges, capture and display the full SQL query (WITH clause + SELECT body, all clauses preserved) so engineers can do complete logic analysis from the app.

## User Story

As a data engineer who clicked a column node in the lineage graph, I want to see the full SQL query that selected this column — including all CTEs and filtering logic — so I can verify correctness and understand the complete data flow without opening the source file.

---

## Architecture Context

The lineage page (`frontend/app/lineage/page.tsx`) mounts `ColumnInspector` when a column node is clicked in the graph. `TransformInspector` was removed in a prior commit (it triggered path DFS on tab open, an OOM vector). The only place SQL logic is displayed is `ColumnInspector`'s "SQL Logic" section, which renders all incoming edges with non-null expressions as stacked syntax-highlighted code blocks.

---

## Behavior

### What gets shown

The `expression` field on passthrough `LineageEdge`s is populated with the full SQL query body: the SELECT statement plus any WITH clause (CTEs) it carries, exactly as SQLGlot formats it. All clauses preserved — WHERE, GROUP BY, HAVING, JOINs, CTE bodies.

**Example — simple SELECT:**
```sql
SELECT
  customer_id AS client_id,
  order_date,
  amount
FROM catalog.raw.orders
WHERE
  status = 'active'
```

**Example — multi-CTE:**
```sql
WITH base AS (
  SELECT
    customer_id,
    order_date,
    amount * 1.1 AS adjusted_amount
  FROM catalog.raw.orders
  WHERE
    status = 'active'
), enriched AS (
  SELECT
    b.customer_id,
    b.order_date,
    b.adjusted_amount,
    c.region
  FROM base AS b
  JOIN catalog.raw.customers AS c
    ON b.customer_id = c.id
)
SELECT
  customer_id,
  order_date,
  adjusted_amount,
  region
FROM enriched
```

All columns from the same SELECT share the same expression value — the full query is the context for each column, not just the individual column reference.

### Multiple upstream sources

A column with multiple passthrough upstream edges (e.g. sourced from 5 different queries) will show 5 stacked SQL blocks in the SQL Logic section. Each block is capped at `max-h-48` with `overflow-y-auto` so long queries scroll within the block rather than pushing content off screen.

### What does NOT change

- Non-passthrough edges (`expression`, `aggregation`, `cast`, `window`, `filter`) keep their current per-column expression strings and rendering.
- Approximate / wildcard passthrough edges (`CLONE`, `COPY INTO`, `confidence="approximate"`) are excluded — they have no SELECT body to show.
- PySpark passthrough edges are out of scope — they have no SQL SELECT to capture.
- The `transform_type`, `source_file`, `source_line`, `source_cell` fields are unchanged.

---

## Scope

**In scope:**
- SQL passthrough edges from SELECT statements (direct SELECT, INSERT...SELECT, CREATE TABLE AS SELECT, CREATE VIEW AS SELECT)
- Multi-CTE queries (full WITH chain captured)
- Renamed columns (`customer_id AS client_id` visible in the full SELECT)
- JOINed sources (JOIN clauses preserved in output)

**Out of scope / deferred:**
- PySpark passthrough
- Approximate/wildcard edges (CLONE, COPY INTO)
- Aggregation/expression/window edges (already handled)

---

## Implementation Surface

### Backend — `backend/parsers/sql.py`

At the call site where passthrough `LineageEdge`s are created (the SELECT processing loop, ~line 455), compute `full_sql` from the SELECT node being processed:

```python
full_sql = select_node.sql(dialect="databricks", pretty=True)
```

`select_node` already carries its WITH clause as `args["with_"]` when CTEs are present, so a single `.sql()` call emits the complete query. Store this as `expression` on passthrough edges instead of the current per-column expression string.

Only override for `certain` passthrough edges. Leave approximate/wildcard edges (which are created separately in the wildcard factory) unchanged.

### Frontend — `frontend/components/column-inspector.tsx`

Two changes:

1. Remove `e.transform_type !== "passthrough"` from the `withExpression` filter (line 43) so passthrough edges with a non-null expression are included in the SQL Logic section.

2. Add `max-h-48 overflow-y-auto` to the wrapper `div` around each `SyntaxHighlighter` block (line 74) so tall SQL queries scroll within the block.

---

## Success Criteria

- Clicking a passthrough column node shows the full SQL query in the SQL Logic section (not "Passed through unchanged")
- CTEs are shown in full — all CTE bodies, all clauses
- Renamed columns (`AS new_name`) are visible in the displayed SQL
- Non-passthrough edges are unaffected
- Approximate/wildcard passthrough edges show no expression (unchanged)
- SQL blocks with many lines scroll within a capped height rather than expanding the panel unboundedly
- Existing tests pass; a new test asserts `expression` is non-null and contains the SELECT body for a passthrough edge from a CTE query
