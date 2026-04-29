# Requirements: Fix Transform Tab Multi-Source Expression Display

**Date:** 2026-04-29  
**Status:** Ready for planning

---

## Problem

When a target column is computed from a formula that references multiple source columns (e.g. `revenue_per_order = r.total_revenue / NULLIF(r.order_count, 0)`), the Transform tab shows two bugs:

1. **Duplicate SQL Logic blocks**: The same expression block renders once per source edge. For `revenue_per_order`, the division formula appears twice — once for `total_revenue` and once for `order_count`.

2. **Wrong expression in Column Transformations**: Each source row shows the formula that *uses* it (the final division expression), not the formula that *computes* it. The user sees `r.total_revenue / NULLIF(r.order_count, 0)` for both `agg_revenue.total_revenue` and `agg_revenue.order_count`, rather than `SUM(amount)` and `COUNT(order_id)` — the aggregations that actually produce those intermediate columns.

---

## Goal

The Transform tab for `revenue_per_order` should show:

```
SQL LOGIC
finance_mart.sql · line 1
r.total_revenue / NULLIF(r.order_count, 0)        ← shown once

COLUMN TRANSFORMATIONS
agg_revenue.total_revenue  [Aggregation]  SUM(amount)
agg_revenue.order_count    [Aggregation]  COUNT(order_id)
```

Column Transformations should answer "how is each source column itself computed?", not "what formula uses it?".

---

## Root Cause

`ColumnInspector` already receives `data.graph.edges` — the full upstream subgraph — but uses it only to filter `incoming` edges (edges where `target_col === selectedColId`). Both bugs stem from how those incoming edges are rendered:

- **Bug 1**: All incoming edges with a non-null expression are rendered as separate SQL Logic blocks. When two edges share the same `(expression, source_file, source_line)`, the block appears twice.

- **Bug 2**: Column Transformations renders `e.transform_type` and `e.expression` directly from each incoming edge, so every row shows the selected column's computation formula rather than the source column's own derivation.

---

## Desired Behavior

### Bug 1 fix — Deduplicate SQL Logic

Before rendering, deduplicate `withExpression` by `(expression, source_file, source_line)`. Identical blocks show once regardless of how many source edges share them.

### Bug 2 fix — Show predecessor expression in Column Transformations

For each row in Column Transformations, look up edges where `target_col === e.source_col` in the full `edges` set (the same prop already passed to the component). Use the `transform_type` and `expression` from that predecessor edge. If no predecessor is found (base table source), fall back to passthrough display.

When a source column has multiple predecessors (e.g. UNION), pick the one with the highest-priority `transform_type` (`window > cast > aggregation > expression > passthrough`).

---

## Scope

**In scope:**
- `frontend/components/column-inspector.tsx`: SQL Logic deduplication + Column Transformations predecessor lookup
- No backend changes required — full subgraph edges are already in the prop

**Out of scope:**
- Showing deep multi-hop chains (more than one step back) in Column Transformations
- Changing the Tree tab or lineage graph display
- PySpark or notebook parsers

---

## Success Criteria

- `mart_finance.revenue_per_order` in the live app: SQL Logic shows the division formula once; Column Transformations shows `SUM(amount)` [Aggregation] for `total_revenue` and `COUNT(order_id)` [Aggregation] for `order_count`
- Columns with a single source edge: no visible change (existing behavior preserved)
- Columns with no predecessor (base table source): Column Transformations row shows passthrough as before
- `npm run build` passes with no TypeScript errors
