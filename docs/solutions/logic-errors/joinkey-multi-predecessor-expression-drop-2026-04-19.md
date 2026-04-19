---
title: Join Key Multi-Predecessor Expression Drop in list_columns
date: 2026-04-19
category: docs/solutions/logic-errors
module: backend/api/routes.py
problem_type: logic_error
component: service_object
severity: medium
symptoms:
  - "Catalog UI shows only one join condition when expanding Join Key transform on __joinkey__ columns with multiple source tables"
  - "Additional join condition expressions beyond preds[0] are silently dropped from the /tables/{table}/columns response"
  - "expression field in ColumnMeta contains only a partial join predicate for multi-predecessor join key nodes"
root_cause: logic_error
resolution_type: code_fix
related_components:
  - backend/lineage/engine.py
  - backend/parsers/sql.py
tags:
  - join-key
  - lineage-graph
  - silent-data-loss
  - networkx
  - fastapi
  - column-metadata
  - multi-predecessor
---

# Join Key Multi-Predecessor Expression Drop in list_columns

## Problem

The `list_columns` endpoint in `backend/api/routes.py` read the `expression` field only from the first predecessor edge (`preds[0]`) when building column metadata for pseudo-columns like `__joinkey__`. For join key nodes with source columns drawn from multiple JOIN ON clauses, this silently discarded every expression after the first — the catalog UI showed only one join condition regardless of how many JOINs contributed to that key.

## Symptoms

- On the catalog page, expanding the "Join Key" transform badge for a `__joinkey__` column displayed only the first JOIN ON expression (e.g., `r.customer_id = c.customer_id`) even when the SQL contained multiple JOINs with distinct ON predicates.
- The source tables column on the same row correctly listed all involved tables (e.g., `agg_revenue, customer_dim`), making the discrepancy between source table count and visible expression count the clearest observable signal.
- No error was raised anywhere in the stack — the data was present in the graph; it was simply never read past the first predecessor.

## What Didn't Work

The bug was identified directly from the symptom. The investigation first checked the frontend catalog component (`frontend/app/catalog/page.tsx`) to rule out a rendering suppression issue before narrowing to the backend API — that detour was quick and conclusive. No failed fix attempts were made. (session history)

## Solution

Iterate every predecessor edge to collect all unique expressions, then newline-join them before writing to the response payload.

**Before (broken) — `backend/api/routes.py`:**
```python
preds = list(state.lineage_graph.predecessors(node))
edge_data = None
source_tables: list[str] = []
if preds:
    edge_data = state.lineage_graph.edges[preds[0], node].get("data")  # only first pred!
    for pred in preds:
        if "." in pred:
            st = pred.rsplit(".", 1)[0]
            if st not in source_tables:
                source_tables.append(st)
cols.append({
    ...
    "expression": edge_data.expression if edge_data else None,  # only first pred's expr!
})
```

**After (fixed):**
```python
preds = list(state.lineage_graph.predecessors(node))
edge_data = None
source_tables: list[str] = []
seen_exprs: list[str] = []
if preds:
    edge_data = state.lineage_graph.edges[preds[0], node].get("data")
    for pred in preds:
        if "." in pred:
            st = pred.rsplit(".", 1)[0]
            if st not in source_tables:
                source_tables.append(st)
        ed = state.lineage_graph.edges[pred, node].get("data")
        if ed and ed.expression and ed.expression not in seen_exprs:
            seen_exprs.append(ed.expression)
combined_expression = "\n".join(seen_exprs) if seen_exprs else None
cols.append({
    ...
    "expression": combined_expression,
})
```

No frontend type changes were required. `expression: string | null` on `ColumnMeta` already accommodates a multi-line string, and the `<code>` element in the catalog renders with `whitespace-pre-wrap`, so newline-joined expressions display as distinct lines.

**Note on `seen_exprs` as `list[str]`:** A list is used (not a set) to preserve insertion order, which keeps the joined output stable relative to the order NetworkX returns predecessors. The more idiomatic Python pattern for ordered deduplication is `dict[str, None]` (O(1) membership, insertion-order preserved since 3.7): `seen_exprs[ed.expression] = None`, then `"\n".join(seen_exprs)`. Both are correct here given the tiny predecessor counts typical of join nodes.

## Why This Works

**`preds[0]`-only access for expression:** The original code hoisted `edge_data` out of the predecessor loop before the loop ran. The loop correctly accumulated all source tables but never touched `edge_data` again. Because `expression` was read from that single hoisted object, only the first predecessor's expression reached the response.

**`preds[0]` still used for other fields — intentionally:** After the fix, `edge_data` (from `preds[0]`) still provides `source_file`, `source_cell`, `source_line`, and `transform_type`. This is correct: for a given column node all predecessor edges come from the same parsed file and carry the same transform type, so any single edge is a valid representative. These fields do not need aggregation — only `expression` did.

**How `__joinkey__` edges are structured:** The SQL parser emits one `LineageEdge` per source column referenced in a JOIN ON predicate, carrying the full ON clause text (`on_expr_str`) as its `expression`. All column refs in a single JOIN ON clause share the same expression string — but each additional JOIN contributes a distinct string. A `__joinkey__` node with N JOINs therefore has multiple predecessor edges carrying N distinct expression values.

**Why `source_tables` was already correct:** It was populated inside the predecessor loop from the start, reading `pred.rsplit(".", 1)[0]` for every predecessor. The bug was purely in the separate pre-loop assignment of `edge_data` — source table collection never depended on it.

**Origin of the bug:** The `expression` field was added to the `list_columns` response when the column drill-down feature was introduced (the expand-arrow UI in the catalog). The `preds[0]`-only pattern was present in that initial implementation and was never noticed because single-JOIN SQL files have only one distinct expression per `__joinkey__` column. (session history)

## Prevention

- **Audit every predecessor loop for asymmetric reads.** When a node can have multiple predecessors, any attribute that should reflect the aggregate of all predecessors must be read inside the loop — not assigned from `preds[0]` before iteration begins. The failure pattern: hoist a value as a "default", loop to collect a list, then emit the hoisted value in the final output. Fields that are homogeneous across predecessors (like `source_file`) are fine to read from `preds[0]`; fields that vary per edge (like `expression`) must be aggregated.

- **Apply the same fix proactively to `__filter__` columns.** Filter pseudo-columns (`table.__filter__`) share the same multi-predecessor edge structure. If expression display is ever added for filter rows in the catalog UI, review `list_columns` at that point and apply the same expression accumulation pattern.

- **Test expression completeness, not just source table count.** When writing tests for `__joinkey__` or `__filter__` columns, assert that the returned `expression` string contains substrings from every JOIN ON / WHERE clause in the SQL fixture. A test checking only `len(source_tables) == 2` would have passed throughout this bug's lifetime. No test currently covers `GET /tables/{table}/columns` expression aggregation for multi-predecessor nodes. (session history)

## Related Issues

- No existing solution docs to cross-reference (first entry in `docs/solutions/`).
- GitHub issue search skipped (gh CLI not permitted in this session). To search manually: `gh issue list --search "join key expression OR joinkey" --state all --limit 5` from the repo root.
