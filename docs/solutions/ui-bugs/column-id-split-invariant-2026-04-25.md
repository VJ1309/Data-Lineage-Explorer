---
title: "TransformInspector column ID split — table prefix truncated by split().at(-2)"
date: 2026-04-25
category: docs/solutions/ui-bugs
module: lineage
problem_type: ui_bug
component: frontend_stimulus
symptoms:
  - "Transform tab summary banner shows 'table.col' instead of 'catalog.schema.table.col'"
  - "Path selector pills show 'via table' instead of 'via catalog.schema.table'"
  - "Distinct paths with identically-named tables in different schemas appear as duplicates"
root_cause: wrong_api
resolution_type: code_fix
severity: high
tags:
  - column-id
  - 4-part-naming
  - lastindexof
  - transform-inspector
  - lineage
  - invariant
  - split-bug
related_components:
  - documentation
---

# TransformInspector column ID split — table prefix truncated by split().at(-2)

## Problem

The `TransformInspector` component implemented its own `colLabel` helper using `split(".")` and `at(-2)` to extract the table name from a column ID. For the codebase's 4-part column IDs (`catalog.schema.table.column`), this silently discards the `catalog.schema` prefix, causing truncated labels throughout the Transform tab with no error or warning.

## Symptoms

- The summary banner shows `"Aggregated from table.col"` instead of `"Aggregated from catalog.schema.table.col"`
- Path selector pills show `"via table"` instead of `"via catalog.schema.table"`
- In projects with identical table names across schemas, distinct paths appear as duplicates (e.g., two pills both reading `"via orders"` instead of `"via raw.sales.orders"` and `"via clean.sales.orders"`)
- `computePillLabels` deduplication appends `(1)`, `(2)` suffixes to paths that are actually distinct, making the UI appear to have redundant paths when none exist

## What Didn't Work

The naive `split(".")` approach:

```typescript
function colLabel(colId: string) {
  const parts = colId.split(".");
  const col = parts.at(-1) ?? colId;
  const tbl = parts.at(-2) ?? "";  // only returns "table", drops "catalog.schema"
  return { col, tbl, full: colId };
}
```

For a 4-part ID like `catalog.schema.table.column`:
- `parts` = `["catalog", "schema", "table", "column"]`
- `parts.at(-2)` = `"table"` — `catalog.schema` is silently discarded

The function looks correct for 2-part names (`schema.column`), which is why the bug survived code review — no crash, no type error, and the display is plausible-looking unless the reviewer knows the expected 4-part format.

## Solution

Use `lastIndexOf(".")` to split at the rightmost dot only, treating everything to the left as the full table identifier:

```typescript
// Before (broken) — frontend/components/transform-inspector.tsx
function colLabel(colId: string) {
  const parts = colId.split(".");
  const col = parts.at(-1) ?? colId;
  const tbl = parts.at(-2) ?? "";
  return { col, tbl, full: colId };
}

// After (correct)
function colLabel(colId: string) {
  const dot = colId.lastIndexOf(".");
  const col = dot === -1 ? colId : colId.slice(dot + 1);
  const tbl = dot === -1 ? "" : colId.slice(0, dot);
  return { col, tbl, full: colId };
}
```

This matches the pattern already used in `frontend/components/lineage-graph.tsx` and `frontend/components/lineage-tree.tsx`.

## Why This Works

All column IDs in this codebase are 4-part: `catalog.schema.table.column`. The "table" portion is a 3-part qualified name (`catalog.schema.table`), not a single word. The only reliable way to split a column ID into its table prefix and column name is to find the last dot — everything to the right is the column, everything to the left is the table.

`split(".")` destroys structural information by fragmenting all segments equally. Once fragmented, `at(-2)` can only recover one segment (`"table"`), not the full prefix (`"catalog.schema.table"`). `lastIndexOf` avoids splitting in the first place — it finds the boundary position without fragmenting.

The Python equivalent is `rsplit(".", 1)`, which splits at the rightmost dot once. Both `lastIndexOf` (JS) and `rsplit(".", 1)` (Python) encode the same semantic: one column name lives after the last dot; everything else is the qualified table name.

## Prevention

**When reviewing any new component that displays column labels:**

1. Search the component for `.split(".")` on a variable that could hold a column ID — flag it immediately. `split(".")` on a column ID is always wrong in this codebase.
2. Verify the table portion is extracted with `lastIndexOf(".")` + `slice(0, dot)`, not `parts.at(-2)` or other indexed segment access.
3. Flag any `colLabel` / `splitColId` helper reimplemented from scratch — new components should reference the established pattern, not re-invent it.

**Pattern to enforce in all frontend components:**

```typescript
const dot = colId.lastIndexOf(".");
const col = dot === -1 ? colId : colId.slice(dot + 1);
const tbl = dot === -1 ? "" : colId.slice(0, dot);
```

**Longer-term prevention:** Consider extracting this to a shared utility in `lib/utils.ts` (e.g., `splitColId(colId: string): { col: string; tbl: string }`) so future components import one canonical function rather than re-implementing it inline.

The invariant is documented in `CLAUDE.md` under "Key invariant — naming convention." Point new contributors to that section before any column-label work begins.

**Risk profile:** Any component added that displays column labels is at risk of repeating this mistake, because the naive `split(".")` approach looks correct to developers unfamiliar with 4-part IDs. Silent regressions of this type are hard to catch without test fixtures using fully-qualified names with both a catalog and schema segment (e.g., `main.raw.orders.amount`, not just `orders.amount`).

## Related Issues

- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md` — different bug class (backend Python, edge aggregation), but also involves `api/routes.py` and `lineage/engine.py`
