# Requirements: Fix Transform Tab Accuracy for ship_ordr_mlstn

**Date:** 2026-04-28  
**Status:** Ready for planning

---

## Problem

The Transform tab (ColumnInspector) in the lineage page shows inaccurate logic breakdowns for columns in `uc_dc_dev.scru_em.ship_ordr_mlstn`. Two distinct bugs contribute:

1. **Wrong/trivial expressions**: The SQL shown for passthrough columns is the trivial consumer-hop expression (e.g. `SELECT * FROM SRC_MATCH_RNK`) rather than the meaningful transformation (e.g. `COALESCE(TDL_GI_FCT.SRC_SYS_CD, '#')` or a GROUP BY statement). This is because `_resolve_temp_views` inherits the CONSUMER edge's expression when collapsing a temp-view chain — discarding the semantically important intermediate expressions.

2. **Wrong source column / unresolved temp views**: Some columns (e.g. `mlstn_event_cd`) show an intermediate CTE name (`src_match_rnk.mlstn_event_cd` or `no_data_part.mlstn_event_cd`) as their source instead of the actual upstream table column. This happens when `_resolve_temp_views` cannot trace the column through the temp view chain because the column is not explicitly named in the CTE's SELECT — leaving the edge unresolved in `lineage_graph`.

---

## Goal

When a user clicks a column node in the lineage graph, the Transform tab should show:
- The **actual source table column** (e.g. `sc_core.delv_line.src_sys_cd`), not an intermediate temp view name
- The **meaningful transformation expression** (e.g. COALESCE, GROUP BY SELECT, window function) that determined the column's value, not the trivial final-hop `SELECT * FROM last_view`

---

## User Story

As a data engineer inspecting `ship_ordr_mlstn.mlstn_event_cd`, I want to see the real source table and the SQL logic that computes this column so I can verify correctness without opening source files.

---

## Observed Behavior (Bugs)

### Bug 1: Trivial expression overwrites meaningful transformations

**Example — `src_sys_cd`:**

`_resolve_temp_views` collapses the chain:
```
delv_line.src_sys_cd
  → [COALESCE in xtrk_base CTE]
  → ship_ordr_mlstn_dtls.src_sys_cd
  → [SELECT * FROM SRC_MATCH_RNK]
  → ship_ordr_mlstn.src_sys_cd
```

The resolved edge in `lineage_graph` gets the consumer's expression:  
`expression = "SELECT\n  *\nFROM SRC_MATCH_RNK"`

ColumnInspector "SQL Logic" section shows: `SELECT * FROM SRC_MATCH_RNK`  
**Expected:** `COALESCE(TDL_GI_FCT.SRC_SYS_CD, '#')` (the meaningful transformation)

### Bug 2: Unresolved temp view left as source

**Example — `mlstn_event_cd`:**

`_resolve_temp_views` can find no upstream for `src_match_rnk.mlstn_event_cd` in `tv_sources` (the column is not explicitly named in cell 3's SELECT and no wildcard edge targets `src_match_rnk.*`). The edge is left unresolved:

`src_match_rnk.mlstn_event_cd → ship_ordr_mlstn.mlstn_event_cd`

ColumnInspector shows source as `src_match_rnk.mlstn_event_cd` (a temp view, not a real table).  
**Expected:** the actual source from `ship_ordr_mlstn_dtls` or a deeper base table.

---

## Architecture Context

- `backend/parsers/sql.py` — `_resolve_temp_views()` collapses temp view chains. Resolution uses the CONSUMER edge's `(transform_type, expression)`, discarding upstream expressions.
- `backend/lineage/engine.py` — `build_graph_with_warnings()` separates `lineage_graph` (resolved) from `raw_graph` (pre-resolution). The raw_graph correctly preserves intermediate steps (including COALESCE at step 3, GROUP BY at step 2), but is only used by `/lineage/paths` which no longer has a UI.
- `frontend/components/column-inspector.tsx` — reads `data.graph.edges` (resolved `lineage_graph` edges) and renders SQL Logic + Column Transformations sections from incoming edges.

---

## Desired Behavior

### Bug 1 fix — Surface the most meaningful expression

When resolving a chain through temp views, instead of inheriting only the consumer's expression, the resolved edge should carry the **most meaningful non-trivial expression** found anywhere in the chain:

- Prefer expressions where `transform_type ∈ {aggregation, window, expression, cast}` over passthrough
- Among passthroughs, prefer expressions that contain `GROUP BY`, `OVER`, `WITH`, `CASE`, `HAVING` over simple `SELECT * FROM view`
- Fall back to the consumer expression only if the entire chain is trivially passthrough

This applies to the expression field on resolved `LineageEdge` objects in `lineage_graph`.

### Bug 2 fix — Resolve unresolved temp view sources

When `_resolve_temp_views` cannot find a column-level entry in `tv_sources` for a temp view column, it should:

1. **Wildcard fallback**: if `tv_sources["tbl.*"]` exists, resolve `tbl.col` via the wildcard chain (already implemented in `_lookup`, but may not be called in all code paths — verify and fix)
2. **Investigate and fix the parser gap**: identify why `src_match_rnk.mlstn_event_cd` has no entry in `tv_sources`. Likely causes:
   - The column comes from `SELECT TABLE.*` in an intra-cell CTE, and the table-qualified wildcard isn't creating the right edge (the `not col_refs` branch may be using `default_table` instead of the table qualifier)
   - OR the intra-cell CTE resolution collapses the edge with a renamed column that breaks the match
3. If resolution still fails, the fallback behavior should still produce a real upstream table (walking wildcard chains) rather than leaving a temp view name as the source

---

## Scope

**In scope:**
- `backend/parsers/sql.py`: fix `_resolve_temp_views` to surface the most meaningful expression in the resolved chain
- `backend/parsers/sql.py`: fix the parser gap causing `mlstn_event_cd`-style columns to have no `tv_sources` entry
- Both bugs must be verified against the live data: `uc_dc_dev.scru_em.ship_ordr_mlstn` columns in the deployed app

**Out of scope / deferred:**
- Re-introducing the `TransformInspector` raw-graph path view (removed due to OOM; separate initiative)
- PySpark parsers
- Other tables not exhibiting these symptoms

---

## Implementation Surface

### `backend/parsers/sql.py` — `_resolve_temp_views`

**Bug 1 fix**: add a helper `_best_expression(chain_edges)` that walks the temp view chain and returns the most semantically meaningful `(transform_type, expression)` pair found. Use this instead of blindly inheriting the consumer edge's fields.

**Bug 2 fix**: audit the `_lookup` wildcard fallback path — confirm it is called for named columns whose temp view has a `.*` entry. Add a test that demonstrates correct resolution for a column that passes through a `SELECT TABLE.*` CTE.

### `backend/parsers/sql.py` — `_parse_select_node`

Investigate whether the `not col_refs` branch correctly handles `SELECT TABLE.*` (table-qualified star). If `sel = Column(this=Star(), table="ML_FM_LM_UPDT")` reaches the `not col_refs` branch, confirm `source_col` uses the table qualifier (`ml_fm_lm_updt.*`) and not `default_table.*`. Fix if incorrect.

---

## Success Criteria

- Clicking `ship_ordr_mlstn.src_sys_cd` in the graph → Transform tab "SQL Logic" shows the COALESCE expression, not `SELECT * FROM SRC_MATCH_RNK`
- Clicking `ship_ordr_mlstn.mlstn_event_cd` → source shown is a real table column, not `src_match_rnk.mlstn_event_cd`
- No regressions in existing backend tests (`python -m pytest tests/ -v`)
- Existing columns with correctly resolved expressions (non-passthrough types) are unaffected
