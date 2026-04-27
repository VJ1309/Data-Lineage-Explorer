---
date: 2026-04-25
topic: databricks-sql-parser-improvements
---

# Databricks SQL Parser Improvements

## Problem Frame

The SQL parser (`backend/parsers/sql.py`) uses SQLGlot with `dialect="databricks"` to extract column-level lineage from uploaded Databricks SQL notebooks. While the parser already handles a broad set of constructs, several common Databricks patterns produce either zero edges (silently dropped) or misclassified edges (wrong transform type). This is a correctness and coverage pass — a systematic improvement while the codebase is fresh — covering expression accuracy, statement coverage, function coverage, and parse-failure observability.

---

## Requirements

**Expression accuracy**

- R1. Emit `__qualify__` pseudo-column edges for columns referenced in a `QUALIFY` clause (window function row-filtering), using `transform_type="filter"`. Pattern mirrors the existing `WHERE` → `__filter__` implementation. Research confirms `exp.Qualify` is already parsed by SQLGlot's Databricks dialect.
- R2. Emit `__having__` pseudo-column edges for columns referenced in a `HAVING` clause, using `transform_type="filter"`. Pattern mirrors `WHERE` → `__filter__`. SQLGlot exposes the HAVING clause on `exp.Select` via `having`.
- R3. Extend `_classify_transform` to classify the following additional SQLGlot aggregate node types as `"aggregation"` (they currently fall through to `"expression"`): `exp.ApproxDistinct`, `exp.Quantile`, `exp.Stddev`, `exp.StddevPop`, `exp.Variance`, `exp.VarPop`, `exp.Percentile`, `exp.PercentileIf`.
- R4. Normalize double-quoted identifiers to backtick equivalents before calling `sqlglot.parse_one`. This corrects SQLGlot issue #6303 where `"col_name"` is tokenized as a string literal instead of a column reference, silently losing column lineage for queries written with ANSI double-quote quoting.

**Statement coverage**

- R5. Handle `MERGE INTO t USING (SELECT ...) AS s ON ...` — extend `_parse_merge` to detect when `using_node` is `exp.Subquery` and call `_process_subquery` to extract source column edges. Currently the subquery source is silently ignored, producing no lineage from the USING branch.
- R6. Handle `COPY INTO target FROM 'path' ...` — detect these statements via `exp.Command` fallback (SQLGlot issue #3388 confirmed this construct falls to `exp.Command`) and emit a synthetic `__file__.*` → `target_table.*` wildcard edge with `confidence="approximate"` and `transform_type="passthrough"`. The target table name must be extracted from the command text.
- R7. Handle `CREATE [SHALLOW/DEEP] CLONE source TO target` — detect via `exp.Command` fallback and emit a `source_table.*` → `target_table.*` wildcard edge with `confidence="approximate"` and `transform_type="passthrough"`. Supports shallow and deep clone variants.

**Function coverage**

- R8. Handle `read_files(...)` and `cloud_files(...)` table-valued functions appearing in a `FROM` clause — when `from_clause.this` is `exp.Anonymous` (not `exp.Table` or `exp.Subquery`), synthesize a source name from the function's first positional argument (the path string), and register it as the source table for that SELECT.

**Observability**

- R9. Expose accumulated parse warnings (`_warnings` list in `parse_sql`) through the backend API so users can see which SQL cells or files failed to parse and why. The surface point (new endpoint vs. field in existing response) is a planning decision.

---

## Acceptance Examples

- AE1. **Covers R1.** Given `INSERT INTO result SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM t QUALIFY rn = 1`, the parser emits an edge `t.rn → result.__qualify__` with `transform_type="filter"`.

- AE2. **Covers R2.** Given `INSERT INTO result SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id HAVING SUM(amount) > 1000`, the parser emits an edge `orders.amount → result.__having__` with `transform_type="filter"`.

- AE3. **Covers R3.** Given `SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM events`, the edge for `approx_users` has `transform_type="aggregation"`, not `"expression"`.

- AE4. **Covers R4.** Given SQL containing `SELECT "order_id" FROM orders`, the parser correctly produces an edge `orders.order_id → result.order_id` rather than silently dropping the column reference.

- AE5. **Covers R5.** Given `MERGE INTO target t USING (SELECT id, val FROM staging WHERE active = 1) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.val = s.val`, the parser emits `staging.val → target.val`.

- AE6. **Covers R6.** Given `COPY INTO my_catalog.my_schema.my_table FROM 'abfss://...' FILEFORMAT = PARQUET`, the parser emits a wildcard edge `__file__.* → my_catalog.my_schema.my_table.*` with `confidence="approximate"`.

- AE7. **Covers R7.** Given `CREATE TABLE my_catalog.schema.new_table CLONE my_catalog.schema.source_table`, the parser emits `my_catalog.schema.source_table.* → my_catalog.schema.new_table.*` with `confidence="approximate"`.

- AE8. **Covers R8.** Given `SELECT id, name FROM read_files('/mnt/landing/orders/*.parquet', format => 'parquet')`, the parser treats `read_files('/mnt/landing/orders/*.parquet', ...)` as a synthetic source named after the path and emits edges from it to the target columns.

---

## Success Criteria

- QUALIFY and HAVING columns appear in the lineage graph as `__qualify__` and `__having__` pseudo-nodes, visible and filterable in the same way `__filter__` and `__joinkey__` are today.
- COPY INTO and CLONE targets appear in the catalog and lineage graph; upstream file/table paths are visible as approximate sources rather than being invisible.
- MERGE statements with subquery sources trace column lineage all the way to the underlying staging tables, not just to the target.
- Aggregate functions `APPROX_COUNT_DISTINCT`, `STDDEV`, `VARIANCE`, `PERCENTILE_APPROX` are classified as `"aggregation"` in the lineage tree.
- Users can see which uploaded SQL cells failed to parse without inspecting server logs.
- All existing tests continue to pass; each new behavior is covered by at least one test in `backend/tests/test_sql_parser.py`.

---

## Scope Boundaries

- Delta Live Tables (`CREATE STREAMING LIVE TABLE`, `CREATE LIVE TABLE`, `APPLY CHANGES INTO`) — explicitly out of scope. These constructs are not yet needed by the user's upload workflow (`.sql` notebook files). Emit a parse warning when detected; do not attempt column-level lineage.
- `SELECT *` expansion to named columns — not feasible without Unity Catalog schema integration; wildcard edges (`source.*`) remain the correct output.
- Lambda / higher-order function column extraction (`transform(arr, x -> x.field)`) — too involved; lambda body column references are lost, which is the current behavior and acceptable.
- `MERGE WITH SCHEMA EVOLUTION` modifier — out of scope; if it causes a parse failure, the warning system (R9) will surface it.
- Unity Catalog schema injection via `sqlglot.optimizer.qualify` — deferred; requires catalog integration that does not yet exist.
- Frontend rendering changes for new pseudo-column types (`__qualify__`, `__having__`) — follow-up after backend ships; existing pseudo-column rendering already handles unknown suffixes.

---

## Key Decisions

- **`exp.Command` as the detection point for COPY INTO and CLONE:** Research confirmed SQLGlot issue #3388 — `COPY INTO` falls to `exp.Command`, not a structured node. Regex extraction on command text is the correct approach, not pre-processing SQL before `parse_one`.
- **Separate pseudo-column names for QUALIFY and HAVING:** `__qualify__` and `__having__` are distinct from `__filter__` because they have different semantics (post-window vs. post-aggregation row filtering). `transform_type` stays `"filter"` for all three — no model changes required.
- **`confidence="approximate"` for COPY INTO and CLONE edges:** These emit wildcard edges without column-level detail. Marking them approximate signals to consumers that the lineage is structural, not traced.
- **R4 normalization applies before `parse_one` per statement, not at file level:** The double-quote normalization is cheap and safe to apply per-statement; it avoids breaking string literals that happen to look like identifiers.
- **`__file__` as the synthetic source table name for COPY INTO:** Provides a recognizable prefix in the lineage graph that distinguishes file-load sources from SQL-derived sources.

---

## Dependencies / Assumptions

- The 4-part column ID invariant (`catalog.schema.table.column`, always split at the rightmost dot via `rsplit(".", 1)`) must be preserved throughout all new handlers. Partial IDs must not surface to the graph.
- New pseudo-column edge types (`__qualify__`, `__having__`) follow the existing per-edge expression text pattern: one `LineageEdge` per source column, full clause expression text on each edge. The consumer layer (`routes.py list_columns`) already handles multi-predecessor aggregation.
- `CREATE TABLE ... CLONE` likely produces `exp.Command`, consistent with COPY INTO behavior, but this must be verified empirically during planning with a test parse call.
- `exp.Qualify` is available in the installed SQLGlot version — verify during planning that `select_node.args.get("qualify")` returns a non-None value for QUALIFY queries.
- The `_warnings` parameter already threads through `parse_sql` and `_parse_single_statement` — the observability work (R9) is primarily an API routing change, not a parser change.

---

## Outstanding Questions

### Resolve Before Planning

*(none)*

### Deferred to Planning

- [Affects R6, R7][Technical] Verify `exp.Command` fallback: confirm `statement.name` vs. `statement.args.get("expression")` is the right way to reconstruct command text for regex matching in SQLGlot's current version.
- [Affects R7][Technical] Empirically verify whether `CREATE TABLE ... CLONE` produces `exp.Command` or a parseable AST node (e.g., `exp.Clone`). SQLGlot added CLONE support recently; if it's a structured node, R7 may require a dedicated handler rather than a regex fallback.
- [Affects R9][Technical] Decide the API surface for parse warnings: new `GET /api/backend/sources/{id}/warnings` endpoint, or add a `warnings` array to the existing source/tables response shape.
- [Affects R3][Needs research] Confirm exact SQLGlot class names for `PercentileApprox` and `CollectList` / `CollectSet` — the SQLGlot expression hierarchy uses non-obvious names for some Spark/Databricks-specific aggregates.

---

## Next Steps

-> `/ce-plan` for structured implementation planning
