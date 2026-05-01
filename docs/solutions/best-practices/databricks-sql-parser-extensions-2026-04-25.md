---
title: Extending the Databricks SQL Parser — Coverage Patterns and SQLGlot Pitfalls
date: 2026-04-25
category: docs/solutions/best-practices
module: parsers/sql.py
problem_type: best_practice
component: tooling
severity: medium
applies_when:
  - Adding support for a new Databricks SQL construct in parsers/sql.py
  - Diagnosing silently-dropped column lineage edges
  - Working with SQLGlot AST nodes for Databricks dialect constructs
tags: sql-parser, sqlglot, databricks, lineage, ast, column-lineage, pseudo-column
last_updated: 2026-05-01
---

# Extending the Databricks SQL Parser — Coverage Patterns and SQLGlot Pitfalls

## Context

`backend/parsers/sql.py` uses SQLGlot with `dialect="databricks"` to extract column-level lineage. Several common Databricks constructs either produced zero edges (silently dropped) or misclassified edges before a systematic improvement pass in April 2026. This document captures the extension patterns and SQLGlot-specific pitfalls so future additions don't rediscover them.

The eight extensions implemented cover: QUALIFY/HAVING filter edges, extended aggregate classification, double-quote normalization, MERGE USING subquery lineage, COPY INTO and CLONE detect-and-degrade, and `read_files()`/`cloud_files()` table-valued functions.

## Guidance

### R1 & R2 — QUALIFY and HAVING pseudo-column edges

Both use the existing `__filter__` pattern but with distinct pseudo-column names (`__qualify__` for post-window filtering, `__having__` for post-aggregation filtering). They are handled by the shared `_emit_predicate_edges()` closure inside `_parse_select_node`, alongside the WHERE clause:

```python
# In _parse_select_node — shared predicate edge emitter (closure):
def _emit_predicate_edges(clause: exp.Expression | None, pseudo_col: str, transform_type: str) -> None:
    if clause is None:
        return
    expr_str = clause.sql(dialect="databricks")
    for col_ref in clause.find_all(exp.Column):
        col_name = col_ref.name
        if not col_name:
            continue
        src = f"{resolved_table}.{col_name}"
        edges.append(LineageEdge(
            source_col=src,
            target_col=f"{target_table}.{pseudo_col}",
            transform_type=transform_type,
            expression=expr_str,
            source_file=source_file,
            source_cell=source_cell,
            source_line=source_line,
        ))

# Calls (after SELECT column walk):
_emit_predicate_edges(select_node.args.get("where"),   "__filter__",   "filter")
_emit_predicate_edges(select_node.args.get("qualify"), "__qualify__",  "filter")
_emit_predicate_edges(select_node.args.get("having"),  "__having__",   "filter")
```

`exp.Qualify` is parsed by the Databricks dialect — access it via `select_node.args.get("qualify")`. `exp.Having` is accessed via `select_node.args.get("having")`.

Note: `LineageEdge` uses combined `source_col` and `target_col` fields (format: `"table.column"`), not separate `source_table`/`source_column`/`target_table`/`target_column` kwargs.

### R3 — Aggregate classification

Add new SQLGlot expression types to the `isinstance` check inside `_classify_transform` in `parsers/sql.py`:

```python
# In _classify_transform, the aggregation branch:
for n in all_nodes:
    if isinstance(n, (exp.Sum, exp.Count, exp.Avg, exp.Max, exp.Min,
                      exp.ArrayAgg, exp.GroupConcat,
                      exp.ApproxDistinct,   # APPROX_COUNT_DISTINCT
                      exp.Quantile,         # PERCENTILE
                      exp.ApproxQuantile,   # PERCENTILE_APPROX
                      exp.Stddev, exp.StddevSamp, exp.StddevPop,
                      exp.Variance)):       # VARIANCE / VAR_SAMP
        return "aggregation", expr_str
```

`exp.PercentileIf` does not exist in SQLGlot — use `exp.ApproxQuantile` for `PERCENTILE_APPROX`. Verify class names empirically with `sqlglot.expressions.__dict__` before adding.

### R4 — Double-quote identifier normalization

SQLGlot issue #6303: `"col_name"` is tokenized as a string literal instead of a column reference when using the Databricks dialect. Fix: normalize double-quoted identifiers to backtick equivalents **per statement before `parse_one`**:

```python
_DOUBLE_QUOTED_IDENT_RE = re.compile(r'"([^"]+)"')

def _normalize_double_quotes(sql: str) -> str:
    return _DOUBLE_QUOTED_IDENT_RE.sub(lambda m: f"`{m.group(1)}`", sql)
```

Call this before `sqlglot.parse_one(sql, dialect="databricks")`. Apply per-statement, not at file level, to avoid breaking multi-statement files.

### R5 — MERGE USING subquery lineage

When `using_node` in `_parse_merge` is `exp.Subquery` (not `exp.Table`), extract column edges from the subquery body by calling `_parse_select_node` directly on each union-branch SELECT, and register the subquery alias in `subquery_aliases` so temp-view resolution can chain through it:

```python
elif isinstance(using_node, exp.Subquery):
    sub_alias = using_node.alias or _next_sub_alias()
    alias_map[sub_alias] = sub_alias
    source_tables.append(sub_alias)
    subquery_aliases.add(sub_alias)

# Then, after setting up alias_map, parse the subquery body:
if isinstance(using_node, exp.Subquery):
    sub_alias = next(iter(subquery_aliases), None)
    if sub_alias:
        sub_selects = _collect_union_selects(using_node.this) if using_node.this else []
        for sub_sel in sub_selects:
            edges.extend(_parse_select_node(
                sub_sel, sub_alias, {}, source_file, source_line, source_cell,
                subquery_aliases=subquery_aliases,
            ))
```

Call `resolve_temp_views(edges, subquery_aliases)` at the end of `_parse_merge` to collapse subquery alias hops into direct source → target edges. (`resolve_temp_views` is now a public function exported from `parsers/sql.py`.)

### R6 — COPY INTO detect-and-degrade

SQLGlot parses `COPY INTO` as `exp.Copy` (not `exp.Command`). Extract the qualified target table from `copy_stmt.this` and emit a wildcard approximate edge via the `_wildcard_edge` helper:

```python
def _parse_copy(
    copy_stmt: exp.Copy,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    if not isinstance(copy_stmt.this, exp.Table):
        return []
    target_table = _qualified_table_name(copy_stmt.this)
    return [_wildcard_edge("__file__.*", f"{target_table}.*", source_file, source_line, source_cell)]
```

Use `__file__` as the synthetic source prefix — it visually distinguishes file-load sources from SQL-derived ones in the lineage graph. `_wildcard_edge` sets `confidence="approximate"` and `qualified=False` automatically.

### R7 — CLONE detect-and-degrade

`CLONE` and `SHALLOW CLONE` are parsed as `exp.Create` with a `clone` arg — handled in `_parse_clone`. `DEEP CLONE` falls to `exp.Command` (SQLGlot limitation) — handled in `_parse_command_fallback`:

```python
# _parse_clone for CLONE / SHALLOW CLONE (exp.Create + clone arg):
def _parse_clone(
    create_stmt: exp.Create,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    clone_node = create_stmt.args.get("clone")
    if not clone_node or not isinstance(create_stmt.this, exp.Table):
        return []
    target_table = _qualified_table_name(create_stmt.this)
    if not (isinstance(clone_node, exp.Clone) and isinstance(clone_node.this, exp.Table)):
        return []
    source_table = _qualified_table_name(clone_node.this)
    return [_wildcard_edge(f"{source_table}.*", f"{target_table}.*", source_file, source_line, source_cell)]

# _parse_command_fallback for DEEP CLONE (exp.Command regex fallback):
_DEEP_CLONE_RE = re.compile(
    r'\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+DEEP\s+CLONE\s+(\S+)',
    re.IGNORECASE,
)

def _parse_command_fallback(
    cmd_text: str,          # receives statement.text, not the exp.Command object
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    m = _DEEP_CLONE_RE.search(cmd_text)
    if m:
        target_table = m.group(1).strip(';')
        source_table = m.group(2).strip(';')
        return [_wildcard_edge(f"{source_table}.*", f"{target_table}.*", source_file, source_line, source_cell)]
    return []
```

### R8 — read_files() and cloud_files() TVF support

`read_files(...)` in a FROM clause is parsed as `exp.Table` wrapping `exp.Anonymous` (not a bare `exp.Anonymous`). Check `from_table.this`:

```python
from_table = select_node.args.get("from")
if from_table:
    tbl = from_table.this
    if isinstance(tbl, exp.Table) and isinstance(tbl.this, exp.Anonymous):
        # TVF: synthesize source name from first positional arg (the path)
        fn = tbl.this
        first_arg = fn.expressions[0] if fn.expressions else None
        path = first_arg.this if first_arg else fn.name
        source_name = path.strip("/").replace("/", "_").replace("*", "")
        # register source_name as the source table for this SELECT
    elif isinstance(tbl, exp.Subquery):
        ...
    elif isinstance(tbl, exp.Table):
        source_name = tbl.name
```

The first positional argument to `read_files()` / `cloud_files()` is the path string — use it as the synthetic source table name.

## Why This Matters

Silently-dropped edges are the worst failure mode for a lineage tool — users see partial graphs with no indication of what's missing. Each pattern above converts a silent drop into either a concrete edge or an approximate wildcard edge, so the graph is at minimum structurally correct even when column-level detail isn't available.

The `confidence="approximate"` flag on COPY INTO and CLONE edges signals to consumers that the lineage is structural rather than traced, preserving trust in the non-approximate portions.

## When to Apply

- Any time a new Databricks SQL construct produces zero lineage edges that it should produce.
- Before assuming a construct is unsupported, check whether it falls to `exp.Command` (DEEP CLONE pattern) or wraps an `exp.Anonymous` (TVF pattern).
- When adding new aggregate functions — verify the SQLGlot expression class name exists in the installed version before adding to the tuple.
- When SQLGlot is upgraded — re-verify that `exp.Copy` still covers COPY INTO, `exp.Create` + `clone` arg still covers CLONE/SHALLOW CLONE, and `exp.Command` is still needed for DEEP CLONE.

## Examples

**QUALIFY filter edge:**
```sql
INSERT INTO result
SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn
FROM t
QUALIFY rn = 1
-- Emits: t.rn → result.__qualify__ (transform_type="filter")
```

**HAVING filter edge:**
```sql
INSERT INTO result
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id HAVING SUM(amount) > 1000
-- Emits: orders.amount → result.__having__ (transform_type="filter")
```

**Double-quote normalization:**
```sql
-- Input (double-quoted ANSI style)
SELECT "order_id" FROM orders
-- After normalization
SELECT `order_id` FROM orders
-- Emits: orders.order_id → result.order_id  (was silently dropped before)
```

**COPY INTO wildcard edge:**
```sql
COPY INTO my_catalog.my_schema.my_table
FROM 'abfss://...' FILEFORMAT = PARQUET
-- Emits: __file__.* → my_catalog.my_schema.my_table.* (confidence="approximate")
```

**CLONE wildcard edge:**
```sql
CREATE TABLE my_catalog.schema.new_table CLONE my_catalog.schema.source_table
-- Emits: my_catalog.schema.source_table.* → my_catalog.schema.new_table.* (confidence="approximate")
```

**read_files() TVF:**
```sql
SELECT id, name FROM read_files('/mnt/landing/orders/*.parquet', format => 'parquet')
-- Source name synthesized from path; emits edges from synthetic source to target columns
```

## Related

- `backend/parsers/sql.py` — implementation; all patterns above live in `_parse_select_node`, `_parse_merge`, `_parse_copy`, `_parse_clone`, `_parse_command_fallback`
- `backend/tests/test_sql_parser.py` — 19 tests covering R1–R8 (added in commit `5075543`)
- `docs/brainstorms/2026-04-25-databricks-sql-parser-improvements-requirements.md` — original requirements with acceptance examples
- `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md` — `ParseResult` return type, helper signature conventions (`source_file`/`source_line`/`source_cell` provenance), `source_col`/`target_col` combined field format, promoted public helpers (`resolve_temp_views`, `detect_temp_views`, `split_databricks_sql`)
- SQLGlot issue #6303 — double-quote identifier tokenization bug (Databricks dialect)
- SQLGlot issue #3388 — COPY INTO falls to `exp.Command` (resolved for `exp.Copy` in current version, but DEEP CLONE still falls to Command)
