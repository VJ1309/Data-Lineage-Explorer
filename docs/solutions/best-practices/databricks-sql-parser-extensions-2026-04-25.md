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
---

# Extending the Databricks SQL Parser — Coverage Patterns and SQLGlot Pitfalls

## Context

`backend/parsers/sql.py` uses SQLGlot with `dialect="databricks"` to extract column-level lineage. Several common Databricks constructs either produced zero edges (silently dropped) or misclassified edges before a systematic improvement pass in April 2026. This document captures the extension patterns and SQLGlot-specific pitfalls so future additions don't rediscover them.

The eight extensions implemented cover: QUALIFY/HAVING filter edges, extended aggregate classification, double-quote normalization, MERGE USING subquery lineage, COPY INTO and CLONE detect-and-degrade, and `read_files()`/`cloud_files()` table-valued functions.

## Guidance

### R1 & R2 — QUALIFY and HAVING pseudo-column edges

Both use the existing `__filter__` pattern but with distinct pseudo-column names (`__qualify__` for post-window filtering, `__having__` for post-aggregation filtering). Handle them **before** the main SELECT column walk in `_parse_select_node`:

```python
# In _parse_select_node, before the SELECT walk:
qualify_node = select_node.args.get("qualify")
if qualify_node:
    for col in qualify_node.find_all(exp.Column):
        edges.append(LineageEdge(
            source_table=source_table, source_column=col.name,
            target_table=target_table, target_column="__qualify__",
            transform_type="filter", expression=qualify_node.sql(dialect="databricks"),
        ))

having_node = select_node.args.get("having")
if having_node:
    for col in having_node.find_all(exp.Column):
        edges.append(LineageEdge(
            source_table=source_table, source_column=col.name,
            target_table=target_table, target_column="__having__",
            transform_type="filter", expression=having_node.sql(dialect="databricks"),
        ))
```

`exp.Qualify` is parsed by the Databricks dialect — access it via `select_node.args.get("qualify")`. `exp.Having` is accessed via `select_node.args.get("having")`.

### R3 — Aggregate classification

Add new SQLGlot expression types to the `_classify_transform` aggregation tuple:

```python
AGGREGATE_TYPES = (
    exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min,
    exp.ApproxDistinct,   # APPROX_COUNT_DISTINCT
    exp.Quantile,         # PERCENTILE
    exp.ApproxQuantile,   # PERCENTILE_APPROX
    exp.Stddev,           # STDDEV / STDDEV_SAMP
    exp.StddevSamp,
    exp.StddevPop,
    exp.Variance,         # VARIANCE / VAR_SAMP
    exp.VarPop,
)
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

When `using_node` in `_parse_merge` is `exp.Subquery` (not `exp.Table`), extract column edges from the subquery source and register the subquery alias in `subquery_aliases` so temp-view resolution can chain through it:

```python
elif isinstance(using_node, exp.Subquery):
    alias = using_node.alias or "__merge_src__"
    subquery_aliases.add(alias)
    sub_edges = _process_subquery(using_node.this, alias, _warnings)
    edges.extend(sub_edges)
```

Call `_resolve_temp_views(edges, subquery_aliases)` at the end of `_parse_merge` to collapse subquery alias hops into direct source → target edges.

### R6 — COPY INTO detect-and-degrade

SQLGlot parses `COPY INTO` as `exp.Copy` (not `exp.Command`). Extract the target table from `statement.this` and emit a wildcard approximate edge:

```python
def _parse_copy(statement: exp.Copy, _warnings: list) -> list[LineageEdge]:
    target = statement.this.name if statement.this else None
    if not target:
        return []
    return [LineageEdge(
        source_table="__file__", source_column="*",
        target_table=target, target_column="*",
        transform_type="passthrough", confidence="approximate",
        expression="COPY INTO",
    )]
```

Use `__file__` as the synthetic source name — it visually distinguishes file-load sources from SQL-derived ones in the lineage graph.

### R7 — CLONE detect-and-degrade

`CLONE` and `SHALLOW CLONE` are parsed as `exp.Create` with a `clone` arg. `DEEP CLONE` falls to `exp.Command` (SQLGlot limitation):

```python
# exp.Create branch
clone_node = statement.args.get("clone")
if clone_node:
    source = clone_node.this.name
    target = statement.this.name
    return [LineageEdge(source_table=source, source_column="*",
                        target_table=target, target_column="*",
                        transform_type="passthrough", confidence="approximate")]

# exp.Command fallback for DEEP CLONE
_DEEP_CLONE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+DEEP\s+CLONE\s+(\S+)",
    re.IGNORECASE,
)
def _parse_command_fallback(statement: exp.Command, _warnings: list) -> list[LineageEdge]:
    text = statement.text or ""
    m = _DEEP_CLONE_RE.search(text)
    if m:
        target, source = m.group(1), m.group(2)
        return [LineageEdge(source_table=source, source_column="*",
                            target_table=target, target_column="*",
                            transform_type="passthrough", confidence="approximate")]
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
- SQLGlot issue #6303 — double-quote identifier tokenization bug (Databricks dialect)
- SQLGlot issue #3388 — COPY INTO falls to `exp.Command` (resolved for `exp.Copy` in current version, but DEEP CLONE still falls to Command)
