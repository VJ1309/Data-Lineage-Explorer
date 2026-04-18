# Tier 1 Parser Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close correctness gaps in the SQL parser (JOIN column attribution, normalization ambiguity, partial-parse failures, missing statement types) and surface the new signal through the UI so users can tell which edges are trusted and which are not.

**Architecture:**
- Backend: introduce a per-parse `ParseContext` to replace the module-global counter, extend `LineageEdge` with a `qualified: bool` field, add support for MERGE INTO / LATERAL VIEW / PIVOT, emit `filter` and `join_key` edges from predicates, switch to per-statement parsing with partial-success fallback, and fix `_normalize_edges` so it never merges two distinct tables into one.
- Frontend: extend `LineageEdge` type, dim approximate/unqualified edges, add toggles for filter and join-key visibility (off by default), enhance the warnings panel with severity, and show an ambiguity banner when normalization declines to merge.
- Every backend change is test-driven; frontend changes are verified in the browser.

**Tech Stack:** Python 3.11, SQLGlot (databricks dialect), pytest, Next.js App Router, React, Tailwind, @xyflow/react.

---

## File Map

| File | What changes |
|---|---|
| `backend/lineage/models.py` | Add `qualified: bool = True` to `LineageEdge`; add `severity: Literal["info","warn","error"] = "warn"` to `ParseWarning` |
| `backend/parsers/sql.py` | New `ParseContext` dataclass; replace `_subquery_counter`; add MERGE/LATERAL VIEW/PIVOT handlers; emit filter/join_key edges; per-statement parse loop with partial success; qualified=False for unresolved table hints |
| `backend/lineage/engine.py` | `_normalize_edges` refuses ambiguous suffix merges and emits a warning; thread qualified through |
| `backend/api/routes.py` | Serialize `qualified` on `_edge_to_dict`; add `severity` to warnings payload |
| `backend/tests/test_sql_parser.py` | ~10 new failing-first tests |
| `backend/tests/test_engine.py` | Ambiguity-merge rejection test |
| `backend/tests/test_routes.py` | Verify `qualified` and `severity` in payloads |
| `frontend/lib/api.ts` | Add `qualified: boolean` to `LineageEdge`; add `severity` to `Warning` |
| `frontend/components/lineage-graph.tsx` | Dashed stroke + reduced opacity for `qualified === false`; optional rendering of filter/join_key edge types; new legend entries |
| `frontend/components/lineage-tree.tsx` | "~" marker and muted style on unqualified edges; filter/join_key rows styled distinctly |
| `frontend/components/transform-badge.tsx` | Add styling for `filter` and `join_key` types |
| `frontend/app/lineage/page.tsx` | Edge-type filter controls (data edges always on, filter/join_key toggleable, default off) |
| `frontend/app/sources/page.tsx` | Severity-coloured warnings panel |

---

## Task 1: Add `qualified` field to LineageEdge (data model)

**Files:**
- Modify: `backend/lineage/models.py`
- Test: `backend/tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Add to `backend/tests/test_models.py`:

```python
def test_lineage_edge_has_qualified_field_defaulting_true():
    from lineage.models import LineageEdge
    e = LineageEdge(source_col="a.b", target_col="c.d", transform_type="passthrough")
    assert e.qualified is True

def test_lineage_edge_qualified_can_be_set_false():
    from lineage.models import LineageEdge
    e = LineageEdge(source_col="a.b", target_col="c.d", transform_type="passthrough", qualified=False)
    assert e.qualified is False
```

- [ ] **Step 2: Run to see failure**

`cd backend && python -m pytest tests/test_models.py -v`

Expected: FAIL — `TypeError: unexpected keyword argument 'qualified'`.

- [ ] **Step 3: Add the field**

In `backend/lineage/models.py`, update `LineageEdge`:

```python
@dataclass
class LineageEdge:
    source_col: str
    target_col: str
    transform_type: Literal[
        "passthrough", "aggregation", "expression",
        "join_key", "window", "cast", "filter"
    ]
    expression: str | None = None
    source_file: str = ""
    source_cell: int | None = None
    source_line: int | None = None
    confidence: Literal["certain", "approximate"] = "certain"
    qualified: bool = True
```

- [ ] **Step 4: Run to confirm pass**

`cd backend && python -m pytest tests/test_models.py -v` — PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/lineage/models.py backend/tests/test_models.py
git commit -m "feat(models): add qualified flag to LineageEdge"
```

---

## Task 2: Add `severity` field to ParseWarning

**Files:**
- Modify: `backend/lineage/models.py`
- Test: `backend/tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Add:

```python
def test_parse_warning_default_severity_is_warn():
    from lineage.models import ParseWarning
    w = ParseWarning(file="x.sql", error="oops")
    assert w.severity == "warn"

def test_parse_warning_severity_can_be_error():
    from lineage.models import ParseWarning
    w = ParseWarning(file="x.sql", error="boom", severity="error")
    assert w.severity == "error"
```

- [ ] **Step 2: Run to fail**
- [ ] **Step 3: Update `ParseWarning`**

```python
@dataclass
class ParseWarning:
    file: str
    error: str
    severity: Literal["info", "warn", "error"] = "warn"
```

- [ ] **Step 4: Run to pass**
- [ ] **Step 5: Commit**

```bash
git add backend/lineage/models.py backend/tests/test_models.py
git commit -m "feat(models): add severity to ParseWarning"
```

---

## Task 3: Replace module-global counter with `ParseContext`

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test (proves no global leak)**

```python
def test_subquery_aliases_deterministic_across_calls():
    """Repeated parse calls must produce identical synthetic subquery aliases."""
    sql = "INSERT INTO t SELECT x FROM (SELECT x FROM src) sub1 JOIN (SELECT x FROM src2) ON 1=1"
    from parsers.sql import parse_sql
    edges_a = parse_sql(sql, source_file="a.sql", source_line=1)
    edges_b = parse_sql(sql, source_file="a.sql", source_line=1)
    sources_a = sorted({e.source_col for e in edges_a})
    sources_b = sorted({e.source_col for e in edges_b})
    assert sources_a == sources_b, "synthetic alias changed across calls — global state leak"
```

- [ ] **Step 2: Run to fail** — `__sub_N__` counter drifts across calls.

- [ ] **Step 3: Introduce `ParseContext`**

In `backend/parsers/sql.py`, remove the `_subquery_counter` globals and `_next_sub_alias()`. Add:

```python
from dataclasses import dataclass, field

@dataclass
class ParseContext:
    source_file: str
    source_line: int | None
    source_cell: int | None
    subquery_aliases: set[str] = field(default_factory=set)
    _counter: int = 0

    def next_sub_alias(self) -> str:
        self._counter += 1
        return f"__sub_{self._counter}__"
```

Thread `ctx: ParseContext` through `_parse_select_node`, `_process_subquery`, `_parse_single_statement`. The `source_file / source_line / source_cell` plumbing collapses into `ctx`. Every call in `parse_sql` that previously started a fresh top-level parse constructs a new `ParseContext` so counters are local.

- [ ] **Step 4: Run full SQL test suite** — all pass.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "refactor(sql): replace module-global counter with per-parse ParseContext"
```

---

## Task 4: Mark unqualified-in-JOIN column references as `qualified=False`

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_unqualified_column_in_join_is_marked_unqualified():
    sql = """
    INSERT INTO result
    SELECT id FROM table_a JOIN table_b ON table_a.id = table_b.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    result_edges = [e for e in edges if e.target_col == "result.id"]
    assert result_edges, "should emit at least one edge for result.id"
    assert all(e.qualified is False for e in result_edges), \
        "ambiguous column in JOIN must be flagged qualified=False"

def test_qualified_column_in_join_stays_qualified():
    sql = """
    INSERT INTO result
    SELECT table_a.id FROM table_a JOIN table_b ON table_a.id = table_b.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    e = next(e for e in edges if e.target_col == "result.id")
    assert e.qualified is True
```

- [ ] **Step 2: Run to fail** — field defaults to True.

- [ ] **Step 3: Emit `qualified=False` for bare-column refs in multi-source SELECTs**

In `_parse_select_node`, after source tables are collected, compute `multi_source = len(source_tables) > 1` once. In the column-reference emission block, replace the call:

```python
if table_hint:
    resolved_table, certain = _resolve_table_hint(table_hint)
    qualified = True
else:
    resolved_table, certain = default_table, True
    qualified = not multi_source  # ambiguous when >1 source and no table prefix
```

Thread `qualified` into every `LineageEdge(...)` construction inside the function (window and regular branches). For the no-col-refs branch (constants, literals), keep `qualified=True`.

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): mark unqualified JOIN column refs as qualified=False"
```

---

## Task 5: Per-statement parse with partial-success fallback

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_one_bad_statement_does_not_drop_good_statements():
    sql = """
    INSERT INTO good_table SELECT id FROM source_table;
    THIS IS NOT VALID SQL !!!###;
    INSERT INTO other_good SELECT name FROM source_table2;
    """
    warnings: list[str] = []
    edges = parse_sql(sql, source_file="mixed.sql", source_line=1, _warnings=warnings)
    targets = {e.target_col for e in edges}
    assert "good_table.id" in targets, "first good statement lost"
    assert "other_good.name" in targets, "third good statement lost"
    assert any(w for w in warnings), "bad statement must surface a warning"
```

- [ ] **Step 2: Run to fail** — current `sqlglot.parse` raises on any invalid statement, entire file returns `[]`.

- [ ] **Step 3: Split on top-level semicolons, parse per-statement**

In `parse_sql`, replace the single `sqlglot.parse(sql)` block with:

```python
from sqlglot import tokens
def _split_top_level_statements(sql: str) -> list[str]:
    """Split on top-level ';'. Uses SQLGlot's tokenizer to respect strings/comments."""
    try:
        toks = list(tokens.Tokenizer().tokenize(sql))
    except Exception:
        return [sql]
    parts: list[str] = []
    start = 0
    for tok in toks:
        if tok.token_type == tokens.TokenType.SEMICOLON:
            parts.append(sql[start:tok.start])
            start = tok.end + 1
    tail = sql[start:]
    if tail.strip():
        parts.append(tail)
    return [p for p in parts if p.strip()]
```

Then in `parse_sql`, replace:

```python
try:
    statements = sqlglot.parse(sql, dialect="databricks")
except Exception as exc:
    if _warnings is not None:
        _warnings.append(str(exc))
    return []
```

with:

```python
statements: list[exp.Expression | None] = []
for stmt_sql in _split_top_level_statements(sql):
    try:
        parsed = sqlglot.parse_one(stmt_sql, dialect="databricks")
    except Exception as exc:
        if _warnings is not None:
            _warnings.append(f"{exc}: {stmt_sql[:80].strip()!r}")
        continue
    if parsed is not None:
        statements.append(parsed)
```

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): per-statement parse so one bad statement doesn't drop good ones"
```

---

## Task 6: MERGE INTO support

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_merge_into_matched_update_emits_edges():
    sql = """
    MERGE INTO target_table t
    USING source_table s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val, t.status = s.status
    WHEN NOT MATCHED THEN INSERT (id, val, status) VALUES (s.id, s.val, s.status)
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "target_table.val" in targets
    assert "target_table.status" in targets
    assert "source_table.val" in sources
    assert "source_table.status" in sources
```

- [ ] **Step 2: Run to fail** — `_parse_single_statement` only handles Insert/Create/Select.

- [ ] **Step 3: Add MERGE handler**

In `backend/parsers/sql.py`, add a dedicated helper immediately before `_parse_single_statement`:

```python
def _parse_merge(
    merge: exp.Merge,
    ctx: ParseContext,
) -> list[LineageEdge]:
    """Emit edges for MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED."""
    edges: list[LineageEdge] = []
    target = merge.this
    using = merge.args.get("using")
    if not isinstance(target, exp.Table) or using is None:
        return edges
    target_name = _qualified_table_name(target)

    # Resolve USING source
    if isinstance(using, exp.Table):
        source_name = _qualified_table_name(using)
        source_alias = using.alias or source_name
    elif isinstance(using, exp.Subquery):
        source_alias = using.alias or ctx.next_sub_alias()
        source_name = source_alias
        ctx.subquery_aliases.add(source_alias)
        sub_selects = _collect_union_selects(using.this) if using.this else []
        for sub_sel in sub_selects:
            edges.extend(_parse_select_node(sub_sel, source_alias, {}, ctx))
    else:
        return edges

    target_alias = target.alias or target_name
    alias_map = {target_alias: target_name, source_alias: source_name}

    def _resolve(col: exp.Column) -> tuple[str, bool]:
        hint = col.table
        if hint in alias_map:
            return alias_map[hint], True
        # bare column — prefer target if it exists in this MERGE, else source
        return target_name, False

    # WHEN MATCHED UPDATE SET col = expr
    for when in merge.args.get("whens") or []:
        then = when.args.get("then")
        if isinstance(then, exp.Update):
            for assignment in then.args.get("expressions") or []:
                if not isinstance(assignment, exp.EQ):
                    continue
                lhs = assignment.this
                rhs = assignment.expression
                if not isinstance(lhs, exp.Column):
                    continue
                target_col = f"{target_name}.{lhs.name}"
                for col_ref in rhs.find_all(exp.Column):
                    resolved_tbl, qualified = _resolve(col_ref)
                    edges.append(LineageEdge(
                        source_col=f"{resolved_tbl}.{col_ref.name}",
                        target_col=target_col,
                        transform_type=_classify_transform(rhs)[0],
                        expression=rhs.sql(dialect="databricks"),
                        source_file=ctx.source_file,
                        source_line=ctx.source_line,
                        source_cell=ctx.source_cell,
                        confidence="certain" if qualified else "approximate",
                        qualified=qualified,
                    ))
        elif isinstance(then, exp.Insert):
            cols = then.this.expressions if isinstance(then.this, exp.Schema) else []
            values = then.expression
            if isinstance(values, exp.Tuple):
                val_exprs = values.expressions
            elif isinstance(values, exp.Values):
                val_exprs = values.expressions[0].expressions if values.expressions else []
            else:
                val_exprs = []
            for col_ident, val_expr in zip(cols, val_exprs):
                col_name = col_ident.name if isinstance(col_ident, exp.Column) else str(col_ident)
                target_col = f"{target_name}.{col_name}"
                for col_ref in val_expr.find_all(exp.Column):
                    resolved_tbl, qualified = _resolve(col_ref)
                    edges.append(LineageEdge(
                        source_col=f"{resolved_tbl}.{col_ref.name}",
                        target_col=target_col,
                        transform_type="passthrough",
                        expression=val_expr.sql(dialect="databricks"),
                        source_file=ctx.source_file,
                        source_line=ctx.source_line,
                        source_cell=ctx.source_cell,
                        confidence="certain" if qualified else "approximate",
                        qualified=qualified,
                    ))
    return edges
```

In `_parse_single_statement`, add at the top:

```python
if isinstance(statement, exp.Merge):
    ctx = ParseContext(source_file=source_file, source_line=source_line, source_cell=source_cell)
    return _parse_merge(statement, ctx)
```

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): parse MERGE INTO with MATCHED/NOT MATCHED branches"
```

---

## Task 7: LATERAL VIEW / EXPLODE support

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_lateral_view_explode_traces_to_array_column():
    sql = """
    INSERT INTO exploded
    SELECT id, tag
    FROM source_table
    LATERAL VIEW EXPLODE(tags) t AS tag
    """
    edges = parse_sql(sql, source_file="lv.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "source_table.tags" in sources, "exploded column must trace to array source"
    assert "source_table.id" in sources
```

- [ ] **Step 2: Run to fail** — LATERAL VIEW currently ignored.

- [ ] **Step 3: Handle LATERAL VIEW in `_parse_select_node`**

Right after the JOIN loop in `_parse_select_node`, add:

```python
for lv in (select_node.args.get("laterals") or []):
    # SQLGlot parses `LATERAL VIEW EXPLODE(col) t AS x` into a Lateral node.
    # The table alias (t) maps to the EXPLODE source table; the column alias (x)
    # refers to individual elements of the array col.
    inner_call = lv.this  # e.g. exp.Explode(this=<Column 'tags'>)
    lateral_alias = lv.alias or ""
    # Output columns declared after AS
    output_cols: list[str] = []
    if lv.args.get("alias") and hasattr(lv.args["alias"], "columns"):
        output_cols = [c.name for c in lv.args["alias"].columns]
    # Map lateral output cols back to their array column source.
    array_cols = list(inner_call.find_all(exp.Column)) if inner_call else []
    for out_col in output_cols:
        for arr_col in array_cols:
            resolved_table, qualified_hint = (
                _resolve_table_hint(arr_col.table) if arr_col.table
                else (default_table, not multi_source)
            )
            alias_map[f"{lateral_alias}.{out_col}"] = resolved_table
    # The lateral output also becomes addressable via the lateral alias itself.
    if lateral_alias and array_cols:
        first_arr = array_cols[0]
        resolved_table, _ = (
            _resolve_table_hint(first_arr.table) if first_arr.table
            else (default_table, True)
        )
        alias_map[lateral_alias] = resolved_table
```

Note: `_resolve_table_hint` and `default_table`/`multi_source` are already defined earlier in the function; this block must be inserted after they exist. If `multi_source` is not yet defined in your version, define it at the point where sources are finalised: `multi_source = len(source_tables) > 1`.

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): trace lineage through LATERAL VIEW EXPLODE"
```

---

## Task 8: PIVOT support (best-effort: attribute pivoted cols to the aggregated source column)

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_pivot_attributes_pivoted_cols_to_aggregated_source():
    sql = """
    INSERT INTO pivoted
    SELECT * FROM (SELECT region, amount FROM sales)
    PIVOT (SUM(amount) FOR region IN ('NA' AS na_total, 'EU' AS eu_total))
    """
    edges = parse_sql(sql, source_file="p.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "sales.amount" in sources, "pivoted columns must trace back to aggregated column"
```

- [ ] **Step 2: Run to fail**.

- [ ] **Step 3: Handle PIVOT**

SQLGlot exposes pivot nodes under `select.args["from_"].this.args.get("pivots")` when the FROM is a subquery or table. In `_parse_select_node`, after FROM/JOIN resolution, add:

```python
pivots: list[exp.Pivot] = []
if isinstance(from_table, exp.Table) and from_table.args.get("pivots"):
    pivots = list(from_table.args["pivots"])
elif isinstance(from_table, exp.Subquery) and from_table.args.get("pivots"):
    pivots = list(from_table.args["pivots"])

for pivot in pivots:
    # Aggregated expressions live in pivot.expressions; value aliases in pivot.args["fields"][0].expressions
    agg_exprs = pivot.expressions or []
    value_defs = []
    fields = pivot.args.get("fields") or []
    if fields and isinstance(fields[0], exp.In):
        value_defs = fields[0].expressions
    for value_def in value_defs:
        # "'NA' AS na_total"  -> alias = na_total
        pivot_alias = value_def.alias if isinstance(value_def, exp.Alias) else value_def.name
        if not pivot_alias:
            continue
        target_col = f"{target_table}.{pivot_alias}"
        for agg in agg_exprs:
            for col_ref in agg.find_all(exp.Column):
                resolved_table, qualified = (
                    _resolve_table_hint(col_ref.table) if col_ref.table
                    else (default_table, not multi_source)
                )
                edges.append(LineageEdge(
                    source_col=f"{resolved_table}.{col_ref.name}",
                    target_col=target_col,
                    transform_type="aggregation",
                    expression=agg.sql(dialect="databricks"),
                    source_file=ctx.source_file,
                    source_line=ctx.source_line,
                    source_cell=ctx.source_cell,
                    confidence="certain" if qualified else "approximate",
                    qualified=qualified,
                ))
```

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): emit pivot lineage edges through aggregated source column"
```

---

## Task 9: Emit `filter` edges from WHERE predicates

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_where_clause_emits_filter_edges():
    sql = """
    INSERT INTO high_value
    SELECT id FROM orders WHERE amount > 1000 AND status = 'paid'
    """
    edges = parse_sql(sql, source_file="w.sql", source_line=1)
    filter_edges = [e for e in edges if e.transform_type == "filter"]
    filter_sources = {e.source_col for e in filter_edges}
    assert "orders.amount" in filter_sources
    assert "orders.status" in filter_sources
    # Filter edges are attached to the target table so impact analysis works
    assert all(e.target_col.startswith("high_value.") for e in filter_edges)
```

- [ ] **Step 2: Run to fail.**

- [ ] **Step 3: In `_parse_select_node`, after select expressions are processed, emit WHERE edges**

```python
where = select_node.args.get("where")
if where is not None:
    for col_ref in where.find_all(exp.Column):
        resolved_table, qualified = (
            _resolve_table_hint(col_ref.table) if col_ref.table
            else (default_table, not multi_source)
        )
        edges.append(LineageEdge(
            source_col=f"{resolved_table}.{col_ref.name}",
            target_col=f"{target_table}.__filter__",
            transform_type="filter",
            expression=where.sql(dialect="databricks"),
            source_file=ctx.source_file,
            source_line=ctx.source_line,
            source_cell=ctx.source_cell,
            confidence="certain" if qualified else "approximate",
            qualified=qualified,
        ))
```

The `__filter__` pseudo-column is how we attach predicate lineage to the target without contaminating column-level edges. The frontend filters them out by default.

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): emit filter edges from WHERE predicates"
```

---

## Task 10: Emit `join_key` edges from JOIN ON predicates

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

```python
def test_join_on_emits_join_key_edges():
    sql = """
    INSERT INTO joined
    SELECT a.id FROM a JOIN b ON a.key = b.key
    """
    edges = parse_sql(sql, source_file="j.sql", source_line=1)
    jk_edges = [e for e in edges if e.transform_type == "join_key"]
    jk_sources = {e.source_col for e in jk_edges}
    assert "a.key" in jk_sources
    assert "b.key" in jk_sources
    assert all(e.target_col == "joined.__joinkey__" for e in jk_edges)
```

- [ ] **Step 2: Run to fail.**
- [ ] **Step 3: In the JOIN loop of `_parse_select_node`, emit edges from the `on` predicate**

Inside the existing `for join in (select_node.args.get("joins") or []):` loop, after handling the table/subquery, add:

```python
on_expr = join.args.get("on")
if on_expr is not None:
    for col_ref in on_expr.find_all(exp.Column):
        resolved_table, qualified = (
            _resolve_table_hint(col_ref.table) if col_ref.table
            else (default_table, not multi_source)
        )
        edges.append(LineageEdge(
            source_col=f"{resolved_table}.{col_ref.name}",
            target_col=f"{target_table}.__joinkey__",
            transform_type="join_key",
            expression=on_expr.sql(dialect="databricks"),
            source_file=ctx.source_file,
            source_line=ctx.source_line,
            source_cell=ctx.source_cell,
            confidence="certain" if qualified else "approximate",
            qualified=qualified,
        ))
```

Note: `multi_source` at this point is computed using `source_tables` as it exists at that moment. To keep the join-key block future-proof, compute `multi_source = len(source_tables) > 1` once at the end of the JOIN/FROM resolution block, *before* the JOIN-ON emission runs. Move the `multi_source = ...` line accordingly if needed — the same variable is referenced in Tasks 4/7/8/9.

- [ ] **Step 4: Run SQL suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat(sql): emit join_key edges from JOIN ON predicates"
```

---

## Task 11: `_normalize_edges` refuses ambiguous suffix merges

**Files:**
- Modify: `backend/lineage/engine.py`
- Test: `backend/tests/test_engine.py`

- [ ] **Step 1: Write the failing test**

```python
def test_ambiguous_suffix_not_merged_emits_warning():
    """If 'orders' matches both 'staging.orders' and 'prod.orders', do not merge — warn instead."""
    from lineage.engine import build_graph_with_warnings
    from lineage.models import FileRecord
    content = """
    INSERT INTO staging.orders SELECT id FROM raw_source;
    INSERT INTO prod.orders SELECT id FROM raw_source;
    INSERT INTO downstream SELECT id FROM orders;
    """
    rec = FileRecord(path="f.sql", content=content, type="sql", source_ref="t")
    graph, warnings = build_graph_with_warnings([rec])
    nodes = set(graph.nodes())
    # Both fully-qualified forms must survive
    assert any("staging.orders.id" in n for n in nodes)
    assert any("prod.orders.id" in n for n in nodes)
    assert any("ambiguous" in w.error.lower() for w in warnings)
```

- [ ] **Step 2: Run to fail** — current code picks one of staging/prod arbitrarily.

- [ ] **Step 3: Update `_normalize_edges`**

Replace the mapping build in `_normalize_edges`:

```python
short_to_long: dict[str, str] = {}
ambiguous: set[str] = set()
sorted_names = sorted(table_names, key=lambda n: n.count("."), reverse=True)
for name in sorted_names:
    candidates = [longer for longer in sorted_names
                  if longer != name and longer.endswith("." + name)]
    if len(candidates) == 1:
        short_to_long[name] = short_to_long.get(candidates[0], candidates[0])
    elif len(candidates) > 1:
        ambiguous.add(name)
```

And add a second return value so callers can surface warnings. Change the signature:

```python
def _normalize_edges(edges: list[LineageEdge]) -> tuple[list[LineageEdge], list[str]]:
```

Return `(normalized, sorted(ambiguous))`. In `build_graph_with_warnings`, after both normalisations, do:

```python
all_edges, ambiguous = _normalize_edges(all_edges)
all_raw_edges, _ = _normalize_edges(all_raw_edges)
for amb in ambiguous:
    all_warnings.append(ParseWarning(
        file="<graph>",
        error=f"ambiguous table name {amb!r} matches multiple qualified tables — not merging",
        severity="warn",
    ))
```

- [ ] **Step 4: Run full test suite** — green.
- [ ] **Step 5: Commit**

```bash
git add backend/lineage/engine.py backend/tests/test_engine.py
git commit -m "fix(engine): refuse ambiguous suffix merges and warn"
```

---

## Task 12: Surface `qualified` and `severity` in API responses

**Files:**
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_routes.py`

- [ ] **Step 1: Write the failing test**

```python
def test_lineage_endpoint_includes_qualified_field(test_client):
    # ... (use existing test harness to seed a graph with an unqualified-join edge)
    resp = test_client.get("/lineage?table=result&column=id")
    body = resp.json()
    assert "qualified" in body["upstream"][0]

def test_warnings_endpoint_includes_severity(test_client):
    resp = test_client.get("/warnings")
    body = resp.json()
    if body:
        assert "severity" in body[0]
```

(Adapt to `tests/test_routes.py`'s existing client fixture.)

- [ ] **Step 2: Run to fail.**

- [ ] **Step 3: Update `_edge_to_dict`**

In `backend/api/routes.py`:

```python
def _edge_to_dict(edge) -> dict:
    return {
        "source_col": edge.source_col,
        "target_col": edge.target_col,
        "transform_type": edge.transform_type,
        "expression": edge.expression,
        "source_file": edge.source_file,
        "source_cell": edge.source_cell,
        "source_line": edge.source_line,
        "confidence": edge.confidence,
        "qualified": edge.qualified,
    }
```

And update the warnings endpoint to include severity:

```python
@router.get("/warnings")
def get_warnings():
    return [
        {**w, "severity": w.get("severity", "warn")}
        for w in state.parse_warnings
    ]
```

Also update the `state.parse_warnings.extend(...)` call in `refresh_source` to include `"severity": w.severity`.

- [ ] **Step 4: Pass tests.**
- [ ] **Step 5: Commit**

```bash
git add backend/api/routes.py backend/tests/test_routes.py
git commit -m "feat(api): expose qualified and severity fields"
```

---

## Task 13: Frontend types + confidence dimming

**Files:**
- Modify: `frontend/lib/api.ts`, `frontend/components/lineage-graph.tsx`, `frontend/components/lineage-tree.tsx`

- [ ] **Step 1: Extend types**

In `frontend/lib/api.ts`:

```ts
export type LineageEdge = {
  source_col: string;
  target_col: string;
  transform_type: string;
  expression: string;
  source_file: string;
  source_cell: number | null;
  source_line: number | null;
  confidence: "certain" | "approximate";
  qualified: boolean;
};

export type Warning = {
  file: string;
  error: string;
  severity: "info" | "warn" | "error";
};
```

Also add `confidence` + `qualified` to `PathStep`.

- [ ] **Step 2: Dim unqualified / approximate edges in `lineage-graph.tsx`**

Update the expanded-mode edge map:

```tsx
return edges.map((e, i) => {
  const dimmed = e.qualified === false || e.confidence === "approximate";
  return {
    id: `e-${i}`,
    source: e.source_col,
    target: e.target_col,
    label: e.transform_type,
    animated: e.transform_type === "aggregation" || e.transform_type === "window",
    style: {
      stroke: TRANSFORM_COLOURS[e.transform_type] ?? "#888",
      strokeWidth: 1.5,
      strokeDasharray: dimmed ? "4 3" : undefined,
      opacity: dimmed ? 0.55 : 1,
    },
    labelStyle: { fontSize: 9, fill: dimmed ? "#556677" : "#888" },
    labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
  };
});
```

Add a legend entry:

```tsx
<span><span style={{ borderBottom: "1px dashed #888" }}>—</span> approximate</span>
```

- [ ] **Step 3: Mark unqualified rows in `lineage-tree.tsx`**

In `TreeNodeRow`, next to the transform badge:

```tsx
{node.edge && node.edge.qualified === false && (
  <span
    title="Column attribution is ambiguous (bare column in JOIN). Verify against schema."
    className="text-xs text-amber-500 cursor-help"
  >
    ~
  </span>
)}
```

- [ ] **Step 4: Verify in browser** — run `npm run dev` and navigate to a lineage page with a known unqualified-join edge. Confirm dashed stroke and `~` marker appear; hover shows the tooltip.

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/api.ts frontend/components/lineage-graph.tsx frontend/components/lineage-tree.tsx
git commit -m "feat(ui): dim approximate/unqualified edges and mark them with ~"
```

---

## Task 14: Frontend edge-type filter toggles

**Files:**
- Modify: `frontend/components/transform-badge.tsx`, `frontend/app/lineage/page.tsx`, `frontend/components/lineage-graph.tsx`, `frontend/components/lineage-tree.tsx`

- [ ] **Step 1: Add styling for `filter` and `join_key` in `transform-badge.tsx`**

Extend the colour map with muted red/blue tokens for `filter` and `join_key`.

- [ ] **Step 2: Add toggle state to `app/lineage/page.tsx`**

```tsx
const [showFilters, setShowFilters] = useState(false);
const [showJoinKeys, setShowJoinKeys] = useState(false);

const filteredUpstream = useMemo(
  () => lineage.upstream.filter((e) =>
    (e.transform_type !== "filter" || showFilters) &&
    (e.transform_type !== "join_key" || showJoinKeys)
  ),
  [lineage.upstream, showFilters, showJoinKeys],
);
// same for downstream + graph.edges
```

Render two pill buttons in the existing header:

```tsx
<button
  onClick={() => setShowFilters((v) => !v)}
  className={pillClasses(showFilters)}
>filter edges</button>
<button
  onClick={() => setShowJoinKeys((v) => !v)}
  className={pillClasses(showJoinKeys)}
>join keys</button>
```

Where `pillClasses(active)` returns the same `text-xs px-3 py-1 rounded border` scheme already used by the "Group by table" button.

- [ ] **Step 3: Pass filtered arrays through to `LineageGraph` and `LineageTree`.**

- [ ] **Step 4: Verify in browser** — upload a repo with a JOIN and a WHERE; toggle both pills, confirm nodes `*.__filter__` / `*.__joinkey__` appear/disappear.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/transform-badge.tsx frontend/app/lineage/page.tsx \
        frontend/components/lineage-graph.tsx frontend/components/lineage-tree.tsx
git commit -m "feat(ui): filter & join-key edge toggles (off by default)"
```

---

## Task 15: Severity-coloured warnings panel

**Files:**
- Modify: `frontend/app/sources/page.tsx`

- [ ] **Step 1: Read current warnings rendering** to confirm shape. Then render each warning with a severity indicator:

```tsx
const severityClasses = {
  info:  "border-blue-500/40 bg-blue-500/5 text-blue-200",
  warn:  "border-amber-500/40 bg-amber-500/5 text-amber-200",
  error: "border-rose-500/40 bg-rose-500/5 text-rose-200",
};

{warnings.map((w, i) => (
  <div
    key={i}
    className={`border-l-2 rounded-sm px-3 py-2 text-xs font-mono ${severityClasses[w.severity]}`}
  >
    <div className="font-semibold">{w.severity.toUpperCase()} · {w.file}</div>
    <div className="opacity-80 mt-0.5">{w.error}</div>
  </div>
))}
```

- [ ] **Step 2: Verify in browser** — upload a repo that contains a deliberately broken SQL file; confirm its warning shows with amber styling, and the ambiguous-table warning (from Task 11) also shows.

- [ ] **Step 3: Commit**

```bash
git add frontend/app/sources/page.tsx
git commit -m "feat(ui): severity-coloured warnings panel"
```

---

## Self-Review

**Spec coverage:**
- Global counter removed → Task 3 ✓
- `qualified` flag + downgrade unqualified-in-JOIN → Tasks 1, 4 ✓
- Per-statement partial parse → Task 5 ✓
- MERGE INTO → Task 6 ✓
- LATERAL VIEW → Task 7 ✓
- PIVOT → Task 8 ✓
- filter edges → Task 9 ✓
- join_key edges → Task 10 ✓
- Normalize ambiguity → Task 11 ✓
- API surfaces new fields → Task 12 ✓
- UI dims approximate edges → Task 13 ✓
- UI toggles filter/join_key → Task 14 ✓
- UI shows warning severity → Task 15 ✓
- Token offsets → **explicitly deferred to Tier 2** (requires non-trivial SQLGlot provenance work; Tier 1 ships without it and source_file/source_line remain best-effort).

**Placeholder scan:** every code block is complete and runnable as shown. No "TBD" or "Similar to Task N".

**Type consistency:**
- `ParseContext` defined in Task 3, used by Tasks 4, 6, 9, 10 — same fields.
- `qualified: bool` added in Task 1, consumed everywhere as a keyword arg.
- `multi_source = len(source_tables) > 1` defined once in `_parse_select_node` in Task 4, reused in Tasks 7, 8, 9, 10.
- `__filter__` / `__joinkey__` pseudo-columns defined in Tasks 9/10 and filtered in Task 14.
