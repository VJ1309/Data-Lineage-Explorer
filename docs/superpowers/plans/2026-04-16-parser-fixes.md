# Parser Engine Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 3 confirmed bugs and 4 design limitations in the SQL/PySpark/notebook parsing engine, plus 3 code-quality issues.

**Architecture:** All changes are confined to `backend/parsers/` and `backend/lineage/engine.py`. The largest change is a refactor of `_parse_single_statement` in `sql.py` into a `_parse_select_node` helper to enable UNION/subquery support. Every fix is test-driven: failing test first, minimal implementation second.

**Tech Stack:** Python 3.11+, SQLGlot (Databricks dialect), Python `ast` module, pytest, nbformat

---

## File Map

| File | What changes |
|---|---|
| `backend/parsers/sql.py` | `_resolve_ctes` (multi-level chains), `_parse_single_statement` → `_parse_select_node` refactor (UNION + subquery support), `_split_databricks_sql` precedence fix, `parse_sql` silent-error fix |
| `backend/parsers/pyspark.py` | `_DataFrameTracker` temp-view collection, `_find_write_source_var` helper, `parse_pyspark` temp-view resolution |
| `backend/parsers/notebook.py` | Cross-cell temp-view collection + resolution |
| `backend/lineage/engine.py` | DFS/BFS docstring fix, `parse_sql` warnings threading |
| `backend/tests/test_sql_parser.py` | New tests: chained CTEs, UNION ALL, subquery FROM, SELECT *, silent parse error warning |
| `backend/tests/test_pyspark_parser.py` | New tests: cross-call temp views, `.write.mode().saveAsTable()` chain |
| `backend/tests/test_notebook_parser.py` | New test: cross-cell temp view resolution |

---

## Task 1: Bug — `notebook.py` cross-cell temp view resolution

**Files:**
- Modify: `backend/parsers/notebook.py`
- Test: `backend/tests/test_notebook_parser.py`

- [ ] **Step 1: Write the failing test**

Add to `backend/tests/test_notebook_parser.py`:

```python
def test_cross_cell_temp_view_resolution():
    """Temp view created in cell 0 must be resolved away in cell 1."""
    nb = _make_notebook([
        _code_cell("%sql\nCREATE OR REPLACE TEMP VIEW stg AS SELECT id, val FROM source_table"),
        _code_cell("%sql\nINSERT INTO final SELECT id, val FROM stg"),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "stg.id" not in targets, "temp view must not appear as a target"
    assert "stg.val" not in targets
    assert "final.id" in targets
    assert "final.val" in targets
    assert "source_table.id" in sources
    assert "source_table.val" in sources
```

- [ ] **Step 2: Run test to verify it fails**

```
cd backend && python -m pytest tests/test_notebook_parser.py::test_cross_cell_temp_view_resolution -v
```

Expected: FAIL — `final.id` not in targets (the stg edges are returned unresolved).

- [ ] **Step 3: Fix `notebook.py`**

Replace the full file content of `backend/parsers/notebook.py`:

```python
"""Notebook parser using nbformat. Routes cells to SQL or PySpark parsers."""
from __future__ import annotations
import nbformat
from parsers.sql import parse_sql, _detect_temp_views, _resolve_temp_views
from parsers.pyspark import parse_pyspark
from lineage.models import LineageEdge


_SQL_MAGICS = ("%sql", "%%sql", "%spark.sql")


def _is_sql_cell(source: str) -> bool:
    stripped = source.strip()
    return any(stripped.startswith(magic) for magic in _SQL_MAGICS)


def _strip_sql_magic(source: str) -> str:
    stripped = source.strip()
    for magic in _SQL_MAGICS:
        if stripped.startswith(magic):
            return stripped[len(magic):].strip()
    return stripped


def parse_notebook(
    content: str,
    source_file: str,
) -> list[LineageEdge]:
    """Parse a Jupyter notebook JSON string and return all lineage edges."""
    try:
        nb = nbformat.reads(content, as_version=4)
    except Exception:
        return []

    edges: list[LineageEdge] = []
    temp_views: set[str] = set()

    for cell_idx, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue

        source = cell.source
        if not source.strip():
            continue

        lang = cell.get("metadata", {}).get("language", "")

        if _is_sql_cell(source) or lang == "sql":
            sql = _strip_sql_magic(source)
            temp_views.update(_detect_temp_views(sql))
            cell_edges = parse_sql(
                sql,
                source_file=source_file,
                source_line=None,
                source_cell=cell_idx,
                _resolve_views=False,  # resolution happens at notebook level
            )
        else:
            cell_edges = parse_pyspark(
                source,
                source_file=source_file,
                source_cell=cell_idx,
            )

        edges.extend(cell_edges)

    return _resolve_temp_views(edges, temp_views)
```

- [ ] **Step 4: Run all notebook tests to verify**

```
cd backend && python -m pytest tests/test_notebook_parser.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/notebook.py backend/tests/test_notebook_parser.py
git commit -m "fix: resolve temp views across cells in Jupyter notebook parser"
```

---

## Task 2: Bug — plain `.py` `spark.sql()` temp view resolution

**Files:**
- Modify: `backend/parsers/pyspark.py`
- Test: `backend/tests/test_pyspark_parser.py`

- [ ] **Step 1: Write the failing test**

Add to `backend/tests/test_pyspark_parser.py`:

```python
SPARK_SQL_CROSS_CALL_TEMP_VIEW = '''\
spark.sql("CREATE OR REPLACE TEMP VIEW staging AS SELECT id, val FROM source_table")
spark.sql("INSERT INTO final SELECT id, val FROM staging")
'''

def test_plain_py_spark_sql_cross_call_temp_view():
    """Temp view created in one spark.sql() call must be resolved in a later call."""
    edges = parse_pyspark(SPARK_SQL_CROSS_CALL_TEMP_VIEW, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "staging.id" not in targets, "temp view must not appear as a target"
    assert "staging.val" not in targets
    assert "final.id" in targets
    assert "final.val" in targets
    assert "source_table.id" in sources
    assert "source_table.val" in sources
```

- [ ] **Step 2: Run test to verify it fails**

```
cd backend && python -m pytest tests/test_pyspark_parser.py::test_plain_py_spark_sql_cross_call_temp_view -v
```

Expected: FAIL — `final.id` not in targets.

- [ ] **Step 3: Fix `_DataFrameTracker` and `parse_pyspark` in `backend/parsers/pyspark.py`**

**3a.** In `_DataFrameTracker.__init__`, add a `temp_views` set (after the existing `self.edges` line):

```python
self.temp_views: set[str] = set()
```

**3b.** In `_DataFrameTracker.visit_Assign`, find the block that handles `spark.sql()` (starts with `sql_str = self._get_spark_sql(value)`). Replace it:

Old:
```python
            sql_str = self._get_spark_sql(value)
            if sql_str:
                sql_edges = _parse_sql(
                    sql_str,
                    source_file=self.source_file,
                    source_line=node.lineno,
                    _resolve_views=self._resolve_views,
                )
                self.edges.extend(sql_edges)
                self.generic_visit(node)
                return
```

New:
```python
            sql_str = self._get_spark_sql(value)
            if sql_str:
                self.temp_views.update(_detect_temp_views(sql_str))
                sql_edges = _parse_sql(
                    sql_str,
                    source_file=self.source_file,
                    source_line=node.lineno,
                    _resolve_views=False,  # resolution at tracker level
                )
                self.edges.extend(sql_edges)
                self.generic_visit(node)
                return
```

**3c.** In `_DataFrameTracker.visit_Expr`, find the block that handles standalone `spark.sql()` (starts with `sql_str = self._get_spark_sql(call)`). Replace it:

Old:
```python
        sql_str = self._get_spark_sql(call)
        if sql_str:
            sql_edges = _parse_sql(
                sql_str,
                source_file=self.source_file,
                source_line=node.lineno,
                _resolve_views=self._resolve_views,
            )
            self.edges.extend(sql_edges)
            self.generic_visit(node)
            return
```

New:
```python
        sql_str = self._get_spark_sql(call)
        if sql_str:
            self.temp_views.update(_detect_temp_views(sql_str))
            sql_edges = _parse_sql(
                sql_str,
                source_file=self.source_file,
                source_line=node.lineno,
                _resolve_views=False,  # resolution at tracker level
            )
            self.edges.extend(sql_edges)
            self.generic_visit(node)
            return
```

**3d.** In `parse_pyspark`, find the plain-Python path (after the Databricks header check). It currently ends with:

```python
    tree = ast.parse(code)

    tracker = _DataFrameTracker(source_file=source_file, resolve_views=False)
    tracker.visit(tree)

    if source_cell is not None:
        for edge in tracker.edges:
            edge.source_cell = source_cell

    return tracker.edges
```

Replace with:

```python
    tree = ast.parse(code)

    tracker = _DataFrameTracker(source_file=source_file, resolve_views=False)
    tracker.visit(tree)

    if source_cell is not None:
        for edge in tracker.edges:
            edge.source_cell = source_cell

    edges = tracker.edges
    if _resolve_views and tracker.temp_views:
        edges = _resolve_temp_views(edges, tracker.temp_views)
    return edges
```

- [ ] **Step 4: Run all pyspark tests to verify**

```
cd backend && python -m pytest tests/test_pyspark_parser.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/pyspark.py backend/tests/test_pyspark_parser.py
git commit -m "fix: resolve spark.sql() temp views across calls in plain Python files"
```

---

## Task 3: Bug — `.write.mode(...).saveAsTable()` chain drops edges

**Files:**
- Modify: `backend/parsers/pyspark.py`
- Test: `backend/tests/test_pyspark_parser.py`

- [ ] **Step 1: Write the failing test**

Add to `backend/tests/test_pyspark_parser.py`:

```python
WRITE_MODE_CHAIN = '''\
df = spark.read.table("raw_orders")
df2 = df.select("order_id", "amount")
df2.write.mode("overwrite").saveAsTable("staging_orders")
'''

def test_write_mode_chain_emits_edges():
    """df.write.mode(...).saveAsTable() must emit the same edges as df.write.saveAsTable()."""
    edges = parse_pyspark(WRITE_MODE_CHAIN, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "staging_orders.order_id" in targets, "write chain with .mode() dropped edges"
    assert "staging_orders.amount" in targets
```

- [ ] **Step 2: Run test to verify it fails**

```
cd backend && python -m pytest tests/test_pyspark_parser.py::test_write_mode_chain_emits_edges -v
```

Expected: FAIL — no edges emitted because the `.mode()` call breaks the chain walk.

- [ ] **Step 3: Add `_find_write_source_var` helper and update `visit_Expr`**

In `backend/parsers/pyspark.py`, add this module-level helper function just before the `_DataFrameTracker` class:

```python
def _find_write_source_var(call: ast.Call) -> str | None:
    """Walk df.write[.chainedMethod()...].writeMethod() to find the df variable.

    Handles patterns like:
      df.write.saveAsTable("t")
      df.write.mode("overwrite").saveAsTable("t")
      df.write.option("k","v").mode("overwrite").saveAsTable("t")
    """
    if not isinstance(call.func, ast.Attribute):
        return None
    obj = call.func.value  # object the final method is called on
    while True:
        if isinstance(obj, ast.Attribute) and obj.attr == "write":
            return obj.value.id if isinstance(obj.value, ast.Name) else None
        if isinstance(obj, ast.Call) and isinstance(obj.func, ast.Attribute):
            obj = obj.func.value
        else:
            return None
```

In `_DataFrameTracker.visit_Expr`, find the block that walks the write chain (starts with `# Walk back: df.write.saveAsTable`). Replace the entire block from the `write_chain = call.func` line through `if src_var is None:`:

Old:
```python
        # Walk back: df.write.saveAsTable -> call.func.value = df.write, .value = df
        write_chain = call.func
        src_var = None
        if isinstance(write_chain, ast.Attribute):
            wv = write_chain.value
            if isinstance(wv, ast.Attribute) and wv.attr == "write":
                dnode = wv.value
                if isinstance(dnode, ast.Name):
                    src_var = dnode.id

        if src_var is None:
            self.generic_visit(node)
            return
```

New:
```python
        src_var = _find_write_source_var(call)
        if src_var is None:
            self.generic_visit(node)
            return
```

- [ ] **Step 4: Run all pyspark tests**

```
cd backend && python -m pytest tests/test_pyspark_parser.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/pyspark.py backend/tests/test_pyspark_parser.py
git commit -m "fix: detect df.write.mode(...).saveAsTable() write chains in PySpark parser"
```

---

## Task 4: Design limitation — multi-level CTE chains

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing test**

Add to `backend/tests/test_sql_parser.py`:

```python
def test_chained_ctes_resolve_to_source():
    """CTE2 referencing CTE1 must resolve all the way to the base table."""
    sql = """
    WITH cte1 AS (SELECT id, val FROM source_table),
         cte2 AS (SELECT id, val FROM cte1)
    INSERT INTO final SELECT id, val FROM cte2
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert any("source_table" in s for s in sources), "must trace back to source_table"
    assert not any(s.startswith("cte1.") for s in sources), "cte1 must be resolved away"
    assert not any(s.startswith("cte2.") for s in sources), "cte2 must be resolved away"
```

- [ ] **Step 2: Run test to verify it fails**

```
cd backend && python -m pytest tests/test_sql_parser.py::test_chained_ctes_resolve_to_source -v
```

Expected: FAIL — sources contain `cte1.id` or `cte2.id` instead of `source_table.id`.

- [ ] **Step 3: Fix `_resolve_ctes` in `backend/parsers/sql.py`**

Find the current `_resolve_ctes` function. Its full body is:

```python
def _resolve_ctes(statement: exp.Expression) -> dict[str, str]:
    """Build map of CTE alias -> source table name (best-effort, one level deep)."""
    cte_map: dict[str, str] = {}
    # SQLGlot uses 'with_' as the arg key for WITH clause
    with_clause = statement.args.get("with_")
    if not with_clause:
        return cte_map
    for cte in with_clause.expressions:
        alias = cte.alias
        cte_select = cte.this
        # SQLGlot uses 'from_' as the arg key for FROM clause
        from_clause = cte_select.args.get("from_")
        if from_clause:
            table_expr = from_clause.this
            if isinstance(table_expr, exp.Table):
                cte_map[alias] = _qualified_table_name(table_expr)
    return cte_map
```

Replace it with:

```python
def _resolve_ctes(statement: exp.Expression) -> dict[str, str]:
    """Build map of CTE alias -> source table name (best-effort, single-source CTEs).

    Resolves chains: if cte2 references cte1, cte2 maps to cte1's source.
    Multi-source CTEs (JOINs) are omitted; _resolve_table_hint falls back gracefully.
    """
    cte_map: dict[str, str] = {}
    with_clause = statement.args.get("with_")
    if not with_clause:
        return cte_map
    for cte in with_clause.expressions:
        alias = cte.alias
        cte_select = cte.this
        from_clause = cte_select.args.get("from_")
        if from_clause:
            table_expr = from_clause.this
            if isinstance(table_expr, exp.Table):
                cte_map[alias] = _qualified_table_name(table_expr)

    # Resolve chains: cte2 -> cte1 -> actual_table
    max_iterations = len(cte_map) + 1
    for _ in range(max_iterations):
        changed = False
        for alias, target in list(cte_map.items()):
            if target in cte_map:
                cte_map[alias] = cte_map[target]
                changed = True
        if not changed:
            break

    return cte_map
```

- [ ] **Step 4: Run all SQL parser tests**

```
cd backend && python -m pytest tests/test_sql_parser.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "fix: resolve multi-level CTE chains in SQL parser"
```

---

## Task 5: Design limitation — UNION / UNION ALL

This task refactors `_parse_single_statement` into a `_parse_select_node` helper so multiple SELECT branches can be parsed from a UNION. Task 6 (subqueries) builds on this refactor.

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing tests**

Add to `backend/tests/test_sql_parser.py`:

```python
def test_union_all_both_branches_produce_edges():
    """Both branches of UNION ALL must emit edges to the same target."""
    sql = """
    INSERT INTO combined
    SELECT id, val FROM table_a
    UNION ALL
    SELECT id, val FROM table_b
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "table_a.id" in sources, "first UNION branch missing"
    assert "table_b.id" in sources, "second UNION branch missing"
    assert "combined.id" in targets


def test_union_standalone_result():
    """UNION without INSERT uses 'result' as synthetic target."""
    sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "t1.a" in sources
    assert "t2.a" in sources
```

- [ ] **Step 2: Run tests to verify they fail**

```
cd backend && python -m pytest tests/test_sql_parser.py::test_union_all_both_branches_produce_edges tests/test_sql_parser.py::test_union_standalone_result -v
```

Expected: both FAIL — only edges from the first branch are emitted.

- [ ] **Step 3: Refactor `_parse_single_statement` in `backend/parsers/sql.py`**

Add these two new helpers immediately before `_parse_single_statement`:

```python
def _collect_union_selects(node: exp.Expression) -> list[exp.Select]:
    """Recursively collect all SELECT branches from a UNION/INTERSECT/EXCEPT tree."""
    if isinstance(node, exp.Select):
        return [node]
    if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
        return _collect_union_selects(node.this) + _collect_union_selects(node.expression)
    return []


def _get_statement_body(statement: exp.Expression) -> exp.Expression | None:
    """Return the query body (SELECT or UNION) stripping INSERT/CREATE wrapper."""
    if isinstance(statement, exp.Select):
        return statement
    if isinstance(statement, exp.Insert):
        return statement.args.get("expression")
    if isinstance(statement, exp.Create):
        return statement.args.get("expression")
    return None
```

Then rename `_parse_single_statement` to `_parse_select_node` and change its signature so it takes a `select_node` directly along with `target_table` and `cte_map` (instead of a full statement):

```python
def _parse_select_node(
    select_node: exp.Select,
    target_table: str,
    cte_map: dict[str, str],
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    """Parse a single SELECT node and return column-level lineage edges."""
    edges: list[LineageEdge] = []

    # Collect source tables from FROM + JOINs
    from_clause = select_node.args.get("from_")
    if from_clause is None:
        return []

    # Build alias -> qualified name map for FROM/JOIN tables
    alias_map: dict[str, str] = {}
    source_tables: list[str] = []
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        qualified = _qualified_table_name(from_table)
        alias = from_table.alias
        resolved = cte_map.get(from_table.name, qualified)
        if alias:
            alias_map[alias] = resolved
        source_tables.append(resolved)
    elif isinstance(from_table, exp.Subquery):
        source_tables.append("subquery")

    for join in select_node.find_all(exp.Join):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            qualified = _qualified_table_name(jtable)
            alias = jtable.alias
            resolved = cte_map.get(jtable.name, qualified)
            if alias:
                alias_map[alias] = resolved
            source_tables.append(resolved)

    if not source_tables:
        return []

    default_table = source_tables[0]

    def _resolve_table_hint(hint: str) -> tuple[str, bool]:
        if hint in alias_map:
            return alias_map[hint], True
        if hint in cte_map:
            return cte_map[hint], True
        for tbl in source_tables:
            if tbl == hint or tbl.endswith(f".{hint}"):
                return tbl, True
        return default_table, False

    # Walk SELECT expressions
    for sel in select_node.selects:
        if isinstance(sel, exp.Alias):
            alias = sel.alias
            expr_node = sel.this
        elif isinstance(sel, exp.Column):
            alias = sel.name
            expr_node = sel
        else:
            alias = sel.sql(dialect="databricks")
            expr_node = sel

        if not alias:
            continue

        target_col = f"{target_table}.{alias}"
        transform_type, expr_str = _classify_transform(expr_node)

        if transform_type == "window":
            win_col_refs = list(expr_node.find_all(exp.Column))
            if win_col_refs:
                for col_ref in win_col_refs:
                    table_hint = col_ref.table
                    col_name = col_ref.name
                    if not col_name:
                        continue
                    if table_hint:
                        resolved_table, certain = _resolve_table_hint(table_hint)
                    else:
                        resolved_table, certain = default_table, True
                    edges.append(LineageEdge(
                        source_col=f"{resolved_table}.{col_name}",
                        target_col=target_col,
                        transform_type=transform_type,
                        expression=expr_str,
                        source_file=source_file,
                        source_cell=source_cell,
                        source_line=source_line,
                        confidence="certain" if certain else "approximate",
                    ))
            else:
                edges.append(LineageEdge(
                    source_col=f"{default_table}.*",
                    target_col=target_col,
                    transform_type=transform_type,
                    expression=expr_str,
                    source_file=source_file,
                    source_cell=source_cell,
                    source_line=source_line,
                    confidence="certain",
                ))
            continue

        col_refs = list(expr_node.find_all(exp.Column))

        if not col_refs:
            edges.append(LineageEdge(
                source_col=f"{default_table}.{alias}",
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain",
            ))
            continue

        for col_ref in col_refs:
            table_hint = col_ref.table
            col_name = col_ref.name
            if not col_name:
                continue
            if table_hint:
                resolved_table, certain = _resolve_table_hint(table_hint)
            else:
                resolved_table, certain = default_table, True
            source_col = f"{resolved_table}.{col_name}"
            edges.append(LineageEdge(
                source_col=source_col,
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain" if certain else "approximate",
            ))

    return edges
```

Replace the old `_parse_single_statement` with a thin wrapper that uses the new helpers:

```python
def _parse_single_statement(
    statement: exp.Expression,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    """Parse a single SQL statement (handles UNION/UNION ALL) and return lineage edges."""
    body = _get_statement_body(statement)
    if body is None:
        body = statement.find(exp.Select)
    if body is None:
        return []

    select_nodes = _collect_union_selects(body)
    if not select_nodes:
        return []

    cte_map = _resolve_ctes(statement)
    target_table = _find_target_table(statement)
    edges: list[LineageEdge] = []
    for select_node in select_nodes:
        edges.extend(_parse_select_node(
            select_node, target_table, cte_map,
            source_file, source_line, source_cell,
        ))
    return edges
```

- [ ] **Step 4: Run full SQL parser test suite**

```
cd backend && python -m pytest tests/test_sql_parser.py -v
```

Expected: ALL PASS (existing tests must not regress).

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat: support UNION/UNION ALL in SQL parser; refactor into _parse_select_node"
```

---

## Task 6: Design limitation — subqueries in FROM

Builds on Task 5's `_parse_select_node` refactor.

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing tests**

Add to `backend/tests/test_sql_parser.py`:

```python
def test_subquery_in_from_traces_to_base_table():
    """Columns from an inline subquery must trace back to the base table, not 'subquery'."""
    sql = """
    INSERT INTO result
    SELECT id, val FROM (SELECT id, val FROM source_table) sub
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "source_table.id" in sources, "must trace through subquery to base table"
    assert "source_table.val" in sources
    assert "result.id" in targets
    assert "result.val" in targets
    assert not any(s.startswith("subquery.") for s in sources), "phantom 'subquery' table found"


def test_subquery_with_alias_join():
    """Subquery in JOIN must also trace through to its source table."""
    sql = """
    INSERT INTO result
    SELECT a.id, sub.metric
    FROM base_table a
    JOIN (SELECT id, SUM(val) AS metric FROM detail_table GROUP BY id) sub
      ON a.id = sub.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "base_table.id" in sources
    assert "detail_table.val" in sources
```

- [ ] **Step 2: Run tests to verify they fail**

```
cd backend && python -m pytest tests/test_sql_parser.py::test_subquery_in_from_traces_to_base_table tests/test_sql_parser.py::test_subquery_with_alias_join -v
```

Expected: FAIL — sources contain `subquery.id` instead of `source_table.id`.

- [ ] **Step 3: Update `_parse_select_node` to handle subqueries recursively**

In `backend/parsers/sql.py`, find `_parse_select_node`. Locate the FROM handling block:

```python
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        qualified = _qualified_table_name(from_table)
        alias = from_table.alias
        resolved = cte_map.get(from_table.name, qualified)
        if alias:
            alias_map[alias] = resolved
        source_tables.append(resolved)
    elif isinstance(from_table, exp.Subquery):
        source_tables.append("subquery")
```

Replace with:

```python
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        qualified = _qualified_table_name(from_table)
        alias = from_table.alias
        resolved = cte_map.get(from_table.name, qualified)
        if alias:
            alias_map[alias] = resolved
        source_tables.append(resolved)
    elif isinstance(from_table, exp.Subquery):
        sub_alias = from_table.alias or f"__sub_{id(from_table)}__"
        source_tables.append(sub_alias)
        if from_table.alias:
            alias_map[from_table.alias] = sub_alias
        sub_selects = _collect_union_selects(from_table.this) if from_table.this else []
        for sub_sel in sub_selects:
            edges.extend(_parse_select_node(
                sub_sel, sub_alias, cte_map,
                source_file, source_line, source_cell,
            ))
```

Also update the JOIN handling block to handle subqueries in JOINs. Find:

```python
    for join in select_node.find_all(exp.Join):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            qualified = _qualified_table_name(jtable)
            alias = jtable.alias
            resolved = cte_map.get(jtable.name, qualified)
            if alias:
                alias_map[alias] = resolved
            source_tables.append(resolved)
```

Replace with:

```python
    for join in select_node.find_all(exp.Join):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            qualified = _qualified_table_name(jtable)
            alias = jtable.alias
            resolved = cte_map.get(jtable.name, qualified)
            if alias:
                alias_map[alias] = resolved
            source_tables.append(resolved)
        elif isinstance(jtable, exp.Subquery):
            sub_alias = jtable.alias or f"__sub_{id(jtable)}__"
            source_tables.append(sub_alias)
            if jtable.alias:
                alias_map[jtable.alias] = sub_alias
            sub_selects = _collect_union_selects(jtable.this) if jtable.this else []
            for sub_sel in sub_selects:
                edges.extend(_parse_select_node(
                    sub_sel, sub_alias, cte_map,
                    source_file, source_line, source_cell,
                ))
```

- [ ] **Step 4: Run full SQL parser test suite**

```
cd backend && python -m pytest tests/test_sql_parser.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat: trace lineage through subqueries in FROM and JOIN clauses"
```

---

## Task 7: Code quality fixes

Covers: cell-filter precedence bug, DFS/BFS docstrings, silent SQLGlot parse errors, and verifying SELECT * wildcard edge emission.

**Files:**
- Modify: `backend/parsers/sql.py`
- Modify: `backend/lineage/engine.py`
- Test: `backend/tests/test_sql_parser.py`
- Test: `backend/tests/test_engine.py`

- [ ] **Step 1: Fix cell-filter precedence bug in `_split_databricks_sql` (`sql.py:354`)**

Find this line in `_split_databricks_sql`:

```python
        if cleaned and not cleaned.startswith("--") or "\n" in cleaned:
```

Replace with:

```python
        if (cleaned and not cleaned.startswith("--")) or "\n" in cleaned:
```

This makes the operator precedence explicit and matches the intended logic.

- [ ] **Step 2: Fix DFS/BFS docstrings in `engine.py`**

In `backend/lineage/engine.py`, find the `upstream` function docstring:

```python
def upstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading TO col_id (BFS backwards)."""
```

Replace with:

```python
def upstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading TO col_id (DFS backwards)."""
```

Find the `downstream` function docstring:

```python
def downstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading FROM col_id (BFS forwards)."""
```

Replace with:

```python
def downstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading FROM col_id (DFS forwards)."""
```

- [ ] **Step 3: Surface SQLGlot parse errors as warnings**

The fix is a two-part change: add an optional `_warnings` parameter to `parse_sql`, and thread it through `_parse_file`.

In `backend/parsers/sql.py`, update the `parse_sql` signature and its try/except block:

Old signature:
```python
def parse_sql(
    sql: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None = None,
    _resolve_views: bool = True,
) -> list[LineageEdge]:
```

New signature and try/except:
```python
def parse_sql(
    sql: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None = None,
    _resolve_views: bool = True,
    _warnings: list[str] | None = None,
) -> list[LineageEdge]:
    """Parse SQL (single or multi-statement) and return column-level lineage edges.

    Supports multiple statements separated by semicolons.
    Detects Databricks-exported .sql notebooks and splits on '-- COMMAND ----------'.
    Uses "result" as the synthetic target table name when no INTO/CREATE is present.
    Returns empty list on parse error (non-fatal). Pass _warnings list to collect errors.
    """
    # Detect Databricks SQL notebook format
    if _DATABRICKS_SQL_SEP in sql and source_cell is None:
        edges: list[LineageEdge] = []
        temp_views: set[str] = set()
        for cell_sql, cell_idx in _split_databricks_sql(sql):
            temp_views.update(_detect_temp_views(cell_sql))
            edges.extend(
                parse_sql(cell_sql, source_file, source_line=None,
                          source_cell=cell_idx, _resolve_views=False,
                          _warnings=_warnings)
            )
        return _resolve_temp_views(edges, temp_views)

    try:
        statements = sqlglot.parse(sql, dialect="databricks")
    except Exception as exc:
        if _warnings is not None:
            _warnings.append(str(exc))
        return []
    ...  # rest of function unchanged
```

In `backend/lineage/engine.py`, update `_parse_file` to pass a warnings collector:

Old:
```python
    try:
        if record.type == "notebook":
            edges = parse_notebook(record.content, source_file=record.path)
        elif record.type == "python":
            edges = parse_pyspark(record.content, source_file=record.path)
        elif record.type == "sql":
            edges = parse_sql(record.content, source_file=record.path, source_line=1)
```

New:
```python
    sql_parse_errors: list[str] = []
    try:
        if record.type == "notebook":
            edges = parse_notebook(record.content, source_file=record.path)
        elif record.type == "python":
            edges = parse_pyspark(record.content, source_file=record.path)
        elif record.type == "sql":
            edges = parse_sql(record.content, source_file=record.path, source_line=1,
                              _warnings=sql_parse_errors)
```

And after the try/except block, append any collected parse errors as warnings:

```python
    for err in sql_parse_errors:
        warnings.append(ParseWarning(file=record.path, error=f"SQL parse error: {err}"))
    return edges, warnings
```

- [ ] **Step 4: Write tests for the code quality fixes**

Add to `backend/tests/test_sql_parser.py`:

```python
def test_select_star_emits_wildcard_edge():
    """SELECT * must emit a source.* -> target.* wildcard edge, not silence."""
    sql = "INSERT INTO target SELECT * FROM source_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].source_col == "source_table.*"
    assert edges[0].target_col == "target.*"


def test_bad_sql_collects_warning():
    """SQLGlot parse failure must surface in _warnings when caller passes the list."""
    warnings: list[str] = []
    edges = parse_sql(
        "THIS IS NOT SQL !!!###",
        source_file="bad.sql",
        source_line=1,
        _warnings=warnings,
    )
    assert edges == []
    assert len(warnings) == 1
    assert warnings[0]  # non-empty error message
```

Add to `backend/tests/test_engine.py` (check what exists there first and add to it):

```python
def test_sql_parse_error_surfaces_as_warning():
    """A file whose SQL cannot be parsed must produce a ParseWarning, not silent empty."""
    from lineage.engine import build_graph_with_warnings
    from lineage.models import FileRecord
    record = FileRecord(
        path="bad.sql",
        content="THIS IS NOT SQL !!!###",
        type="sql",
        source_ref="test",
    )
    _, warnings = build_graph_with_warnings([record])
    assert any("bad.sql" in w.file for w in warnings), (
        "parse error in bad.sql must produce a ParseWarning"
    )
```

- [ ] **Step 5: Run the full test suite**

```
cd backend && python -m pytest tests/ -v
```

Expected: ALL PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/parsers/sql.py backend/lineage/engine.py \
        backend/tests/test_sql_parser.py backend/tests/test_engine.py
git commit -m "fix: precedence bug in cell filter, DFS docstrings, surface SQLGlot parse errors"
```

---

## Self-Review

**Spec coverage:**
- Bug 1 (notebook cross-cell temp views) → Task 1 ✓
- Bug 2 (plain .py spark.sql temp views) → Task 2 ✓
- Bug 3 (.write.mode chain) → Task 3 ✓
- Limitation 4 (multi-level CTE) → Task 4 ✓
- Limitation 5 (UNION/UNION ALL) → Task 5 ✓
- Limitation 6 (subqueries in FROM) → Task 6 ✓
- Code quality (precedence, docstrings, silent errors) → Task 7 ✓
- SELECT * wildcard edge verification → Task 7 ✓

**Placeholder scan:** No TBD or TODO items. All code blocks are complete.

**Type consistency:**
- `_parse_select_node` is defined in Task 5 and used by `_parse_single_statement` (same task). Task 6 also calls it — same signature. ✓
- `_collect_union_selects` defined in Task 5, used in Task 6. ✓
- `_detect_temp_views` and `_resolve_temp_views` are already in `sql.py`; imported in Task 1 and Task 2. ✓
- `_find_write_source_var` defined and used in Task 3 within the same file. ✓
- `_warnings: list[str] | None = None` added in Task 7, threaded consistently. ✓
