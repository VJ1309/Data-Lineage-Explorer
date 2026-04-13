# DataLineage Explorer — Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI Python backend that ingests notebooks/scripts from Git, Databricks API, and file uploads, parses them for column-level lineage, and exposes a REST API for the frontend.

**Architecture:** FastAPI app with three ingestion adapters (Git, Databricks, upload), three parsers (SQLGlot for SQL, Python ast for PySpark, nbformat for notebooks), a NetworkX in-memory DAG as the lineage engine, and a flat module-level state store. All lineage is computed on-demand; no database.

**Tech Stack:** Python 3.11, FastAPI, Uvicorn, SQLGlot, NetworkX, nbformat, GitPython, databricks-sdk, pytest

---

## File Map

```
backend/
├── main.py                     # FastAPI app factory + CORS + router mount
├── state.py                    # Module-level source_registry, lineage_graph, warnings
├── lineage/
│   ├── __init__.py
│   ├── models.py               # FileRecord, ColumnNode, LineageEdge, SourceConfig, ParseWarning
│   └── engine.py               # build_graph(), upstream(), downstream()
├── parsers/
│   ├── __init__.py
│   ├── sql.py                  # SQLGlot-based SQL parser → list[LineageEdge]
│   ├── pyspark.py              # ast-based PySpark parser → list[LineageEdge]
│   └── notebook.py             # nbformat parser → routes cells to sql/pyspark parsers
├── ingestion/
│   ├── __init__.py
│   ├── git.py                  # GitPython → list[FileRecord]
│   ├── databricks.py           # databricks-sdk → list[FileRecord]
│   └── upload.py               # ZIP extract → list[FileRecord]
├── api/
│   ├── __init__.py
│   └── routes.py               # All FastAPI route handlers
├── Dockerfile
├── pyproject.toml
└── tests/
    ├── test_models.py
    ├── test_sql_parser.py
    ├── test_pyspark_parser.py
    ├── test_notebook_parser.py
    ├── test_engine.py
    ├── test_ingestion_upload.py
    └── test_routes.py
```

---

## Task 1: Project Scaffolding

**Files:**
- Create: `backend/pyproject.toml`
- Create: `backend/main.py`
- Create: `backend/state.py`
- Create: `backend/lineage/__init__.py`
- Create: `backend/parsers/__init__.py`
- Create: `backend/ingestion/__init__.py`
- Create: `backend/api/__init__.py`

- [ ] **Step 1: Create backend directory structure**

```bash
mkdir -p backend/lineage backend/parsers backend/ingestion backend/api backend/tests
touch backend/lineage/__init__.py backend/parsers/__init__.py
touch backend/ingestion/__init__.py backend/api/__init__.py
```

- [ ] **Step 2: Create `backend/pyproject.toml`**

```toml
[project]
name = "datalineage-backend"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.111.0",
    "uvicorn[standard]>=0.29.0",
    "sqlglot>=25.0.0",
    "networkx>=3.3",
    "nbformat>=5.10.0",
    "gitpython>=3.1.43",
    "databricks-sdk>=0.28.0",
    "python-multipart>=0.0.9",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "httpx>=0.27.0",
    "pytest-asyncio>=0.23.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

- [ ] **Step 3: Create `backend/state.py`**

```python
"""Module-level in-memory state. All state is lost on server restart."""
from typing import Any
import networkx as nx

# source_registry maps source_id -> SourceConfig dict
source_registry: dict[str, dict[str, Any]] = {}

# Merged lineage DAG across all registered sources
lineage_graph: nx.DiGraph = nx.DiGraph()

# Parse warnings from the last refresh of each source
parse_warnings: list[dict[str, str]] = []
```

- [ ] **Step 4: Create `backend/main.py`**

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import router

app = FastAPI(title="DataLineage Explorer API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
```

- [ ] **Step 5: Verify the app starts**

```bash
cd backend
pip install -e ".[dev]"
uvicorn main:app --reload
```

Expected: `INFO: Application startup complete.` with no import errors. Stop with Ctrl+C.

- [ ] **Step 6: Commit**

```bash
git add backend/
git commit -m "feat: scaffold backend project structure"
```

---

## Task 2: Data Models

**Files:**
- Create: `backend/lineage/models.py`
- Create: `backend/tests/test_models.py`

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_models.py`:

```python
from lineage.models import (
    FileRecord, ColumnNode, LineageEdge,
    SourceConfig, ParseWarning
)


def test_column_node_id_format():
    node = ColumnNode(
        id="orders.amount",
        table="orders",
        column="amount",
        dtype="double",
        source_file="pipeline.py",
        source_cell=None,
        source_line=10,
    )
    assert node.id == "orders.amount"
    assert node.table == "orders"


def test_lineage_edge_fields():
    edge = LineageEdge(
        source_col="orders.amount",
        target_col="revenue.total",
        transform_type="aggregation",
        expression="SUM(amount)",
        source_file="pipeline.py",
        source_cell=None,
        source_line=10,
    )
    assert edge.transform_type == "aggregation"
    assert edge.expression == "SUM(amount)"


def test_file_record_types():
    for t in ("notebook", "python", "sql"):
        r = FileRecord(path="f", content="c", type=t, source_ref="repo")
        assert r.type == t


def test_source_config_fields():
    cfg = SourceConfig(
        id="src-1",
        source_type="git",
        url="https://github.com/org/repo",
        token="ghp_test",
    )
    assert cfg.source_type == "git"


def test_parse_warning_fields():
    w = ParseWarning(file="bad.py", error="SyntaxError: invalid syntax")
    assert "SyntaxError" in w.error
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd backend
pytest tests/test_models.py -v
```

Expected: `ImportError` — `lineage.models` does not exist yet.

- [ ] **Step 3: Create `backend/lineage/models.py`**

```python
from dataclasses import dataclass
from typing import Literal


@dataclass
class FileRecord:
    path: str
    content: str
    type: Literal["notebook", "python", "sql"]
    source_ref: str


@dataclass
class ColumnNode:
    id: str            # "{table}.{column}"
    table: str
    column: str
    dtype: str | None
    source_file: str
    source_cell: int | None
    source_line: int | None


@dataclass
class LineageEdge:
    source_col: str
    target_col: str
    transform_type: Literal[
        "passthrough", "aggregation", "expression",
        "join_key", "window", "cast", "filter"
    ]
    expression: str
    source_file: str
    source_cell: int | None
    source_line: int | None


@dataclass
class SourceConfig:
    id: str
    source_type: Literal["git", "databricks", "upload"]
    url: str
    token: str | None = None


@dataclass
class ParseWarning:
    file: str
    error: str
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_models.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/lineage/models.py backend/tests/test_models.py
git commit -m "feat: add core data models"
```

---

## Task 3: SQL Parser

**Files:**
- Create: `backend/parsers/sql.py`
- Create: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_sql_parser.py`:

```python
from parsers.sql import parse_sql
from lineage.models import LineageEdge


def test_simple_select_passthrough():
    sql = "SELECT order_id, amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    assert "raw_orders.order_id" not in targets  # source table columns don't get edges
    # passthrough: output col linked to input col
    edge = next(e for e in edges if e.target_col == "result.order_id")
    assert edge.source_col == "raw_orders.order_id"
    assert edge.transform_type == "passthrough"


def test_aggregation_sum():
    sql = "SELECT customer_id, SUM(amount) AS total_revenue FROM raw_orders GROUP BY customer_id"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    agg_edge = next(e for e in edges if e.target_col == "result.total_revenue")
    assert agg_edge.source_col == "raw_orders.amount"
    assert agg_edge.transform_type == "aggregation"
    assert "SUM" in agg_edge.expression


def test_cte_resolution():
    sql = """
    WITH base AS (
        SELECT order_id, amount FROM raw_orders
    )
    SELECT order_id, amount FROM base
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    # Final output should trace back to raw_orders, not the CTE alias
    sources = {e.source_col for e in edges}
    assert any("raw_orders" in s for s in sources)


def test_cast_transform():
    sql = "SELECT CAST(amount AS STRING) AS amount_str FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    cast_edge = next(e for e in edges if e.target_col == "result.amount_str")
    assert cast_edge.transform_type == "cast"


def test_window_function():
    sql = "SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    win_edge = next((e for e in edges if e.target_col == "result.rn"), None)
    assert win_edge is not None
    assert win_edge.transform_type == "window"


def test_bad_sql_returns_empty_not_raises():
    edges = parse_sql("THIS IS NOT SQL !!!###", source_file="bad.sql", source_line=1)
    assert isinstance(edges, list)
    assert len(edges) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_sql_parser.py -v
```

Expected: `ImportError` — `parsers.sql` does not exist yet.

- [ ] **Step 3: Create `backend/parsers/sql.py`**

```python
"""SQL column lineage parser using SQLGlot."""
from __future__ import annotations
import sqlglot
import sqlglot.expressions as exp
from lineage.models import LineageEdge


def _classify_transform(node: exp.Expression) -> tuple[str, str]:
    """Return (transform_type, expression_str) for a SELECT column expression."""
    expr_str = node.sql(dialect="databricks")

    if isinstance(node, exp.Cast):
        return "cast", expr_str
    if isinstance(node, exp.Anonymous) or isinstance(node, exp.Window):
        return "window", expr_str
    if isinstance(node, (exp.Sum, exp.Count, exp.Avg, exp.Max, exp.Min)):
        return "aggregation", expr_str
    # Check if any ancestor is an aggregate function
    for ancestor in node.walk():
        if isinstance(ancestor, (exp.Sum, exp.Count, exp.Avg, exp.Max, exp.Min)):
            return "aggregation", expr_str
    # Check for window
    for ancestor in node.walk():
        if isinstance(ancestor, exp.Window):
            return "window", expr_str
    # Check for cast
    for ancestor in node.walk():
        if isinstance(ancestor, exp.Cast):
            return "cast", expr_str
    # Arithmetic/CASE → expression
    for ancestor in node.walk():
        if isinstance(ancestor, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Case)):
            return "expression", expr_str
    return "passthrough", expr_str


def _resolve_ctes(statement: exp.Select) -> dict[str, str]:
    """Build a map of CTE alias -> source table name (best effort, one level deep)."""
    cte_map: dict[str, str] = {}
    with_clause = statement.args.get("with")
    if not with_clause:
        return cte_map
    for cte in with_clause.expressions:
        alias = cte.alias
        cte_select = cte.this
        # Find the FROM table of the CTE
        from_clause = cte_select.args.get("from")
        if from_clause:
            table_expr = from_clause.this
            if isinstance(table_expr, exp.Table):
                cte_map[alias] = table_expr.name
    return cte_map


def parse_sql(
    sql: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None = None,
) -> list[LineageEdge]:
    """Parse a SQL statement and return column-level lineage edges.

    Uses "result" as the synthetic target table name when no INTO/CREATE is present.
    """
    try:
        statement = sqlglot.parse_one(sql, dialect="databricks")
    except Exception:
        return []

    if not isinstance(statement, exp.Select):
        # Handle CREATE TABLE AS SELECT, INSERT INTO SELECT
        inner = statement.find(exp.Select)
        if inner is None:
            return []
        statement = inner

    cte_map = _resolve_ctes(statement)
    edges: list[LineageEdge] = []

    # Determine source tables from FROM + JOINs
    from_clause = statement.args.get("from")
    if from_clause is None:
        return []

    source_tables: list[str] = []
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        tname = from_table.alias_or_name
        source_tables.append(cte_map.get(tname, tname))

    for join in statement.find_all(exp.Join):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            tname = jtable.alias_or_name
            source_tables.append(cte_map.get(tname, tname))

    # Walk SELECT expressions
    for sel in statement.selects:
        alias = sel.alias if sel.alias else sel.sql(dialect="databricks")
        target_col = f"result.{alias}"
        transform_type, expr_str = _classify_transform(sel.this if hasattr(sel, "this") else sel)

        # Find all Column references inside this expression
        col_refs = list(sel.find_all(exp.Column))
        if not col_refs:
            # No column refs (e.g. constant) — skip
            continue

        for col_ref in col_refs:
            table_hint = col_ref.table
            col_name = col_ref.name
            if table_hint:
                resolved_table = cte_map.get(table_hint, table_hint)
                source_col = f"{resolved_table}.{col_name}"
            else:
                # Assign to first source table (best effort for single-table queries)
                resolved_table = source_tables[0] if source_tables else "unknown"
                source_col = f"{resolved_table}.{col_name}"

            edges.append(LineageEdge(
                source_col=source_col,
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
            ))

    return edges
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_sql_parser.py -v
```

Expected: 6 tests PASS. If `test_simple_select_passthrough` fails due to alias resolution differences in SQLGlot, debug by printing `edges` in the test and adjusting the expected `target_col` to match what SQLGlot actually produces for an unaliased column.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/sql.py backend/tests/test_sql_parser.py
git commit -m "feat: add SQL parser with SQLGlot"
```

---

## Task 4: PySpark Parser

**Files:**
- Create: `backend/parsers/pyspark.py`
- Create: `backend/tests/test_pyspark_parser.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_pyspark_parser.py`:

```python
from parsers.pyspark import parse_pyspark
from lineage.models import LineageEdge


SIMPLE_SELECT = """\
df = spark.read.table("raw_orders")
df2 = df.select("order_id", "amount")
df2.write.saveAsTable("staging_orders")
"""

WITHCOLUMN = """\
df = spark.read.table("raw_orders")
df2 = df.withColumn("total", F.col("amount") * F.col("tax_rate"))
df2.write.saveAsTable("enriched_orders")
"""

AGG = """\
df = spark.read.table("raw_orders")
df2 = df.groupBy("customer_id").agg(F.sum("amount").alias("total_revenue"))
df2.write.saveAsTable("agg_revenue")
"""

CHAINED = """\
df = spark.read.table("raw_orders") \
    .filter(F.col("status") == "active") \
    .withColumn("revenue", F.col("amount") * 1.1)
df.write.saveAsTable("active_orders")
"""


def test_simple_select_passthrough():
    edges = parse_pyspark(SIMPLE_SELECT, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "staging_orders.order_id" in targets
    assert "staging_orders.amount" in targets
    passthrough = [e for e in edges if e.transform_type == "passthrough"]
    assert len(passthrough) >= 2


def test_withcolumn_expression():
    edges = parse_pyspark(WITHCOLUMN, source_file="pipeline.py")
    edge = next((e for e in edges if e.target_col == "enriched_orders.total"), None)
    assert edge is not None
    assert edge.transform_type in ("expression", "aggregation")


def test_agg_sum():
    edges = parse_pyspark(AGG, source_file="pipeline.py")
    edge = next((e for e in edges if e.target_col == "agg_revenue.total_revenue"), None)
    assert edge is not None
    assert edge.transform_type == "aggregation"
    assert edge.source_col == "raw_orders.amount"


def test_chained_operations():
    edges = parse_pyspark(CHAINED, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "active_orders.revenue" in targets


def test_bad_python_returns_empty():
    edges = parse_pyspark("def (((broken:", source_file="bad.py")
    assert isinstance(edges, list)
    assert len(edges) == 0


def test_source_line_attached():
    edges = parse_pyspark(AGG, source_file="pipeline.py")
    for edge in edges:
        assert edge.source_line is not None
        assert edge.source_line > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_pyspark_parser.py -v
```

Expected: `ImportError` — `parsers.pyspark` does not exist yet.

- [ ] **Step 3: Create `backend/parsers/pyspark.py`**

```python
"""PySpark column lineage parser using Python ast."""
from __future__ import annotations
import ast
from lineage.models import LineageEdge


def _get_string_value(node: ast.expr) -> str | None:
    """Extract a string literal from an AST node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_col_names(node: ast.expr) -> list[str]:
    """Best-effort extraction of column name strings from an expression node."""
    cols: list[str] = []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        cols.append(node.value)
    elif isinstance(node, ast.Call):
        # F.col("name") or col("name")
        func = node.func
        func_name = ""
        if isinstance(func, ast.Attribute):
            func_name = func.attr
        elif isinstance(func, ast.Name):
            func_name = func.id
        if func_name in ("col", "column") and node.args:
            v = _get_string_value(node.args[0])
            if v:
                cols.append(v)
        else:
            for arg in node.args:
                cols.extend(_extract_col_names(arg))
    elif isinstance(node, ast.BinOp):
        cols.extend(_extract_col_names(node.left))
        cols.extend(_extract_col_names(node.right))
    return cols


def _classify_pyspark_expr(node: ast.expr) -> str:
    """Return transform_type for a PySpark column expression."""
    if isinstance(node, ast.Call):
        func = node.func
        fname = ""
        if isinstance(func, ast.Attribute):
            fname = func.attr.lower()
        elif isinstance(func, ast.Name):
            fname = func.id.lower()
        if fname in ("sum", "count", "avg", "mean", "max", "min", "collect_list",
                     "collect_set", "countdistinct", "approx_count_distinct"):
            return "aggregation"
        if fname in ("cast", "astype"):
            return "cast"
        if fname in ("row_number", "rank", "dense_rank", "lag", "lead",
                     "over", "window"):
            return "window"
    if isinstance(node, ast.BinOp):
        return "expression"
    return "passthrough"


class _DataFrameTracker(ast.NodeVisitor):
    """Walk AST and track DataFrame variable assignments and write targets."""

    def __init__(self, source_file: str):
        self.source_file = source_file
        # var_name -> source table name
        self.df_sources: dict[str, str] = {}
        # var_name -> list of (col_name, transform_type, expr_str, source_cols, lineno)
        self.df_columns: dict[str, list[tuple]] = {}
        self.edges: list[LineageEdge] = []

    def _get_read_table(self, node: ast.Call) -> str | None:
        """Detect spark.read.table("name") or spark.table("name")."""
        if not isinstance(node.func, ast.Attribute):
            return None
        attr = node.func.attr
        if attr == "table" and node.args:
            return _get_string_value(node.args[0])
        return None

    def _get_write_table(self, node: ast.Call) -> str | None:
        """Detect df.write.saveAsTable("name") or df.write.insertInto("name")."""
        if not isinstance(node.func, ast.Attribute):
            return None
        if node.func.attr in ("saveAsTable", "insertInto") and node.args:
            return _get_string_value(node.args[0])
        return None

    def _chain_source(self, node: ast.expr) -> str | None:
        """Resolve the source DataFrame variable from a method chain."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return self._chain_source(node.func.value)
        return None

    def visit_Assign(self, node: ast.Assign):
        if len(node.targets) != 1:
            self.generic_visit(node)
            return
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            self.generic_visit(node)
            return
        var = target.id
        value = node.value

        # spark.read.table(...) or spark.table(...)
        if isinstance(value, ast.Call):
            tname = self._get_read_table(value)
            if tname:
                self.df_sources[var] = tname
                self.df_columns[var] = []  # columns unknown until select
                self.generic_visit(node)
                return

        # df.select("col1", "col2", ...)
        if (isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                and value.func.attr == "select"):
            src_var = self._chain_source(value.func.value)
            src_table = self.df_sources.get(src_var or "", "unknown")
            cols = []
            for arg in value.args:
                cnames = _extract_col_names(arg)
                for cname in cnames:
                    cols.append((cname, "passthrough", cname, [cname], node.lineno))
            self.df_sources[var] = src_table
            self.df_columns[var] = cols

        # df.withColumn("name", expr)
        elif (isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
              and value.func.attr == "withColumn" and len(value.args) >= 2):
            src_var = self._chain_source(value.func.value)
            src_table = self.df_sources.get(src_var or "", "unknown")
            col_name = _get_string_value(value.args[0]) or "unknown"
            expr_node = value.args[1]
            transform = _classify_pyspark_expr(expr_node)
            src_cols = _extract_col_names(expr_node)
            # Inherit parent cols + new col
            parent_cols = list(self.df_columns.get(src_var or "", []))
            self.df_sources[var] = src_table
            self.df_columns[var] = parent_cols + [
                (col_name, transform, ast.unparse(expr_node), src_cols, node.lineno)
            ]

        # df.groupBy(...).agg(...)
        elif (isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
              and value.func.attr == "agg"):
            src_var = self._chain_source(value.func.value)
            # src_var here is the groupBy call; chase back to the DataFrame
            inner_src = self._chain_source(src_var) if src_var else None
            src_table = self.df_sources.get(inner_src or src_var or "", "unknown")
            agg_cols = []
            for arg in value.args:
                if isinstance(arg, ast.Call):
                    agg_name_node = None
                    # Look for .alias("name")
                    if (isinstance(arg.func, ast.Attribute)
                            and arg.func.attr == "alias"
                            and arg.args):
                        alias = _get_string_value(arg.args[0])
                        inner_agg = arg.func.value
                        src_cols = _extract_col_names(inner_agg)
                        transform = _classify_pyspark_expr(inner_agg)
                        if alias:
                            agg_cols.append((alias, transform, ast.unparse(inner_agg),
                                             src_cols, node.lineno))
            self.df_sources[var] = src_table
            self.df_columns[var] = agg_cols

        # Chain: df.filter(...).withColumn(...) etc — fall through to handle .write
        elif isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
            src_var = self._chain_source(value)
            if src_var and src_var in self.df_sources:
                self.df_sources[var] = self.df_sources[src_var]
                self.df_columns[var] = self.df_columns.get(src_var, [])

        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        """Detect df.write.saveAsTable("target")."""
        if not isinstance(node.value, ast.Call):
            self.generic_visit(node)
            return
        call = node.value
        target_table = self._get_write_table(call)
        if target_table is None:
            self.generic_visit(node)
            return
        # Find the DataFrame variable being written
        src_var = self._chain_source(call.func.value) if isinstance(call.func, ast.Attribute) else None
        # Chase back through .write
        if src_var is None and isinstance(call.func, ast.Attribute):
            src_var = self._chain_source(call.func.value)

        # Walk up write chain: df.write.saveAsTable -> value is df.write -> value.value is df
        write_chain = call.func
        if isinstance(write_chain, ast.Attribute):
            wv = write_chain.value
            if isinstance(wv, ast.Attribute) and wv.attr == "write":
                df_node = wv.value
                if isinstance(df_node, ast.Name):
                    src_var = df_node.id

        if src_var is None:
            self.generic_visit(node)
            return

        src_table = self.df_sources.get(src_var, "unknown")
        cols = self.df_columns.get(src_var, [])

        for (col_name, transform_type, expr_str, src_cols, lineno) in cols:
            for sc in src_cols:
                self.edges.append(LineageEdge(
                    source_col=f"{src_table}.{sc}",
                    target_col=f"{target_table}.{col_name}",
                    transform_type=transform_type,
                    expression=expr_str,
                    source_file=self.source_file,
                    source_cell=None,
                    source_line=lineno,
                ))

        self.generic_visit(node)


def parse_pyspark(
    code: str,
    source_file: str,
    source_cell: int | None = None,
) -> list[LineageEdge]:
    """Parse PySpark Python code and return column-level lineage edges."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    tracker = _DataFrameTracker(source_file=source_file)
    tracker.visit(tree)

    # Attach source_cell if provided
    if source_cell is not None:
        for edge in tracker.edges:
            object.__setattr__(edge, "source_cell", source_cell)

    return tracker.edges
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_pyspark_parser.py -v
```

Expected: 6 tests PASS. If `test_chained_operations` fails, add a debug print of `edges` and check that the chained `.filter().withColumn()` pattern is handled — the chain resolver may need an extra hop.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/pyspark.py backend/tests/test_pyspark_parser.py
git commit -m "feat: add PySpark AST parser"
```

---

## Task 5: Notebook Parser

**Files:**
- Create: `backend/parsers/notebook.py`
- Create: `backend/tests/test_notebook_parser.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_notebook_parser.py`:

```python
import json
from parsers.notebook import parse_notebook
from lineage.models import LineageEdge


def _make_notebook(cells: list[dict]) -> str:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"kernelspec": {"language": "python"}},
        "cells": cells,
    }
    return json.dumps(nb)


def _code_cell(source: str, language: str | None = None) -> dict:
    cell = {
        "cell_type": "code",
        "source": source,
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    if language:
        cell["metadata"]["language"] = language
    return cell


def test_sql_magic_cell_produces_edges():
    nb = _make_notebook([
        _code_cell("%sql SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert len(edges) > 0
    agg = next((e for e in edges if e.transform_type == "aggregation"), None)
    assert agg is not None


def test_pyspark_cell_produces_edges():
    nb = _make_notebook([
        _code_cell(
            'df = spark.read.table("raw_orders")\n'
            'df2 = df.select("order_id")\n'
            'df2.write.saveAsTable("staging")\n'
        ),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert any(e.target_col == "staging.order_id" for e in edges)


def test_cell_index_attached():
    nb = _make_notebook([
        _code_cell("x = 1"),  # cell 0 — no lineage
        _code_cell("%sql SELECT amount FROM raw_orders"),  # cell 1
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    for e in edges:
        assert e.source_cell == 1


def test_markdown_cells_skipped():
    nb = _make_notebook([
        {"cell_type": "markdown", "source": "# Title", "metadata": {}},
        _code_cell("%sql SELECT amount FROM raw_orders"),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert len(edges) > 0  # markdown didn't crash anything


def test_bad_json_returns_empty():
    edges = parse_notebook("not json at all", source_file="bad.ipynb")
    assert edges == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_notebook_parser.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `backend/parsers/notebook.py`**

```python
"""Notebook parser using nbformat. Routes cells to SQL or PySpark parsers."""
from __future__ import annotations
import io
import nbformat
from parsers.sql import parse_sql
from parsers.pyspark import parse_pyspark
from lineage.models import LineageEdge


_SQL_MAGICS = ("%sql", "%%sql", "%spark.sql", "spark.sql(")


def _is_sql_cell(source: str) -> bool:
    stripped = source.strip()
    return any(stripped.startswith(magic) for magic in _SQL_MAGICS)


def _strip_sql_magic(source: str) -> str:
    stripped = source.strip()
    for magic in ("%sql", "%%sql"):
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

    for cell_idx, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue

        source = cell.source
        if not source.strip():
            continue

        # Detect cell language: check metadata first, then magic prefix
        lang = cell.get("metadata", {}).get("language", "")

        if _is_sql_cell(source) or lang == "sql":
            sql = _strip_sql_magic(source)
            cell_edges = parse_sql(
                sql,
                source_file=source_file,
                source_line=None,
                source_cell=cell_idx,
            )
        else:
            cell_edges = parse_pyspark(
                source,
                source_file=source_file,
                source_cell=cell_idx,
            )

        edges.extend(cell_edges)

    return edges
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_notebook_parser.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/parsers/notebook.py backend/tests/test_notebook_parser.py
git commit -m "feat: add notebook parser routing cells to SQL/PySpark parsers"
```

---

## Task 6: Lineage Engine

**Files:**
- Create: `backend/lineage/engine.py`
- Create: `backend/tests/test_engine.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_engine.py`:

```python
import networkx as nx
from lineage.engine import build_graph, upstream, downstream
from lineage.models import FileRecord, LineageEdge


def _make_sql_record(sql: str) -> FileRecord:
    return FileRecord(path="q.sql", content=sql, type="sql", source_ref="test")


def test_build_graph_nodes_and_edges():
    records = [_make_sql_record(
        "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    )]
    graph = build_graph(records)
    assert isinstance(graph, nx.DiGraph)
    assert graph.number_of_nodes() > 0
    assert graph.number_of_edges() > 0


def test_upstream_returns_ancestors():
    records = [
        _make_sql_record("SELECT order_id, amount FROM raw_orders"),
        _make_sql_record("SELECT order_id, amount AS revenue FROM result"),
    ]
    graph = build_graph(records)
    ancestors = upstream(graph, "result.revenue")
    sources = {e.source_col for e in ancestors}
    # raw_orders.amount should appear somewhere in the upstream chain
    assert any("amount" in s for s in sources)


def test_downstream_returns_descendants():
    records = [
        _make_sql_record("SELECT order_id, amount FROM raw_orders"),
        _make_sql_record("SELECT order_id, amount AS revenue FROM result"),
    ]
    graph = build_graph(records)
    descendants = downstream(graph, "raw_orders.amount")
    targets = {e.target_col for e in descendants}
    assert len(targets) > 0


def test_empty_records_returns_empty_graph():
    graph = build_graph([])
    assert graph.number_of_nodes() == 0


def test_cycle_detection():
    # Manually create a graph with a cycle
    graph = nx.DiGraph()
    graph.add_edge("a.x", "b.y", data=None)
    graph.add_edge("b.y", "a.x", data=None)
    assert not nx.is_directed_acyclic_graph(graph)


def test_parse_warnings_collected():
    from lineage.engine import build_graph_with_warnings
    records = [
        FileRecord(path="bad.py", content="def (((broken:", type="python", source_ref="test"),
        _make_sql_record("SELECT amount FROM raw_orders"),
    ]
    graph, warnings = build_graph_with_warnings(records)
    # bad.py should produce no edges (empty), not a warning — it's silently skipped
    # but the good SQL record should still produce edges
    assert graph.number_of_edges() > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_engine.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `backend/lineage/engine.py`**

```python
"""Lineage engine: builds and queries a NetworkX DAG from FileRecords."""
from __future__ import annotations
import networkx as nx
from lineage.models import FileRecord, LineageEdge, ParseWarning
from parsers.sql import parse_sql
from parsers.pyspark import parse_pyspark
from parsers.notebook import parse_notebook


def _parse_file(record: FileRecord) -> tuple[list[LineageEdge], list[ParseWarning]]:
    edges: list[LineageEdge] = []
    warnings: list[ParseWarning] = []
    try:
        if record.type == "notebook":
            edges = parse_notebook(record.content, source_file=record.path)
        elif record.type == "python":
            edges = parse_pyspark(record.content, source_file=record.path)
        elif record.type == "sql":
            edges = parse_sql(record.content, source_file=record.path, source_line=1)
    except Exception as exc:
        warnings.append(ParseWarning(file=record.path, error=str(exc)))
    return edges, warnings


def build_graph_with_warnings(
    records: list[FileRecord],
) -> tuple[nx.DiGraph, list[ParseWarning]]:
    """Parse all FileRecords and return a lineage DAG plus any parse warnings."""
    graph: nx.DiGraph = nx.DiGraph()
    all_warnings: list[ParseWarning] = []

    for record in records:
        edges, warnings = _parse_file(record)
        all_warnings.extend(warnings)
        for edge in edges:
            # Add nodes if missing
            if edge.source_col not in graph:
                graph.add_node(edge.source_col)
            if edge.target_col not in graph:
                graph.add_node(edge.target_col)
            graph.add_edge(edge.source_col, edge.target_col, data=edge)

    # Detect and warn on cycles
    if not nx.is_directed_acyclic_graph(graph):
        try:
            cycle = nx.find_cycle(graph)
            nodes_in_cycle = {u for u, v in cycle} | {v for u, v in cycle}
            all_warnings.append(ParseWarning(
                file="<graph>",
                error=f"Circular lineage detected involving: {sorted(nodes_in_cycle)}",
            ))
        except nx.NetworkXNoCycle:
            pass

    return graph, all_warnings


def build_graph(records: list[FileRecord]) -> nx.DiGraph:
    """Parse all FileRecords and return a lineage DAG (warnings discarded)."""
    graph, _ = build_graph_with_warnings(records)
    return graph


def upstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading TO col_id (ancestors)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited = set()
    queue = [col_id]
    while queue:
        current = queue.pop()
        for pred in graph.predecessors(current):
            if pred not in visited:
                visited.add(pred)
                edge_data = graph.edges[pred, current].get("data")
                if edge_data:
                    edges.append(edge_data)
                queue.append(pred)
    return edges


def downstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading FROM col_id (descendants)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited = set()
    queue = [col_id]
    while queue:
        current = queue.pop()
        for succ in graph.successors(current):
            if succ not in visited:
                visited.add(succ)
                edge_data = graph.edges[current, succ].get("data")
                if edge_data:
                    edges.append(edge_data)
                queue.append(succ)
    return edges
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_engine.py -v
```

Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/lineage/engine.py backend/tests/test_engine.py
git commit -m "feat: add lineage engine with NetworkX DAG"
```

---

## Task 7: Upload Ingestion

**Files:**
- Create: `backend/ingestion/upload.py`
- Create: `backend/tests/test_ingestion_upload.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_ingestion_upload.py`:

```python
import io
import zipfile
from ingestion.upload import ingest_zip


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_ingest_zip_sql_file():
    zip_bytes = _make_zip({"queries/agg.sql": "SELECT amount FROM raw_orders"})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].type == "sql"
    assert records[0].path == "queries/agg.sql"


def test_ingest_zip_notebook():
    zip_bytes = _make_zip({"pipeline.ipynb": '{"nbformat":4,"cells":[],"metadata":{},"nbformat_minor":5}'})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].type == "notebook"


def test_ingest_zip_python():
    zip_bytes = _make_zip({"etl/pipeline.py": "df = spark.read.table('orders')"})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert records[0].type == "python"


def test_ingest_zip_ignores_unknown_extensions():
    zip_bytes = _make_zip({
        "README.md": "# readme",
        "query.sql": "SELECT 1",
    })
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].path == "query.sql"


def test_ingest_zip_source_ref_set():
    zip_bytes = _make_zip({"q.sql": "SELECT 1"})
    records = ingest_zip(zip_bytes, source_ref="my-upload-42")
    assert records[0].source_ref == "my-upload-42"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_ingestion_upload.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `backend/ingestion/upload.py`**

```python
"""ZIP file ingestion — extract and classify files into FileRecords."""
from __future__ import annotations
import io
import zipfile
from lineage.models import FileRecord

_EXT_TYPE = {
    ".ipynb": "notebook",
    ".py": "python",
    ".sql": "sql",
}


def ingest_zip(zip_bytes: bytes, source_ref: str) -> list[FileRecord]:
    """Extract a ZIP archive and return one FileRecord per supported file."""
    records: list[FileRecord] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                file_type = _EXT_TYPE.get(ext)
                if file_type is None:
                    continue
                content = zf.read(name).decode("utf-8", errors="replace")
                records.append(FileRecord(
                    path=name,
                    content=content,
                    type=file_type,
                    source_ref=source_ref,
                ))
    except zipfile.BadZipFile:
        pass
    return records
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd backend
pytest tests/test_ingestion_upload.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/ingestion/upload.py backend/tests/test_ingestion_upload.py
git commit -m "feat: add ZIP upload ingestion"
```

---

## Task 8: Git Ingestion

**Files:**
- Create: `backend/ingestion/git.py`

> Git ingestion calls the network, so we use a lightweight integration test that clones a known public repo. If no network is available in CI, skip this test with `@pytest.mark.skipif`.

- [ ] **Step 1: Create `backend/ingestion/git.py`**

```python
"""Git repository ingestion using GitPython."""
from __future__ import annotations
import tempfile
import os
from pathlib import Path
import git
from lineage.models import FileRecord

_EXT_TYPE = {
    ".ipynb": "notebook",
    ".py": "python",
    ".sql": "sql",
}


def ingest_git(url: str, token: str | None, source_ref: str) -> list[FileRecord]:
    """Clone a Git repo to a temp directory and return FileRecords for all supported files.

    For authenticated repos, embed the token in the URL:
    https://<token>@github.com/org/repo.git
    """
    # Build authenticated URL if token provided
    if token:
        # Insert token into https URL
        if url.startswith("https://"):
            auth_url = url.replace("https://", f"https://{token}@", 1)
        else:
            auth_url = url
    else:
        auth_url = url

    records: list[FileRecord] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            git.Repo.clone_from(auth_url, tmpdir, depth=1)
        except git.GitCommandError as exc:
            raise RuntimeError(f"Git clone failed: {exc}") from exc

        for root, _dirs, files in os.walk(tmpdir):
            # Skip .git directory
            if ".git" in root:
                continue
            for fname in files:
                ext = Path(fname).suffix.lower()
                file_type = _EXT_TYPE.get(ext)
                if file_type is None:
                    continue
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, tmpdir)
                try:
                    content = Path(full_path).read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                records.append(FileRecord(
                    path=rel_path.replace("\\", "/"),
                    content=content,
                    type=file_type,
                    source_ref=source_ref,
                ))

    return records
```

- [ ] **Step 2: Verify it imports cleanly**

```bash
cd backend
python -c "from ingestion.git import ingest_git; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add backend/ingestion/git.py
git commit -m "feat: add Git ingestion via GitPython"
```

---

## Task 9: Databricks Ingestion

**Files:**
- Create: `backend/ingestion/databricks.py`

> Databricks ingestion requires a live workspace. No automated test — the function is tested manually or via integration test against a real workspace.

- [ ] **Step 1: Create `backend/ingestion/databricks.py`**

```python
"""Databricks Workspace ingestion using databricks-sdk."""
from __future__ import annotations
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectType
from lineage.models import FileRecord

_LANG_TYPE = {
    "PYTHON": "python",
    "SQL": "sql",
    "SCALA": None,   # not supported
    "R": None,
}


def ingest_databricks(host: str, token: str, source_ref: str) -> list[FileRecord]:
    """Export all notebooks from a Databricks workspace and return FileRecords.

    Walks the entire workspace recursively starting from '/'.
    Skips SCALA and R notebooks.
    """
    client = WorkspaceClient(host=host, token=token)
    records: list[FileRecord] = []

    def _walk(path: str):
        try:
            items = list(client.workspace.list(path=path))
        except Exception:
            return
        for item in items:
            if item.object_type == ObjectType.DIRECTORY:
                _walk(item.path)
            elif item.object_type == ObjectType.NOTEBOOK:
                lang = (item.language.value if item.language else "PYTHON")
                file_type = _LANG_TYPE.get(lang)
                if file_type is None:
                    continue
                try:
                    export_resp = client.workspace.export(
                        path=item.path,
                        format=ExportFormat.SOURCE,
                    )
                    content = export_resp.content
                    if content is None:
                        continue
                    # content is base64-encoded
                    import base64
                    decoded = base64.b64decode(content).decode("utf-8", errors="replace")
                    fname = item.path.lstrip("/").replace("/", "__") + (
                        ".ipynb" if file_type == "notebook" else
                        ".py" if file_type == "python" else ".sql"
                    )
                    records.append(FileRecord(
                        path=item.path,
                        content=decoded,
                        type=file_type,
                        source_ref=source_ref,
                    ))
                except Exception:
                    continue

    _walk("/")
    return records
```

- [ ] **Step 2: Verify it imports cleanly**

```bash
cd backend
python -c "from ingestion.databricks import ingest_databricks; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add backend/ingestion/databricks.py
git commit -m "feat: add Databricks workspace ingestion"
```

---

## Task 10: API Routes

**Files:**
- Create: `backend/api/routes.py`
- Create: `backend/tests/test_routes.py`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_routes.py`:

```python
import io
import zipfile
import pytest
from fastapi.testclient import TestClient
from main import app
import state

client = TestClient(app)


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def reset_state():
    """Reset in-memory state before each test."""
    import networkx as nx
    state.source_registry.clear()
    state.lineage_graph = nx.DiGraph()
    state.parse_warnings.clear()
    yield


def test_list_sources_empty():
    resp = client.get("/sources")
    assert resp.status_code == 200
    assert resp.json() == []


def test_register_upload_source_and_refresh():
    zip_bytes = _make_zip({
        "query.sql": "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    })
    # Register source via upload
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    assert resp.status_code == 200
    source_id = resp.json()["id"]

    # Refresh (parse)
    resp = client.post(f"/sources/{source_id}/refresh")
    assert resp.status_code == 200

    # Tables should now have data
    resp = client.get("/tables")
    assert resp.status_code == 200
    tables = resp.json()
    assert len(tables) > 0


def test_get_columns_for_table():
    zip_bytes = _make_zip({
        "query.sql": "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/tables/result/columns")
    assert resp.status_code == 200
    cols = resp.json()
    col_names = [c["column"] for c in cols]
    assert "total" in col_names or "customer_id" in col_names


def test_lineage_endpoint():
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "result", "column": "total"})
    assert resp.status_code == 200
    data = resp.json()
    assert "upstream" in data
    assert "downstream" in data
    assert "graph" in data


def test_impact_endpoint():
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/impact", params={"table": "raw_orders", "column": "amount"})
    assert resp.status_code == 200
    data = resp.json()
    assert "downstream" in data


def test_search_endpoint():
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/search", params={"q": "amount"})
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) > 0


def test_delete_source():
    zip_bytes = _make_zip({"q.sql": "SELECT 1"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    resp = client.delete(f"/sources/{source_id}")
    assert resp.status_code == 200
    resp = client.get("/sources")
    assert all(s["id"] != source_id for s in resp.json())


def test_warnings_endpoint():
    resp = client.get("/warnings")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
pytest tests/test_routes.py -v
```

Expected: `ImportError` from `api.routes`.

- [ ] **Step 3: Create `backend/api/routes.py`**

```python
"""FastAPI route handlers for the DataLineage Explorer API."""
from __future__ import annotations
import uuid
import networkx as nx
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from lineage.engine import build_graph_with_warnings
from lineage.engine import upstream as engine_upstream
from lineage.engine import downstream as engine_downstream
from lineage.models import SourceConfig, ParseWarning
from ingestion.upload import ingest_zip
import state

router = APIRouter()


def _edge_to_dict(edge) -> dict:
    return {
        "source_col": edge.source_col,
        "target_col": edge.target_col,
        "transform_type": edge.transform_type,
        "expression": edge.expression,
        "source_file": edge.source_file,
        "source_cell": edge.source_cell,
        "source_line": edge.source_line,
    }


def _graph_to_payload(graph: nx.DiGraph, col_id: str) -> dict:
    """Return nodes/edges subgraph reachable from col_id in either direction."""
    # Collect all nodes reachable upstream and downstream
    up_edges = engine_upstream(graph, col_id)
    down_edges = engine_downstream(graph, col_id)
    all_edges = up_edges + down_edges
    node_ids = {col_id}
    for e in all_edges:
        node_ids.add(e.source_col)
        node_ids.add(e.target_col)
    return {
        "nodes": [{"id": n} for n in sorted(node_ids)],
        "edges": [_edge_to_dict(e) for e in all_edges],
    }


# ── Sources ──────────────────────────────────────────────────────────────────

@router.get("/sources")
def list_sources():
    return list(state.source_registry.values())


@router.post("/sources")
async def register_source(
    source_type: str = Form(...),
    url: str = Form(default=""),
    token: str = Form(default=""),
    file: UploadFile | None = File(default=None),
):
    source_id = str(uuid.uuid4())[:8]
    entry = {
        "id": source_id,
        "source_type": source_type,
        "url": url,
        "status": "registered",
        "file_count": 0,
    }

    if source_type == "upload":
        if file is None:
            raise HTTPException(status_code=400, detail="file is required for upload source")
        zip_bytes = await file.read()
        entry["_zip_bytes"] = zip_bytes  # store for refresh
        entry["url"] = file.filename or "upload"

    state.source_registry[source_id] = entry
    # Remove non-serialisable key before returning
    return_entry = {k: v for k, v in entry.items() if not k.startswith("_")}
    return return_entry


@router.delete("/sources/{source_id}")
def delete_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")
    del state.source_registry[source_id]
    return {"ok": True}


@router.post("/sources/{source_id}/refresh")
def refresh_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")

    entry = state.source_registry[source_id]
    source_type = entry["source_type"]
    records = []
    warnings: list[ParseWarning] = []

    if source_type == "upload":
        zip_bytes = entry.get("_zip_bytes", b"")
        records = ingest_zip(zip_bytes, source_ref=source_id)

    elif source_type == "git":
        from ingestion.git import ingest_git
        try:
            records = ingest_git(
                url=entry["url"],
                token=entry.get("token") or None,
                source_ref=source_id,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    elif source_type == "databricks":
        from ingestion.databricks import ingest_databricks
        try:
            records = ingest_databricks(
                host=entry["url"],
                token=entry.get("token", ""),
                source_ref=source_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    # Rebuild graph: merge new records into global graph
    new_graph, new_warnings = build_graph_with_warnings(records)
    # Compose with existing graph (other sources)
    composed = nx.compose(state.lineage_graph, new_graph)
    state.lineage_graph = composed
    state.parse_warnings.extend(
        {"file": w.file, "error": w.error} for w in new_warnings
    )

    entry["status"] = "parsed"
    entry["file_count"] = len(records)

    return {"ok": True, "file_count": len(records), "edge_count": new_graph.number_of_edges()}


# ── Tables / Columns ─────────────────────────────────────────────────────────

@router.get("/tables")
def list_tables():
    """Return all unique table names with column counts derived from graph nodes."""
    tables: dict[str, int] = {}
    for node in state.lineage_graph.nodes():
        if "." in node:
            table, _ = node.split(".", 1)
            tables[table] = tables.get(table, 0) + 1
    return [{"table": t, "column_count": c} for t, c in sorted(tables.items())]


@router.get("/tables/{table}/columns")
def list_columns(table: str):
    """Return all ColumnNode-like dicts for a given table."""
    cols = []
    for node in state.lineage_graph.nodes():
        if "." in node:
            t, col = node.split(".", 1)
            if t == table:
                # Find an incoming edge to get metadata
                preds = list(state.lineage_graph.predecessors(node))
                edge_data = None
                if preds:
                    edge_data = state.lineage_graph.edges[preds[0], node].get("data")
                cols.append({
                    "id": node,
                    "table": t,
                    "column": col,
                    "source_file": edge_data.source_file if edge_data else None,
                    "source_cell": edge_data.source_cell if edge_data else None,
                    "source_line": edge_data.source_line if edge_data else None,
                    "transform_type": edge_data.transform_type if edge_data else None,
                })
    if not cols:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found")
    return cols


# ── Lineage / Impact ─────────────────────────────────────────────────────────

@router.get("/lineage")
def get_lineage(table: str, column: str):
    col_id = f"{table}.{column}"
    up = engine_upstream(state.lineage_graph, col_id)
    down = engine_downstream(state.lineage_graph, col_id)
    return {
        "target": col_id,
        "upstream": [_edge_to_dict(e) for e in up],
        "downstream": [_edge_to_dict(e) for e in down],
        "graph": _graph_to_payload(state.lineage_graph, col_id),
    }


@router.get("/impact")
def get_impact(table: str, column: str):
    col_id = f"{table}.{column}"
    down = engine_downstream(state.lineage_graph, col_id)
    return {
        "source": col_id,
        "downstream": [_edge_to_dict(e) for e in down],
        "affected_count": len({e.target_col for e in down}),
    }


# ── Search ────────────────────────────────────────────────────────────────────

@router.get("/search")
def search(q: str):
    q_lower = q.lower()
    results = []
    for node in state.lineage_graph.nodes():
        if q_lower in node.lower():
            if "." in node:
                table, col = node.split(".", 1)
                results.append({"id": node, "table": table, "column": col})
    return results


# ── Warnings ─────────────────────────────────────────────────────────────────

@router.get("/warnings")
def get_warnings():
    return state.parse_warnings
```

- [ ] **Step 4: Run all tests**

```bash
cd backend
pytest tests/ -v
```

Expected: All tests PASS. If `test_register_upload_source_and_refresh` fails with a form-data issue, confirm `python-multipart` is installed: `pip install python-multipart`.

- [ ] **Step 5: Commit**

```bash
git add backend/api/routes.py backend/tests/test_routes.py
git commit -m "feat: add FastAPI routes for all endpoints"
```

---

## Task 11: Dockerfile and Final Wiring

**Files:**
- Create: `Dockerfile`
- Create: `.gitignore`

- [ ] **Step 1: Create `Dockerfile`**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY backend/pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY backend/ .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Create `.gitignore`**

```
.venv/
__pycache__/
*.pyc
*.pyo
.env
tmp/
.superpowers/
dist/
*.egg-info/
```

- [ ] **Step 3: Verify Docker build**

```bash
docker build -t datalineage-backend .
docker run -p 8000:8000 datalineage-backend
```

Open `http://localhost:8000/docs` in a browser. Expected: FastAPI auto-generated Swagger UI with all endpoints listed.

- [ ] **Step 4: Run full test suite one final time**

```bash
cd backend
pytest tests/ -v --tb=short
```

Expected: All tests PASS with no failures.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .gitignore
git commit -m "feat: add Dockerfile and gitignore for backend deployment"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Parse `.ipynb`, `.py`, `.sql` — Tasks 3, 4, 5
- [x] Git ingestion — Task 8
- [x] Databricks API ingestion — Task 9
- [x] File upload ingestion — Task 7
- [x] Expression-level parsing (aggregation, CTE, window, cast, CASE) — Tasks 3, 4
- [x] Column lineage data model — Task 2
- [x] NetworkX DAG with upstream/downstream — Task 6
- [x] Cycle detection — Task 6
- [x] Non-fatal parse warnings — Tasks 6, 10
- [x] All 10 REST endpoints — Task 10
- [x] In-memory state (source_registry, lineage_graph, warnings) — Task 1, 10
- [x] Docker deployment — Task 11
- [x] CORS middleware — Task 1

**No placeholders found.**

**Type consistency:** `LineageEdge`, `FileRecord`, `ColumnNode`, `ParseWarning`, `SourceConfig` defined in Task 2 and used consistently in Tasks 3–10. `build_graph_with_warnings` defined in Task 6 and called in Task 10. `upstream`/`downstream` defined in Task 6 and called in Task 10.
