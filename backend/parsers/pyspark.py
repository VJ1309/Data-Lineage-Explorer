"""PySpark column lineage parser using Python ast."""
from __future__ import annotations
import ast
from lineage.models import LineageEdge
from parsers.sql import parse_sql as _parse_sql
from parsers.sql import _detect_temp_views, _resolve_temp_views


def _get_string_value(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_col_names(node: ast.expr) -> list[str]:
    """Best-effort extraction of column name strings from an expression node."""
    cols: list[str] = []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        cols.append(node.value)
    elif isinstance(node, ast.Call):
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
    fname = ""
    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute):
            fname = func.attr.lower()
        elif isinstance(func, ast.Name):
            fname = func.id.lower()
    if fname in ("sum", "count", "avg", "mean", "max", "min", "collect_list",
                 "collect_set", "countdistinct", "approx_count_distinct"):
        return "aggregation"
    if fname in ("cast", "astype"):
        return "cast"
    if fname in ("row_number", "rank", "dense_rank", "lag", "lead", "over", "window"):
        return "window"
    if isinstance(node, ast.BinOp):
        return "expression"
    return "passthrough"


def _get_call_attr(node: ast.expr) -> str:
    """Return the attribute name of a Call's func if it's an Attribute call."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _chase_df_var(node: ast.expr) -> str | None:
    """Walk a method chain to find the base DataFrame variable name."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return _chase_df_var(node.func.value)
    return None


class _DataFrameTracker(ast.NodeVisitor):
    def __init__(self, source_file: str, resolve_views: bool = True):
        self.source_file = source_file
        self._resolve_views = resolve_views
        self.df_sources: dict[str, str] = {}   # var -> source table
        self.df_columns: dict[str, list[tuple]] = {}  # var -> [(col, transform, expr, src_cols, lineno)]
        self._join_sources: dict[str, list[str]] = {}  # var -> [table1, table2, ...]
        self.edges: list[LineageEdge] = []

    def _propagate_join_sources(self, target_var: str, source_var: str | None) -> None:
        """Copy join source info from source_var to target_var."""
        if source_var and source_var in self._join_sources:
            self._join_sources[target_var] = list(self._join_sources[source_var])

    def _get_read_table(self, node: ast.Call) -> str | None:
        if not isinstance(node.func, ast.Attribute):
            return None
        if node.func.attr == "table" and node.args:
            return _get_string_value(node.args[0])
        return None

    def _get_write_table(self, node: ast.Call) -> str | None:
        if not isinstance(node.func, ast.Attribute):
            return None
        if node.func.attr in ("saveAsTable", "insertInto") and node.args:
            return _get_string_value(node.args[0])
        return None

    def _get_spark_sql(self, node: ast.Call) -> str | None:
        """Extract SQL string from spark.sql('...') calls."""
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "sql":
            return None
        if not node.args:
            return None
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        # Handle f-strings or concatenation — skip (too complex)
        if isinstance(arg, ast.JoinedStr):
            return None
        return None

    def _resolve_source(self, var: str | None) -> tuple[str, list[tuple]]:
        """Return (source_table, columns) for a df variable."""
        if var is None:
            return "unknown", []
        return self.df_sources.get(var, "unknown"), self.df_columns.get(var, [])

    def visit_Assign(self, node: ast.Assign):
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            self.generic_visit(node)
            return

        var = node.targets[0].id
        value = node.value

        # spark.read.table("name") or spark.table("name")
        if isinstance(value, ast.Call):
            tname = self._get_read_table(value)
            if tname:
                self.df_sources[var] = tname
                self.df_columns[var] = []
                self.generic_visit(node)
                return

            # df = spark.sql("SELECT ...") — parse SQL and emit edges directly
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

        # Handle method chains: check outermost call
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
            attr = value.func.attr
            src_var = _chase_df_var(value.func.value)
            src_table, src_cols = self._resolve_source(src_var)

            if attr == "select":
                # Build a set of join key names from the parent for preservation
                parent_join_keys = {
                    c[0] for c in src_cols if c[1] == "join_key"
                }
                cols = []
                for arg in value.args:
                    transform = _classify_pyspark_expr(arg)
                    cnames = _extract_col_names(arg)
                    for cname in cnames:
                        # Preserve join_key transform if this column was a join key
                        if cname in parent_join_keys:
                            cols.append((cname, "join_key", cname, [cname], node.lineno))
                        else:
                            cols.append((cname, transform, cname, [cname], node.lineno))
                self.df_sources[var] = src_table
                self.df_columns[var] = cols
                self._propagate_join_sources(var, src_var)

            elif attr == "withColumn" and len(value.args) >= 2:
                col_name = _get_string_value(value.args[0]) or "unknown"
                expr_node = value.args[1]
                transform = _classify_pyspark_expr(expr_node)
                src_col_names = _extract_col_names(expr_node)
                parent_cols = list(src_cols)
                self.df_sources[var] = src_table
                self.df_columns[var] = parent_cols + [
                    (col_name, transform, ast.unparse(expr_node), src_col_names, node.lineno)
                ]
                self._propagate_join_sources(var, src_var)

            elif attr == "agg":
                # groupBy(...).agg(...) — src_var is the groupBy call, chase further
                inner_src = _chase_df_var(value.func.value)
                inner_table, _ = self._resolve_source(inner_src)
                agg_cols = []
                for arg in value.args:
                    if isinstance(arg, ast.Call) and isinstance(arg.func, ast.Attribute) \
                            and arg.func.attr == "alias" and arg.args:
                        alias_name = _get_string_value(arg.args[0])
                        inner_agg = arg.func.value
                        col_names = _extract_col_names(inner_agg)
                        transform = _classify_pyspark_expr(inner_agg)
                        if alias_name:
                            agg_cols.append((alias_name, transform, ast.unparse(inner_agg),
                                             col_names, node.lineno))
                self.df_sources[var] = inner_table
                self.df_columns[var] = agg_cols

            elif attr == "join" and (len(value.args) >= 2 or any(kw.arg == "on" for kw in value.keywords)):
                # df.join(other_df, on=..., how=...)
                right_arg = value.args[0]
                right_var = None
                if isinstance(right_arg, ast.Name):
                    right_var = right_arg.id

                right_table, right_cols = self._resolve_source(right_var)

                # Extract join keys from the second argument
                join_keys: list[str] = []
                if len(value.args) >= 2:
                    on_arg = value.args[1]
                    if isinstance(on_arg, ast.Constant) and isinstance(on_arg.value, str):
                        join_keys = [on_arg.value]
                    elif isinstance(on_arg, ast.List):
                        for elt in on_arg.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                join_keys.append(elt.value)
                    else:
                        join_keys.extend(_extract_col_names(on_arg))
                # Also check 'on' keyword argument
                for kw in value.keywords:
                    if kw.arg == "on":
                        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                            join_keys = [kw.value.value]
                        elif isinstance(kw.value, ast.List):
                            for elt in kw.value.elts:
                                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                    join_keys.append(elt.value)
                        else:
                            join_keys.extend(_extract_col_names(kw.value))

                # Merge columns from both sides
                merged_cols = list(src_cols)
                for rc in right_cols:
                    if rc not in merged_cols:
                        merged_cols.append(rc)

                # Add join key columns if not already tracked
                for jk in join_keys:
                    # Record join key from left side
                    jk_entry = (jk, "join_key", jk, [jk], node.lineno)
                    if jk_entry not in merged_cols:
                        merged_cols.append(jk_entry)

                # Track both source tables
                self.df_sources[var] = src_table
                self.df_columns[var] = merged_cols
                self._join_sources[var] = [src_table]
                if right_table != "unknown":
                    self._join_sources[var].append(right_table)

            elif attr in ("filter", "where", "dropDuplicates", "drop", "limit",
                          "orderBy", "sort", "distinct", "repartition", "cache"):
                # Pass-through operations: inherit parent df
                self.df_sources[var] = src_table
                self.df_columns[var] = list(src_cols)
                self._propagate_join_sources(var, src_var)

            else:
                # Unknown op: inherit if possible
                if src_var and src_var in self.df_sources:
                    self.df_sources[var] = src_table
                    self.df_columns[var] = list(src_cols)
                    self._propagate_join_sources(var, src_var)

        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        if not isinstance(node.value, ast.Call):
            self.generic_visit(node)
            return
        call = node.value

        # Handle standalone spark.sql("...") calls (e.g., CREATE VIEW)
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

        target_table = self._get_write_table(call)
        if target_table is None:
            self.generic_visit(node)
            return

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

        src_table, cols = self._resolve_source(src_var)
        join_sources = self._join_sources.get(src_var, [])

        for (col_name, transform_type, expr_str, src_cols, lineno) in cols:
            for sc in (src_cols or [col_name]):
                # For join_key columns, emit edges from all joined tables
                if transform_type == "join_key" and join_sources:
                    for jtable in join_sources:
                        self.edges.append(LineageEdge(
                            source_col=f"{jtable}.{sc}",
                            target_col=f"{target_table}.{col_name}",
                            transform_type=transform_type,
                            expression=expr_str,
                            source_file=self.source_file,
                            source_cell=None,
                            source_line=lineno,
                        ))
                else:
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


_DATABRICKS_HEADER = "# Databricks notebook source"
_COMMAND_SEP = "# COMMAND ----------"
_MAGIC_PREFIX = "# MAGIC "
_SQL_MAGICS = ("%sql", "%sql ")


def _parse_databricks_py(code: str, source_file: str) -> list[LineageEdge]:
    """Parse a Databricks-exported .py notebook with # COMMAND and # MAGIC markers."""
    edges: list[LineageEdge] = []
    temp_views: set[str] = set()
    cells = code.split(_COMMAND_SEP)

    for cell_idx, cell in enumerate(cells):
        lines = cell.strip().splitlines()
        if not lines:
            continue

        # Check if this cell uses %sql magic
        magic_lines = [l for l in lines if l.startswith(_MAGIC_PREFIX)]
        if magic_lines:
            # Extract content after # MAGIC prefix
            content_lines = []
            is_sql = False
            for line in magic_lines:
                content = line[len(_MAGIC_PREFIX):]
                if any(content.startswith(m) for m in _SQL_MAGICS):
                    is_sql = True
                    # Strip the %sql prefix from first line
                    content = content.lstrip()
                    for m in _SQL_MAGICS:
                        if content.startswith(m):
                            content = content[len(m):].strip()
                            break
                    if content:
                        content_lines.append(content)
                elif content.startswith("%"):
                    # Other magic (%python, %md, %run) — skip
                    break
                else:
                    content_lines.append(content)

            if is_sql and content_lines:
                sql = "\n".join(content_lines)
                temp_views.update(_detect_temp_views(sql))
                sql_edges = _parse_sql(
                    sql,
                    source_file=source_file,
                    source_line=None,
                    source_cell=cell_idx,
                    _resolve_views=False,
                )
                edges.extend(sql_edges)
        else:
            # Pure Python cell — strip any non-MAGIC comment-only preamble
            # (like the header line) and parse as PySpark
            py_lines = [l for l in lines if not l.startswith("# Databricks")]
            py_code = "\n".join(py_lines).strip()
            if py_code:
                cell_edges = parse_pyspark(
                    py_code, source_file=source_file,
                    source_cell=cell_idx, _resolve_views=False,
                )
                edges.extend(cell_edges)

    return _resolve_temp_views(edges, temp_views)


def parse_pyspark(
    code: str,
    source_file: str,
    source_cell: int | None = None,
    _resolve_views: bool = True,
) -> list[LineageEdge]:
    """Parse PySpark Python code and return column-level lineage edges.

    Automatically detects Databricks-exported .py notebooks
    (files starting with '# Databricks notebook source') and
    routes them through the Databricks notebook parser.
    """
    # Detect Databricks exported notebook format
    if code.lstrip().startswith(_DATABRICKS_HEADER) and source_cell is None:
        return _parse_databricks_py(code, source_file)

    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    tracker = _DataFrameTracker(source_file=source_file, resolve_views=False)
    tracker.visit(tree)

    if source_cell is not None:
        for edge in tracker.edges:
            edge.source_cell = source_cell

    return tracker.edges
