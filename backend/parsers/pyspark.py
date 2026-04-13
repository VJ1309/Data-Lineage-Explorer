"""PySpark column lineage parser using Python ast."""
from __future__ import annotations
import ast
from lineage.models import LineageEdge


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
    def __init__(self, source_file: str):
        self.source_file = source_file
        self.df_sources: dict[str, str] = {}   # var -> source table
        self.df_columns: dict[str, list[tuple]] = {}  # var -> [(col, transform, expr, src_cols, lineno)]
        self.edges: list[LineageEdge] = []

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

        # Handle method chains: check outermost call
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
            attr = value.func.attr
            src_var = _chase_df_var(value.func.value)
            src_table, src_cols = self._resolve_source(src_var)

            if attr == "select":
                cols = []
                for arg in value.args:
                    cnames = _extract_col_names(arg)
                    for cname in cnames:
                        cols.append((cname, "passthrough", cname, [cname], node.lineno))
                self.df_sources[var] = src_table
                self.df_columns[var] = cols

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

            elif attr in ("filter", "where", "dropDuplicates", "drop", "limit",
                          "orderBy", "sort", "distinct", "repartition", "cache"):
                # Pass-through operations: inherit parent df
                self.df_sources[var] = src_table
                self.df_columns[var] = list(src_cols)

            else:
                # Unknown op: inherit if possible
                if src_var and src_var in self.df_sources:
                    self.df_sources[var] = src_table
                    self.df_columns[var] = list(src_cols)

        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        if not isinstance(node.value, ast.Call):
            self.generic_visit(node)
            return
        call = node.value
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

        for (col_name, transform_type, expr_str, src_cols, lineno) in cols:
            for sc in (src_cols or [col_name]):
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

    if source_cell is not None:
        for edge in tracker.edges:
            object.__setattr__(edge, "source_cell", source_cell)

    return tracker.edges
