"""SQL column lineage parser using SQLGlot."""
from __future__ import annotations
import sqlglot
import sqlglot.expressions as exp
from lineage.models import LineageEdge


def _classify_transform(node: exp.Expression) -> tuple[str, str | None]:
    """Return (transform_type, expression_str) for a SELECT column expression."""
    try:
        expr_str = node.sql(dialect="databricks")
    except Exception:
        expr_str = str(node)

    # Walk node tree to classify
    all_nodes = list(node.walk())

    for n in all_nodes:
        if isinstance(n, exp.Window):
            return "window", expr_str
    for n in all_nodes:
        if isinstance(n, exp.Cast):
            return "cast", expr_str
    for n in all_nodes:
        if isinstance(n, (exp.Sum, exp.Count, exp.Avg, exp.Max, exp.Min,
                          exp.ArrayAgg, exp.GroupConcat)):
            return "aggregation", expr_str
    for n in all_nodes:
        if isinstance(n, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Case,
                          exp.If, exp.Coalesce)):
            return "expression", expr_str
    return "passthrough", expr_str


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
        # SQLGlot uses 'from_' as the arg key for FROM clause
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


def _qualified_table_name(table: exp.Table) -> str:
    """Return schema-qualified table name when schema is present."""
    name = table.name
    schema = table.db  # SQLGlot uses .db for the schema part
    catalog = table.catalog
    if catalog and schema:
        return f"{catalog}.{schema}.{name}"
    if schema:
        return f"{schema}.{name}"
    return name


def _is_temp_view(statement: exp.Expression) -> bool:
    """Check if a statement creates a temporary view."""
    if not isinstance(statement, exp.Create):
        return False
    kind = (statement.args.get("kind") or "").upper()
    if kind != "VIEW":
        return False
    props = statement.args.get("properties")
    if props and hasattr(props, 'expressions'):
        for prop in props.expressions:
            if isinstance(prop, exp.TemporaryProperty):
                return True
    return False


def _find_target_table(statement: exp.Expression) -> str:
    """Extract the target table name from INSERT INTO or CREATE TABLE AS SELECT."""
    if isinstance(statement, exp.Insert):
        if isinstance(statement.this, exp.Table):
            return _qualified_table_name(statement.this)
    elif isinstance(statement, exp.Create):
        if isinstance(statement.this, exp.Table):
            return _qualified_table_name(statement.this)
    return "result"


def _collect_union_selects(node: exp.Expression) -> list[exp.Select]:
    """Recursively collect all SELECT branches from a UNION/INTERSECT/EXCEPT tree."""
    if isinstance(node, exp.Select):
        return [node]
    if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
        return _collect_union_selects(node.this) + _collect_union_selects(node.expression)
    if isinstance(node, exp.Subquery):
        return _collect_union_selects(node.this)
    return []


def _get_statement_body(statement: exp.Expression) -> exp.Expression | None:
    """Return the query body (SELECT or UNION) stripping INSERT/CREATE wrapper."""
    if isinstance(statement, exp.Select):
        return statement
    if isinstance(statement, (exp.Union, exp.Intersect, exp.Except)):
        return statement
    if isinstance(statement, exp.Insert):
        return statement.args.get("expression")
    if isinstance(statement, exp.Create):
        return statement.args.get("expression")
    return None


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

    default_table = source_tables[0] if source_tables else "unknown"

    def _resolve_table_hint(hint: str) -> tuple[str, bool]:
        """Resolve a table alias/name. Returns (resolved_table, is_certain)."""
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


def _resolve_temp_views(
    edges: list[LineageEdge],
    temp_views: set[str],
) -> list[LineageEdge]:
    """Short-circuit temp views: connect their sources directly to their consumers.

    Given edges: src.col -> temp.col -> target.col
    Produces:    src.col -> target.col (with downstream transform/expression)
    Removes all edges to/from temp views.
    """
    if not temp_views:
        return edges

    # Build full map of temp_view.col -> ultimate non-temp-view sources
    # by iteratively resolving chains (step2 -> step1 -> source_table)
    tv_sources: dict[str, list[str]] = {}
    for e in edges:
        tbl = e.target_col.rsplit(".", 1)[0] if "." in e.target_col else ""
        if tbl in temp_views:
            tv_sources.setdefault(e.target_col, []).append(e.source_col)

    # Resolve chains: if a source in tv_sources is itself a temp view col, expand it
    max_iterations = len(temp_views) + 1
    for _ in range(max_iterations):
        changed = False
        for tv_col, sources in tv_sources.items():
            expanded: list[str] = []
            for src in sources:
                src_tbl = src.rsplit(".", 1)[0] if "." in src else ""
                if src_tbl in temp_views and src in tv_sources:
                    expanded.extend(tv_sources[src])
                    changed = True
                else:
                    expanded.append(src)
            tv_sources[tv_col] = expanded
        if not changed:
            break

    resolved: list[LineageEdge] = []
    for e in edges:
        src_tbl = e.source_col.rsplit(".", 1)[0] if "." in e.source_col else ""
        tgt_tbl = e.target_col.rsplit(".", 1)[0] if "." in e.target_col else ""

        if tgt_tbl in temp_views:
            continue

        if src_tbl in temp_views and e.source_col in tv_sources:
            for upstream_col in tv_sources[e.source_col]:
                resolved.append(LineageEdge(
                    source_col=upstream_col,
                    target_col=e.target_col,
                    transform_type=e.transform_type,
                    expression=e.expression,
                    source_file=e.source_file,
                    source_cell=e.source_cell,
                    source_line=e.source_line,
                    confidence=e.confidence,
                ))
        else:
            resolved.append(e)

    return resolved


def _detect_temp_views(sql_text: str) -> set[str]:
    """Parse SQL to find temp view names without extracting full lineage."""
    try:
        statements = sqlglot.parse(sql_text, dialect="databricks")
    except Exception:
        return set()
    names = set()
    for stmt in statements:
        if stmt and _is_temp_view(stmt):
            if isinstance(stmt.this, exp.Table):
                names.add(_qualified_table_name(stmt.this))
    return names


_DATABRICKS_SQL_HEADER = "-- Databricks notebook source"
_DATABRICKS_SQL_SEP = "-- COMMAND ----------"


def _split_databricks_sql(sql: str) -> list[tuple[str, int]]:
    """Split a Databricks-exported .sql notebook into (cell_sql, cell_index) pairs."""
    cells: list[tuple[str, int]] = []
    cell_idx = 0
    for chunk in sql.split(_DATABRICKS_SQL_SEP):
        # Strip the header comment and whitespace
        cleaned = chunk.strip()
        if cleaned == _DATABRICKS_SQL_HEADER.strip():
            cell_idx += 1
            continue
        # Remove leading header if present
        if cleaned.startswith(_DATABRICKS_SQL_HEADER):
            cleaned = cleaned[len(_DATABRICKS_SQL_HEADER):].strip()
        if cleaned and not cleaned.startswith("--") or "\n" in cleaned:
            # Filter out cells that are only comments (like %md magic)
            non_comment_lines = [
                l for l in cleaned.splitlines()
                if l.strip() and not l.strip().startswith("--")
            ]
            if non_comment_lines:
                cells.append((cleaned, cell_idx))
        cell_idx += 1
    return cells


def parse_sql(
    sql: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None = None,
    _resolve_views: bool = True,
) -> list[LineageEdge]:
    """Parse SQL (single or multi-statement) and return column-level lineage edges.

    Supports multiple statements separated by semicolons.
    Detects Databricks-exported .sql notebooks and splits on '-- COMMAND ----------'.
    Uses "result" as the synthetic target table name when no INTO/CREATE is present.
    Returns empty list on parse error (non-fatal).
    """
    # Detect Databricks SQL notebook format
    if _DATABRICKS_SQL_SEP in sql and source_cell is None:
        edges: list[LineageEdge] = []
        temp_views: set[str] = set()
        for cell_sql, cell_idx in _split_databricks_sql(sql):
            temp_views.update(_detect_temp_views(cell_sql))
            edges.extend(
                parse_sql(cell_sql, source_file, source_line=None,
                          source_cell=cell_idx, _resolve_views=False)
            )
        return _resolve_temp_views(edges, temp_views)

    try:
        statements = sqlglot.parse(sql, dialect="databricks")
    except Exception:
        return []

    temp_views: set[str] = set()
    edges: list[LineageEdge] = []
    for statement in statements:
        if statement is None:
            continue
        if _is_temp_view(statement) and isinstance(statement.this, exp.Table):
            temp_views.add(_qualified_table_name(statement.this))
        edges.extend(
            _parse_single_statement(statement, source_file, source_line, source_cell)
        )
    if _resolve_views and temp_views:
        return _resolve_temp_views(edges, temp_views)
    return edges
