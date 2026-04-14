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
                cte_map[alias] = table_expr.name
    return cte_map


def _find_select(statement: exp.Expression) -> exp.Select | None:
    """Extract the innermost SELECT from any statement type."""
    if isinstance(statement, exp.Select):
        return statement
    return statement.find(exp.Select)


def _find_target_table(statement: exp.Expression) -> str:
    """Extract the target table name from INSERT INTO or CREATE TABLE AS SELECT."""
    if isinstance(statement, exp.Insert):
        if isinstance(statement.this, exp.Table):
            return statement.this.name
    elif isinstance(statement, exp.Create):
        if isinstance(statement.this, exp.Table):
            return statement.this.name
    return "result"


def parse_sql(
    sql: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None = None,
) -> list[LineageEdge]:
    """Parse a SQL statement and return column-level lineage edges.

    Uses "result" as the synthetic target table name when no INTO/CREATE is present.
    Returns empty list on parse error (non-fatal).
    """
    try:
        statement = sqlglot.parse_one(sql, dialect="databricks")
    except Exception:
        return []

    if statement is None:
        return []

    select_node = _find_select(statement)
    if select_node is None:
        return []

    cte_map = _resolve_ctes(statement)
    target_table = _find_target_table(statement)
    edges: list[LineageEdge] = []

    # Collect source tables from FROM + JOINs
    # SQLGlot uses 'from_' as the arg key for FROM clause
    from_clause = select_node.args.get("from_")
    if from_clause is None:
        return []

    source_tables: list[str] = []
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        tname = from_table.alias_or_name
        source_tables.append(cte_map.get(tname, tname))
    elif isinstance(from_table, exp.Subquery):
        source_tables.append("subquery")

    for join in select_node.find_all(exp.Join):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            tname = jtable.alias_or_name
            source_tables.append(cte_map.get(tname, tname))

    default_table = source_tables[0] if source_tables else "unknown"

    # Walk SELECT expressions
    for sel in select_node.selects:
        # Determine output column alias
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

        # For window functions, extract actual column refs from the window expression
        # (PARTITION BY / ORDER BY columns) to avoid a self-loop on the output alias.
        if transform_type == "window":
            win_col_refs = list(expr_node.find_all(exp.Column))
            if win_col_refs:
                for col_ref in win_col_refs:
                    table_hint = col_ref.table
                    col_name = col_ref.name
                    if not col_name:
                        continue
                    resolved_table = cte_map.get(table_hint, table_hint) if table_hint else default_table
                    edges.append(LineageEdge(
                        source_col=f"{resolved_table}.{col_name}",
                        target_col=target_col,
                        transform_type=transform_type,
                        expression=expr_str,
                        source_file=source_file,
                        source_cell=source_cell,
                        source_line=source_line,
                    ))
            else:
                # No column refs found — fall back to default_table with a generic marker
                edges.append(LineageEdge(
                    source_col=f"{default_table}.*",
                    target_col=target_col,
                    transform_type=transform_type,
                    expression=expr_str,
                    source_file=source_file,
                    source_cell=source_cell,
                    source_line=source_line,
                ))
            continue

        # Find all Column references inside this expression (excluding OVER clause)
        col_refs = list(expr_node.find_all(exp.Column))

        if not col_refs:
            # No column refs (constant, function with no col args) — create edge to unknown
            edges.append(LineageEdge(
                source_col=f"{default_table}.{alias}",
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
            ))
            continue

        for col_ref in col_refs:
            table_hint = col_ref.table
            col_name = col_ref.name
            if not col_name:
                continue
            if table_hint:
                resolved_table = cte_map.get(table_hint, table_hint)
            else:
                resolved_table = default_table
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
