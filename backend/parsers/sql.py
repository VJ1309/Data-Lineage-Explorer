"""SQL column lineage parser using SQLGlot."""
from __future__ import annotations
import sqlglot
import sqlglot.expressions as exp
from sqlglot import tokens
from lineage.models import LineageEdge


def _split_top_level_statements(sql: str) -> list[str]:
    """Split SQL on top-level ';' using SQLGlot's tokenizer (respects strings/comments).

    Returns the original string unchanged if tokenisation fails, so a single
    malformed statement still goes to the per-statement parse path and surfaces
    a proper parse error instead of being dropped silently.
    """
    try:
        toks = list(tokens.Tokenizer().tokenize(sql))
    except Exception:
        return [sql] if sql.strip() else []
    parts: list[str] = []
    start = 0
    for tok in toks:
        if tok.token_type == tokens.TokenType.SEMICOLON:
            # tok.start/end are inclusive char offsets
            chunk = sql[start:tok.start]
            if chunk.strip():
                parts.append(chunk)
            start = tok.end + 1
    tail = sql[start:]
    if tail.strip():
        parts.append(tail)
    return parts


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


def _resolve_ctes(
    statement: exp.Expression,
) -> tuple[dict[str, str], dict[str, list[exp.Select]]]:
    """Build CTE resolution maps.

    Returns:
        simple_map  – alias → single physical source table (single-FROM CTEs)
        multi_map   – alias → [Select nodes] for CTEs that contain JOINs

    Chains in simple_map are resolved (cte2 → cte1 → actual_table).
    Multi-source CTEs are returned as parsed Select bodies so the caller can
    treat them like inline subqueries via _resolve_temp_views.
    """
    simple_map: dict[str, str] = {}
    multi_map: dict[str, list[exp.Select]] = {}

    with_clause = statement.args.get("with_")
    if not with_clause:
        # For CREATE TABLE/VIEW AS WITH ... SELECT ..., SQLGlot attaches the
        # WITH clause to the inner SELECT body, not the outer wrapper.
        body = statement.args.get("expression")
        if body is not None:
            with_clause = body.args.get("with_")
    if not with_clause:
        return simple_map, multi_map

    for cte in with_clause.expressions:
        alias = cte.alias
        cte_select = cte.this
        from_clause = cte_select.args.get("from_")
        has_join = bool(cte_select.args.get("joins"))

        if not has_join and from_clause:
            table_expr = from_clause.this
            if isinstance(table_expr, exp.Table):
                simple_map[alias] = _qualified_table_name(table_expr)
        elif has_join:
            select_nodes = _collect_union_selects(cte_select)
            if select_nodes:
                multi_map[alias] = select_nodes

    # Resolve chains: cte2 -> cte1 -> actual_table
    max_iterations = len(simple_map) + 1
    for _ in range(max_iterations):
        changed = False
        for alias, target in list(simple_map.items()):
            if target in simple_map:
                simple_map[alias] = simple_map[target]
                changed = True
        if not changed:
            break

    return simple_map, multi_map


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


_subquery_counter: list[int] = [0]  # mutable container for module-level counter


def _next_sub_alias() -> str:
    _subquery_counter[0] += 1
    return f"__sub_{_subquery_counter[0]}__"


def _process_subquery(
    subq: exp.Subquery,
    edges: list[LineageEdge],
    source_tables: list[str],
    cte_map: dict[str, str],
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
    subquery_aliases: set[str] | None = None,
) -> None:
    """Recursively parse a subquery and register its alias as a source table."""
    sub_alias = subq.alias or _next_sub_alias()
    source_tables.append(sub_alias)
    if subquery_aliases is not None:
        subquery_aliases.add(sub_alias)
    sub_selects = _collect_union_selects(subq.this) if subq.this else []
    for sub_sel in sub_selects:
        edges.extend(_parse_select_node(
            sub_sel, sub_alias, cte_map, source_file, source_line, source_cell,
            subquery_aliases=subquery_aliases,
        ))


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
    subquery_aliases: set[str] | None = None,
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
        _process_subquery(from_table, edges, source_tables, cte_map,
                          source_file, source_line, source_cell,
                          subquery_aliases=subquery_aliases)

    for join in (select_node.args.get("joins") or []):
        jtable = join.this
        if isinstance(jtable, exp.Table):
            qualified = _qualified_table_name(jtable)
            alias = jtable.alias
            resolved = cte_map.get(jtable.name, qualified)
            if alias:
                alias_map[alias] = resolved
            source_tables.append(resolved)
        elif isinstance(jtable, exp.Subquery):
            _process_subquery(jtable, edges, source_tables, cte_map,
                              source_file, source_line, source_cell,
                              subquery_aliases=subquery_aliases)

    default_table = source_tables[0] if source_tables else "unknown"
    multi_source = len(source_tables) > 1

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
                        qualified = True
                    else:
                        resolved_table, certain = default_table, True
                        qualified = not multi_source
                    edges.append(LineageEdge(
                        source_col=f"{resolved_table}.{col_name}",
                        target_col=target_col,
                        transform_type=transform_type,
                        expression=expr_str,
                        source_file=source_file,
                        source_cell=source_cell,
                        source_line=source_line,
                        confidence="certain" if certain and qualified else "approximate",
                        qualified=qualified,
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
                qualified = True
            else:
                resolved_table, certain = default_table, True
                qualified = not multi_source
            source_col = f"{resolved_table}.{col_name}"
            edges.append(LineageEdge(
                source_col=source_col,
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain" if certain and qualified else "approximate",
                qualified=qualified,
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

    cte_map, multi_cte_bodies = _resolve_ctes(statement)
    target_table = _find_target_table(statement)
    subquery_aliases: set[str] = set()
    edges: list[LineageEdge] = []

    # Parse multi-source (JOINed) CTEs as virtual subqueries so _resolve_temp_views
    # can short-circuit them just like inline subqueries / temp views.
    for cte_alias, cte_selects in multi_cte_bodies.items():
        subquery_aliases.add(cte_alias)
        for cte_sel in cte_selects:
            edges.extend(_parse_select_node(
                cte_sel, cte_alias, cte_map,
                source_file, source_line, source_cell,
                subquery_aliases=subquery_aliases,
            ))

    for select_node in select_nodes:
        edges.extend(_parse_select_node(
            select_node, target_table, cte_map,
            source_file, source_line, source_cell,
            subquery_aliases=subquery_aliases,
        ))
    if subquery_aliases:
        edges = _resolve_temp_views(edges, subquery_aliases)
    return edges


def _resolve_temp_views(
    edges: list[LineageEdge],
    temp_views: set[str],
) -> list[LineageEdge]:
    """Short-circuit temp views: connect their sources directly to their consumers.

    Given edges: src.col -> temp.col -> target.col
    Produces:    src.col -> target.col (with downstream transform/expression)
    Removes all edges to/from temp views.

    Handles two wildcard cases:
    - Wildcard source (temp.* -> target.*): expands by collecting all sources
      from all column-level entries for that temp view table.
    - Named source not found (temp.col with no entry): falls back to the
      temp.* wildcard entry when only a SELECT * edge was recorded.
    """
    if not temp_views:
        return edges

    # Build full map of temp_view.col -> upstream sources
    tv_sources: dict[str, list[str]] = {}
    for e in edges:
        tbl = e.target_col.rsplit(".", 1)[0] if "." in e.target_col else ""
        if tbl in temp_views:
            tv_sources.setdefault(e.target_col, []).append(e.source_col)

    def _lookup(src: str) -> list[str] | None:
        """Return upstream sources for a temp view column with wildcard fallback.

        - Exact match: return tv_sources[src].
        - Wildcard source (tbl.*): collect sources from ALL entries for that table.
        - Named source not found: fall back to the tbl.* wildcard entry.
        Returns None when no upstream can be found.
        """
        if src in tv_sources:
            return tv_sources[src]
        src_tbl = src.rsplit(".", 1)[0] if "." in src else ""
        if src_tbl not in temp_views:
            return None
        if src.endswith(".*"):
            # Gather all sources recorded for any column of this temp view
            seen: set[str] = set()
            result: list[str] = []
            for key, values in tv_sources.items():
                if key.rsplit(".", 1)[0] == src_tbl:
                    for v in values:
                        if v not in seen:
                            seen.add(v)
                            result.append(v)
            return result if result else None
        # Named column — fall back to wildcard entry but substitute the column name.
        # e.g. lookup("FULL_SRC_RNK.SRC_SYS_CD") w/ wildcard → ["real_tbl.SRC_SYS_CD"]
        wildcard_sources = tv_sources.get(f"{src_tbl}.*")
        if wildcard_sources is None:
            return None
        col_name = src.rsplit(".", 1)[-1]
        return [
            (s[:-1] + col_name if s.endswith(".*") else s)
            for s in wildcard_sources
        ]

    # Resolve chains iteratively until stable
    max_iterations = len(temp_views) + 1
    for _ in range(max_iterations):
        changed = False
        for tv_col, sources in tv_sources.items():
            expanded: list[str] = []
            for src in sources:
                src_tbl = src.rsplit(".", 1)[0] if "." in src else ""
                if src_tbl in temp_views:
                    upstream = _lookup(src)
                    if upstream is not None:
                        expanded.extend(upstream)
                        changed = True
                    else:
                        expanded.append(src)
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

        if src_tbl in temp_views:
            # Wildcard source → wildcard target: emit per-column edges using tv_sources.
            # e.g. SRC_MATCH_RNK.* → ship_ordr_mlstn.* becomes
            #      upstream_of(SRC_MATCH_RNK.col) → ship_ordr_mlstn.col for each named col.
            if e.source_col.endswith(".*") and e.target_col.endswith(".*"):
                tgt_base = e.target_col[:-2]  # strip ".*"
                seen_per_col: set[tuple[str, str]] = set()
                for tv_key, tv_vals in tv_sources.items():
                    tv_key_tbl = tv_key.rsplit(".", 1)[0] if "." in tv_key else ""
                    if tv_key_tbl == src_tbl and not tv_key.endswith(".*"):
                        col_name = tv_key.rsplit(".", 1)[-1]
                        tgt_col = f"{tgt_base}.{col_name}"
                        for upcol in tv_vals:
                            key = (upcol, tgt_col)
                            if key not in seen_per_col:
                                seen_per_col.add(key)
                                resolved.append(LineageEdge(
                                    source_col=upcol,
                                    target_col=tgt_col,
                                    transform_type=e.transform_type,
                                    expression=e.expression,
                                    source_file=e.source_file,
                                    source_cell=e.source_cell,
                                    source_line=e.source_line,
                                    confidence=e.confidence,
                                ))
            # Always also resolve the wildcard edge itself (preserves the * row in UI).
            upstream_cols = _lookup(e.source_col)
            if upstream_cols:
                seen_up: set[str] = set()
                for upstream_col in upstream_cols:
                    if upstream_col not in seen_up:
                        seen_up.add(upstream_col)
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
        if (cleaned and not cleaned.startswith("--")) or "\n" in cleaned:
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
    _warnings: list[str] | None = None,
    _raw_out: list[LineageEdge] | None = None,
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
                          source_cell=cell_idx, _resolve_views=False,
                          _warnings=_warnings)
            )
        if _raw_out is not None:
            _raw_out.extend(edges)
        return _resolve_temp_views(edges, temp_views)

    statements: list[exp.Expression] = []
    for stmt_sql in _split_top_level_statements(sql):
        try:
            parsed = sqlglot.parse_one(stmt_sql, dialect="databricks")
        except Exception as exc:
            if _warnings is not None:
                preview = stmt_sql.strip().splitlines()[0][:80]
                _warnings.append(f"{exc} (near: {preview!r})")
            continue
        if parsed is not None:
            statements.append(parsed)

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
    if _raw_out is not None:
        _raw_out.extend(edges)
    if _resolve_views and temp_views:
        return _resolve_temp_views(edges, temp_views)
    return edges
