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
        cte_body = cte.this
        # cte_body is a Select for simple CTEs, or a Union/Except/Intersect for set ops
        is_select = isinstance(cte_body, exp.Select)
        from_clause = cte_body.args.get("from_") if is_select else None
        has_join = bool(cte_body.args.get("joins")) if is_select else False

        if is_select and not has_join and from_clause:
            table_expr = from_clause.this
            if isinstance(table_expr, exp.Table):
                simple_map[alias] = _qualified_table_name(table_expr)
                continue

        # Multi-source: JOINed SELECT, UNION ALL/EXCEPT/INTERSECT, or FROM-subquery
        select_nodes = _collect_union_selects(cte_body)
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

    # PIVOT: emit synthetic aggregation edges sub_alias.agg_col → sub_alias.pivot_out_col
    # so that temp-view resolution chains source → aggregated col → pivot output.
    for pivot in (subq.args.get("pivots") or []):
        if not isinstance(pivot, exp.Pivot):
            continue
        agg_cols: list[str] = []
        for agg_fn in (pivot.args.get("expressions") or []):
            for col in agg_fn.find_all(exp.Column):
                if col.name:
                    agg_cols.append(col.name)
        out_cols = [c.name for c in (pivot.args.get("columns") or []) if c.name]
        for out_col in out_cols:
            for agg_col in agg_cols:
                edges.append(LineageEdge(
                    source_col=f"{sub_alias}.{agg_col}",
                    target_col=f"{sub_alias}.{out_col}",
                    transform_type="aggregation",
                    expression=pivot.sql(dialect="databricks"),
                    source_file=source_file,
                    source_cell=source_cell,
                    source_line=source_line,
                    confidence="certain",
                    qualified=True,
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

    join_on_exprs: list[exp.Expression] = []
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
        on_expr = join.args.get("on")
        if on_expr is not None:
            join_on_exprs.append(on_expr)

    default_table = source_tables[0] if source_tables else "unknown"
    multi_source = len(source_tables) > 1

    # LATERAL VIEW EXPLODE(t.arr) e AS item  → treat `e.item` as a passthrough of
    # the array source column (t.arr). POSEXPLODE yields (pos, value); the value
    # slot traces to the array, the pos slot is synthetic (still linked for reference).
    # Map: "alias.col_name" → list[source_col_id]
    lateral_expand: dict[str, list[str]] = {}
    for lat in (select_node.args.get("laterals") or []):
        lat_alias_node = lat.args.get("alias")
        if not isinstance(lat_alias_node, exp.TableAlias):
            continue
        lat_alias = lat_alias_node.this.name if lat_alias_node.this else None
        if not lat_alias:
            continue
        lat_cols = [c.name for c in (lat_alias_node.columns or []) if c.name]
        lat_fn = lat.this
        if not isinstance(lat_fn, exp.Expression):
            continue
        src_col_refs = list(lat_fn.find_all(exp.Column))
        src_col_ids: list[str] = []
        for cref in src_col_refs:
            hint = cref.table
            name = cref.name
            if not name:
                continue
            if hint and hint in alias_map:
                tbl = alias_map[hint]
            elif hint:
                tbl = hint
            else:
                tbl = default_table
            src_col_ids.append(f"{tbl}.{name}")
        if not src_col_ids:
            continue
        # All output cols (both value and POSEXPLODE pos) trace to the array source.
        # We keep the position slot linked for traceability — consumers can filter
        # on transform_type if they want to suppress it.
        for out_col in lat_cols:
            lateral_expand[f"{lat_alias}.{out_col}"] = list(src_col_ids)

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

    # JOIN ... ON → __joinkey__ edges. Emit one edge per distinct column in
    # each ON predicate so join keys are visible but do not pollute normal
    # column lineage.
    for on_expr in join_on_exprs:
        on_expr_str = on_expr.sql(dialect="databricks")
        seen_jk_src: set[str] = set()
        for col_ref in on_expr.find_all(exp.Column):
            col_name = col_ref.name
            if not col_name:
                continue
            table_hint = col_ref.table
            if table_hint:
                resolved_table, _certain = _resolve_table_hint(table_hint)
                qualified = True
            else:
                resolved_table = default_table
                qualified = not multi_source
            src = f"{resolved_table}.{col_name}"
            if src in seen_jk_src:
                continue
            seen_jk_src.add(src)
            edges.append(LineageEdge(
                source_col=src,
                target_col=f"{target_table}.__joinkey__",
                transform_type="join_key",
                expression=on_expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain" if qualified else "approximate",
                qualified=qualified,
            ))

    # WHERE → __filter__ edges. Emit one edge per distinct column reference
    # in the predicate to target.__filter__ so consumers can see which columns
    # gated the row selection (predicate lineage, not column lineage).
    where_clause = select_node.args.get("where")
    if where_clause is not None:
        filter_expr_str = where_clause.sql(dialect="databricks")
        seen_filter_src: set[str] = set()
        for col_ref in where_clause.find_all(exp.Column):
            col_name = col_ref.name
            if not col_name:
                continue
            table_hint = col_ref.table
            if table_hint:
                resolved_table, _certain = _resolve_table_hint(table_hint)
                qualified = True
            else:
                resolved_table = default_table
                qualified = not multi_source
            src = f"{resolved_table}.{col_name}"
            if src in seen_filter_src:
                continue
            seen_filter_src.add(src)
            edges.append(LineageEdge(
                source_col=src,
                target_col=f"{target_table}.__filter__",
                transform_type="filter",
                expression=filter_expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain" if qualified else "approximate",
                qualified=qualified,
            ))

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
            # Pure literal expressions (NULL, 'string', 42, TRUE) have no real source
            # column. Attributing them to default_table creates phantom source entries
            # for CTE aliases when the CTE body doesn't define that column.
            if isinstance(expr_node, (exp.Null, exp.Literal, exp.Boolean)):
                continue
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
            # Lateral view alias expansion: e.item → underlying array source(s)
            if table_hint and f"{table_hint}.{col_name}" in lateral_expand:
                for src_id in lateral_expand[f"{table_hint}.{col_name}"]:
                    edges.append(LineageEdge(
                        source_col=src_id,
                        target_col=target_col,
                        transform_type=transform_type,
                        expression=expr_str,
                        source_file=source_file,
                        source_cell=source_cell,
                        source_line=source_line,
                        confidence="certain",
                        qualified=True,
                    ))
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


def _parse_merge(
    merge: exp.Merge,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    """Parse a MERGE INTO statement into column-level lineage edges.

    Handles both WHEN MATCHED THEN UPDATE and WHEN NOT MATCHED THEN INSERT branches.
    Ignores the ON predicate columns (those belong to __joinkey__ lineage, not data flow).
    """
    target_node = merge.this
    using_node = merge.args.get("using")
    if not isinstance(target_node, exp.Table):
        return []
    target_table = _qualified_table_name(target_node)
    target_alias = target_node.alias or target_node.name

    alias_map: dict[str, str] = {target_alias: target_table}
    source_tables: list[str] = [target_table]
    if isinstance(using_node, exp.Table):
        using_qualified = _qualified_table_name(using_node)
        using_alias = using_node.alias or using_node.name
        alias_map[using_alias] = using_qualified
        source_tables.append(using_qualified)

    default_table = source_tables[-1] if len(source_tables) > 1 else target_table

    def _resolve(hint: str) -> tuple[str, bool]:
        if hint in alias_map:
            return alias_map[hint], True
        for tbl in source_tables:
            if tbl == hint or tbl.endswith(f".{hint}"):
                return tbl, True
        return default_table, False

    def _col_name(col: exp.Column) -> tuple[str, str | None]:
        """Return (column_name, source_table_or_None). None when resolution can't pick a side."""
        name = col.name
        hint = col.table
        if not hint:
            return name, None
        resolved, _ = _resolve(hint)
        return name, resolved

    def _make_edge(
        source_col: str, target_col: str, transform_type: str, expr_str: str,
    ) -> LineageEdge:
        return LineageEdge(
            source_col=source_col,
            target_col=target_col,
            transform_type=transform_type,
            expression=expr_str,
            source_file=source_file,
            source_cell=source_cell,
            source_line=source_line,
            confidence="certain",
            qualified=True,
        )

    edges: list[LineageEdge] = []
    whens = merge.args.get("whens")
    if whens is None:
        return edges

    for when in whens.expressions:
        then = when.args.get("then")
        if isinstance(then, exp.Update):
            for eq in then.args.get("expressions") or []:
                if not isinstance(eq, exp.EQ):
                    continue
                lhs = eq.this
                rhs = eq.expression
                if not isinstance(lhs, exp.Column):
                    continue
                tgt_name, tgt_tbl = _col_name(lhs)
                target_col = f"{tgt_tbl or target_table}.{tgt_name}"
                transform_type, expr_str = _classify_transform(rhs)
                col_refs = list(rhs.find_all(exp.Column)) if isinstance(rhs, exp.Expression) else []
                if not col_refs and isinstance(rhs, exp.Column):
                    col_refs = [rhs]
                if col_refs:
                    for cref in col_refs:
                        src_name, src_tbl = _col_name(cref)
                        edges.append(_make_edge(
                            source_col=f"{src_tbl or default_table}.{src_name}",
                            target_col=target_col,
                            transform_type=transform_type,
                            expr_str=expr_str,
                        ))
        elif isinstance(then, exp.Insert):
            cols_tuple = then.this
            vals_expr = then.expression
            tgt_cols: list[exp.Column] = []
            if isinstance(cols_tuple, exp.Tuple):
                tgt_cols = [c for c in cols_tuple.expressions if isinstance(c, exp.Column)]
            val_items: list[exp.Expression] = []
            if isinstance(vals_expr, exp.Values):
                first = vals_expr.expressions[0] if vals_expr.expressions else None
                if isinstance(first, exp.Tuple):
                    val_items = list(first.expressions)
            elif isinstance(vals_expr, exp.Tuple):
                val_items = list(vals_expr.expressions)
            for tgt_col, val in zip(tgt_cols, val_items):
                tgt_name = tgt_col.name
                target_col = f"{target_table}.{tgt_name}"
                transform_type, expr_str = _classify_transform(val)
                col_refs = list(val.find_all(exp.Column)) if isinstance(val, exp.Expression) else []
                if not col_refs and isinstance(val, exp.Column):
                    col_refs = [val]
                if col_refs:
                    for cref in col_refs:
                        src_name, src_tbl = _col_name(cref)
                        edges.append(_make_edge(
                            source_col=f"{src_tbl or default_table}.{src_name}",
                            target_col=target_col,
                            transform_type=transform_type,
                            expr_str=expr_str,
                        ))
    return edges


def _parse_single_statement(
    statement: exp.Expression,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> list[LineageEdge]:
    """Parse a single SQL statement (handles UNION/UNION ALL) and return lineage edges."""
    if isinstance(statement, exp.Merge):
        return _parse_merge(statement, source_file, source_line, source_cell)
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

    # Normalize temp_views to lowercase — Databricks identifiers are case-insensitive
    # and SQL notebooks frequently mix cases across cells (CREATE VIEW uses lowercase
    # SQLGlot normalization; SELECT statements may reference columns in UPPERCASE).
    temp_views_lower = {v.lower() for v in temp_views}

    # Build full map of temp_view.col -> upstream sources (all keys lowercased)
    tv_sources: dict[str, list[str]] = {}
    for e in edges:
        tbl = e.target_col.lower().rsplit(".", 1)[0] if "." in e.target_col else ""
        if tbl in temp_views_lower:
            key = e.target_col.lower()
            tv_sources.setdefault(key, []).append(e.source_col.lower())

    def _lookup(src: str) -> list[str] | None:
        """Return upstream sources for a temp view column with wildcard fallback.

        - Exact match: return tv_sources[src].
        - Wildcard source (tbl.*): collect sources from ALL entries for that table.
        - Named source not found: fall back to the tbl.* wildcard entry.
        Returns None when no upstream can be found.
        """
        src = src.lower()
        if src in tv_sources:
            return tv_sources[src]
        src_tbl = src.rsplit(".", 1)[0] if "." in src else ""
        if src_tbl not in temp_views_lower:
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
    max_iterations = len(temp_views_lower) + 1
    for _ in range(max_iterations):
        changed = False
        for tv_col, sources in tv_sources.items():
            expanded: list[str] = []
            for src in sources:
                src_tbl = src.rsplit(".", 1)[0] if "." in src else ""
                if src_tbl in temp_views_lower:
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
        src_tbl = e.source_col.lower().rsplit(".", 1)[0] if "." in e.source_col else ""
        tgt_tbl = e.target_col.lower().rsplit(".", 1)[0] if "." in e.target_col else ""

        if tgt_tbl in temp_views_lower:
            continue

        if src_tbl in temp_views_lower:
            # Wildcard source → wildcard target: emit per-column edges using tv_sources.
            # e.g. SRC_MATCH_RNK.* → ship_ordr_mlstn.* becomes
            #      upstream_of(SRC_MATCH_RNK.col) → ship_ordr_mlstn.col for each named col.
            if e.source_col.endswith(".*") and e.target_col.endswith(".*"):
                tgt_base = e.target_col[:-2]  # strip ".*"
                seen_per_col: set[tuple[str, str]] = set()
                for tv_key, tv_vals in tv_sources.items():
                    tv_key_tbl = tv_key.rsplit(".", 1)[0] if "." in tv_key else ""
                    if tv_key_tbl == src_tbl.lower() and not tv_key.endswith(".*"):
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
