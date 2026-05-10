"""Lineage engine: builds and queries a NetworkX DAG from FileRecords."""
from __future__ import annotations
import os
from typing import Literal
import networkx as nx
from lineage.ids import split_column_id
from lineage.models import (
    ColumnMeta,
    FileRecord,
    GraphResult,
    LineageEdge,
    ParseResult,
    ParseWarning,
    TraceStep,
    TraceStepJoin,
    TraceStepPredicate,
    TraceStepWrite,
)
from parsers.sql import (
    parse_sql,
    DATABRICKS_SQL_SEP,
    split_databricks_sql,
    detect_temp_views,
    resolve_temp_views,
)
from parsers.pyspark import parse_pyspark
from parsers.notebook import parse_notebook


def _parse_file(
    record: FileRecord,
) -> tuple[list[LineageEdge], list[LineageEdge], list[ParseWarning]]:
    result: ParseResult = ParseResult()
    warnings: list[ParseWarning] = []
    try:
        if record.type == "notebook":
            result = parse_notebook(record.content, source_file=record.path)
        elif record.type == "python":
            result = parse_pyspark(record.content, source_file=record.path)
        elif record.type == "sql":
            if DATABRICKS_SQL_SEP in record.content:
                cell_edges: list[LineageEdge] = []
                cell_raw: list[LineageEdge] = []
                cell_warnings: list[str] = []
                temp_views: set[str] = set()
                for cell_sql, cell_idx in split_databricks_sql(record.content):
                    temp_views.update(detect_temp_views(cell_sql))
                    r = parse_sql(
                        cell_sql, source_file=record.path, source_line=None,
                        source_cell=cell_idx, _resolve_views=False,
                    )
                    cell_edges.extend(r.edges)
                    cell_raw.extend(r.raw_edges)
                    cell_warnings.extend(r.warnings)
                result = ParseResult(
                    edges=resolve_temp_views(cell_edges, temp_views),
                    raw_edges=cell_raw,
                    warnings=cell_warnings,
                )
            else:
                result = parse_sql(record.content, source_file=record.path, source_line=1)
        else:
            warnings.append(ParseWarning(
                file=record.path,
                error=f"Unknown file type: {record.type!r}",
            ))
    except Exception as exc:
        warnings.append(ParseWarning(file=record.path, error=str(exc)))
    for err in result.warnings:
        warnings.append(ParseWarning(file=record.path, error=f"SQL parse error: {err}"))
    return result.edges, result.raw_edges, warnings


def _normalize_edges(
    edges: list[LineageEdge],
) -> tuple[list[LineageEdge], list[str]]:
    """Normalize edge identifiers: lowercase + resolve short table names to full form.

    In Databricks/Unity Catalog, the same table can be referenced as:
      - catalog.schema.table  (full 3-part name)
      - schema.table          (2-part, when catalog is implicit)
      - table                 (1-part, when both are implicit)
    This merges them to the longest known form ONLY when the short name
    maps unambiguously to a single qualified form. If a short name could
    refer to two different fully-qualified tables (e.g. both
    staging.orders and prod.orders), it is left as-is and the ambiguous
    name is returned so the caller can surface a warning.

    Returns (normalized_edges, ambiguous_names).
    """
    # Step 1: Collect all unique table names (lowercased)
    table_names: set[str] = set()
    for e in edges:
        src = e.source_col.lower()
        tgt = e.target_col.lower()
        if "." in src:
            table_names.add(split_column_id(src)[0])
        if "." in tgt:
            table_names.add(split_column_id(tgt)[0])

    # Step 2: Build a mapping from short names to their longest matching form.
    # Only merge when there is exactly one candidate; otherwise record as ambiguous.
    short_to_long: dict[str, str] = {}
    ambiguous: set[str] = set()
    sorted_names = sorted(table_names, key=lambda n: n.count("."), reverse=True)
    for name in sorted_names:
        candidates = [
            longer for longer in sorted_names
            if longer != name and longer.endswith("." + name)
        ]
        if len(candidates) == 1:
            cand = candidates[0]
            short_to_long[name] = short_to_long.get(cand, cand)
        elif len(candidates) > 1:
            ambiguous.add(name)

    ambiguous_list = sorted(ambiguous)

    if not short_to_long:
        # Only case normalization needed
        normalized = []
        for e in edges:
            normalized.append(LineageEdge(
                source_col=e.source_col.lower(),
                target_col=e.target_col.lower(),
                transform_type=e.transform_type,
                expression=e.expression,
                source_file=e.source_file,
                source_cell=e.source_cell,
                source_line=e.source_line,
                confidence=e.confidence,
                qualified=e.qualified,
            ))
        return normalized, ambiguous_list

    def _resolve_col(col_id: str) -> str:
        col_lower = col_id.lower()
        if "." not in col_lower:
            return col_lower
        table, col = split_column_id(col_lower)
        resolved = short_to_long.get(table, table)
        return f"{resolved}.{col}"

    normalized = []
    for e in edges:
        normalized.append(LineageEdge(
            source_col=_resolve_col(e.source_col),
            target_col=_resolve_col(e.target_col),
            transform_type=e.transform_type,
            expression=e.expression,
            source_file=e.source_file,
            source_cell=e.source_cell,
            source_line=e.source_line,
            confidence=e.confidence,
            qualified=e.qualified,
        ))
    return normalized, ambiguous_list


def _compute_file_stats(
    graph: nx.DiGraph, warnings: list[ParseWarning]
) -> tuple[dict[str, dict], set[str]]:
    """Per-file edge/approximate/warning counts, plus the set of files with error severity.

    Iterates the deduped DAG (graph.edges(data=True)) — not the raw pre-graph edge list —
    so endpoint-pair duplicates collapse to a single edge_count increment. Must run after
    any synthetic warnings (e.g., cycle detection) have been appended so file_stats
    captures their warning_count.
    """
    file_stats: dict[str, dict] = {}
    for _, _, d in graph.edges(data=True):
        edge_data = d.get("data")
        if edge_data and edge_data.source_file:
            fname = edge_data.source_file
            if fname not in file_stats:
                file_stats[fname] = {"edge_count": 0, "approximate_count": 0, "warning_count": 0}
            file_stats[fname]["edge_count"] += 1
            if edge_data.confidence == "approximate":
                file_stats[fname]["approximate_count"] += 1

    error_files: set[str] = set()
    for w in warnings:
        if w.file:
            if w.file not in file_stats:
                file_stats[w.file] = {"edge_count": 0, "approximate_count": 0, "warning_count": 0}
            file_stats[w.file]["warning_count"] += 1
            if w.severity == "error":
                error_files.add(w.file)

    return file_stats, error_files


def build_graph_with_warnings(records: list[FileRecord]) -> GraphResult:
    """Parse all FileRecords and return a GraphResult bundling both DAGs, warnings, and per-file stats."""
    graph: nx.DiGraph = nx.DiGraph()
    all_warnings: list[ParseWarning] = []

    all_edges: list[LineageEdge] = []
    all_raw_edges: list[LineageEdge] = []
    for record in records:
        edges, raw_edges, warnings = _parse_file(record)
        all_warnings.extend(warnings)
        all_edges.extend(edges)
        all_raw_edges.extend(raw_edges)

    # Normalize identifiers: lowercase + resolve short table names
    all_edges, ambiguous = _normalize_edges(all_edges)
    all_raw_edges, _ = _normalize_edges(all_raw_edges)
    for amb in ambiguous:
        all_warnings.append(ParseWarning(
            file="<graph>",
            error=(
                f"ambiguous table name {amb!r} matches multiple qualified tables — "
                f"leaving as-is; qualify references to disambiguate"
            ),
            severity="warn",
        ))

    for edge in all_edges:
        graph.add_edge(edge.source_col, edge.target_col, data=edge)

    raw_graph: nx.DiGraph = nx.DiGraph()
    for edge in all_raw_edges:
        raw_graph.add_edge(edge.source_col, edge.target_col, data=edge)

    # Detect cycles and warn
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

    file_stats, error_files = _compute_file_stats(graph, all_warnings)

    return GraphResult(
        graph=graph,
        raw_graph=raw_graph,
        warnings=all_warnings,
        file_stats=file_stats,
        error_files=error_files,
    )


def build_graph(records: list[FileRecord]) -> nx.DiGraph:
    """Parse all FileRecords and return a lineage DAG (warnings discarded)."""
    return build_graph_with_warnings(records).graph


def upstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading TO col_id (DFS backwards)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited_edges: set[tuple[str, str]] = set()
    queue = [col_id]
    visited_nodes: set[str] = {col_id}
    while queue:
        current = queue.pop()
        for pred in graph.predecessors(current):
            edge_key = (pred, current)
            if edge_key not in visited_edges:
                visited_edges.add(edge_key)
                edge_data = graph.edges[pred, current].get("data")
                if edge_data:
                    edges.append(edge_data)
            if pred not in visited_nodes:
                visited_nodes.add(pred)
                queue.append(pred)
    return edges


def downstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading FROM col_id (DFS forwards)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited_edges: set[tuple[str, str]] = set()
    visited_nodes: set[str] = {col_id}
    queue = [col_id]
    while queue:
        current = queue.pop()
        for succ in graph.successors(current):
            edge_key = (current, succ)
            if edge_key not in visited_edges:
                visited_edges.add(edge_key)
                edge_data = graph.edges[current, succ].get("data")
                if edge_data:
                    edges.append(edge_data)
            if succ not in visited_nodes:
                visited_nodes.add(succ)
                queue.append(succ)
    return edges


def column_metadata(graph: nx.DiGraph, table: str) -> list[ColumnMeta]:
    """Return ColumnMeta for every column node belonging to `table`.

    Walks predecessors to build deduped, order-preserving source_tables and
    expressions. edge_data is captured from preds[0] only — preserving the
    current API contract where source_file/source_cell/source_line/transform_type
    come from the first predecessor even when expressions aggregate across all.

    Returns an empty list when no columns belong to `table` — the route layer
    is responsible for raising 404.
    """
    cols: list[ColumnMeta] = []
    for node in graph.nodes():
        if "." not in node:
            continue
        t, col = split_column_id(node)
        if t != table:
            continue
        preds = list(graph.predecessors(node))
        edge_data = None
        source_tables: list[str] = []
        seen_exprs: list[str] = []
        if preds:
            edge_data = graph.edges[preds[0], node].get("data")
            for pred in preds:
                if "." in pred:
                    st = split_column_id(pred)[0]
                    if st not in source_tables:
                        source_tables.append(st)
                ed = graph.edges[pred, node].get("data")
                if ed and ed.expression and ed.expression not in seen_exprs:
                    seen_exprs.append(ed.expression)
        cols.append(ColumnMeta(
            node_id=node,
            table=t,
            column=col,
            source_tables=source_tables,
            expressions=seen_exprs,
            edge_data=edge_data,
        ))
    return cols


def trace_paths(raw_graph: nx.DiGraph, col_id: str, max_paths: int = 500) -> tuple[list[list[dict]], bool]:
    """DFS backward from col_id, following both named and wildcard edges.

    Wildcard edges (tbl.* → other.*) are synthesized into named column edges
    using the column name being traced at each depth level, so the full
    source→temp_view→target chain is reconstructed correctly.

    Cycles are prevented by the per-path visited set (with backtracking).
    Uses mutable backtracking (single shared path list, append/pop) to avoid
    creating a new list copy at every recursion level.
    """

    def step_dict(src: str, tgt: str, edge_data) -> dict:
        if edge_data:
            return {
                "source_col": src,
                "target_col": tgt,
                "transform_type": edge_data.transform_type,
                "expression": edge_data.expression,
                "source_file": edge_data.source_file,
                "source_cell": edge_data.source_cell,
                "source_line": edge_data.source_line,
                "confidence": edge_data.confidence,
                "qualified": edge_data.qualified,
            }
        return {"source_col": src, "target_col": tgt, "transform_type": None,
                "expression": None, "source_file": None, "source_cell": None,
                "source_line": None, "confidence": "certain", "qualified": True}

    def get_preds(node: str, visited: set[str]) -> list[tuple[str, str, object]]:
        """Return (pred_node, target_node, edge_data) for all effective predecessors."""
        result = []
        if node in raw_graph:
            for pred in raw_graph.predecessors(node):
                if pred not in visited:
                    result.append((pred, node, raw_graph.edges[pred, node].get("data")))
        # Wildcard expansion: if tbl.col, check tbl.* predecessors
        if "." in node:
            tbl, col = split_column_id(node)
            wc = f"{tbl}.*"
            if wc in raw_graph and wc not in visited:
                for pred_wc in raw_graph.predecessors(wc):
                    if pred_wc in visited:
                        continue
                    e = raw_graph.edges[pred_wc, wc].get("data")
                    pred_named = f"{pred_wc[:-2]}.{col}" if pred_wc.endswith(".*") else pred_wc
                    if pred_named not in visited:
                        result.append((pred_named, node, e))
        return result

    all_paths: list[list[dict]] = []
    truncated = False
    # Single mutable path — append on enter, pop on exit (no per-level list copies)
    current_path: list[dict] = []

    def dfs(node: str, visited: set[str]) -> None:
        nonlocal truncated
        if truncated:
            return
        preds = get_preds(node, visited)
        if not preds:
            if current_path:
                all_paths.append(list(reversed(current_path)))
                if len(all_paths) >= max_paths:
                    truncated = True
            return
        for pred, tgt, edge_data in preds:
            if truncated:
                return
            current_path.append(step_dict(pred, tgt, edge_data))
            visited.add(pred)
            dfs(pred, visited)
            current_path.pop()
            visited.discard(pred)

    dfs(col_id, {col_id})
    return all_paths, truncated


# ── Lineage Trace ────────────────────────────────────────────────────────────

# Synthetic-column suffixes the SQL parser uses to mark predicate / join lineage.
_FILTER_SUFFIXES: dict[str, str] = {
    "__filter__": "where",
    "__qualify__": "qualify",
    "__having__": "having",
}
_JOIN_SUFFIX = "__joinkey__"
_TRACE_TEMP_VIEW_DEPTH_CAP = 16


def _is_synthetic_column(col_id: str) -> bool:
    """A column whose name segment is wrapped in __ — the parser's predicate /
    joinkey / placeholder convention. Used to keep synthetic targets out of the
    upstream-columns chip list."""
    if "." not in col_id:
        return False
    _, col = split_column_id(col_id)
    return col.startswith("__") and col.endswith("__")


def _kind_from_source_file(source_file: str | None) -> str:
    """Coarse SQL-vs-PySpark classification by file extension. Notebooks count
    as SQL when the dispatch path produced SQL edges and as Python otherwise —
    here we use extension only since every edge already carries the file."""
    if not source_file:
        return "sql"
    ext = os.path.splitext(source_file)[1].lower()
    if ext == ".py":
        return "pyspark"
    if ext == ".ipynb":
        return "pyspark"
    return "sql"


def _index_raw_graph(raw_graph: nx.DiGraph) -> tuple[
    dict[str, list[LineageEdge]],
    dict[str, list[LineageEdge]],
]:
    """Single-pass bucketing of raw_graph.edges() into target→edges and source→edges
    indexes. Avoids re-scanning the graph for every column or temp-view lookup."""
    by_target: dict[str, list[LineageEdge]] = {}
    by_source: dict[str, list[LineageEdge]] = {}
    for u, v, d in raw_graph.edges(data=True):
        edge = d.get("data")
        if edge is None:
            continue
        by_target.setdefault(v, []).append(edge)
        by_source.setdefault(u, []).append(edge)
    return by_target, by_source


def _table_synthetic_targets(
    by_target: dict[str, list[LineageEdge]], table: str, suffix: str
) -> list[LineageEdge]:
    """Return raw edges whose target column is `{table}.{suffix}` (e.g. agg_revenue.__filter__)."""
    return list(by_target.get(f"{table}.{suffix}", []))


def _is_temp_view_node(
    table: str,
    by_target: dict[str, list[LineageEdge]],
    by_source: dict[str, list[LineageEdge]],
    resolved_graph: nx.DiGraph,
) -> bool:
    """True when `table` is a temp-view-or-CTE node in raw_graph.

    A temp view is a table that has writers in raw_graph but does NOT have any
    real column nodes in the resolved lineage_graph (because resolve_temp_views
    short-circuited it out). This is intentionally a derived check rather than
    a parser-emitted flag: we don't introduce a new state global, and the
    resolved-vs-raw asymmetry is exactly the signal we need.
    """
    has_writers = any(
        tgt.startswith(f"{table}.")
        for tgt in by_target.keys()
    )
    if not has_writers:
        return False
    # Real terminal table → some column on it exists in resolved graph.
    for node in resolved_graph.nodes():
        if "." in node:
            t, _ = split_column_id(node)
            if t == table:
                return False
    return True


def _predicate_from_synthetic_edges(
    edges: list[LineageEdge],
    kind: Literal["where", "having", "qualify"],
) -> list[TraceStepPredicate]:
    """Group synthetic predicate edges by (expression, source_line) and collapse
    each group into one TraceStepPredicate carrying the deduped source columns."""
    groups: dict[tuple[str | None, int | None], TraceStepPredicate] = {}
    for e in edges:
        key = (e.expression, e.source_line)
        pred = groups.get(key)
        if pred is None:
            pred = TraceStepPredicate(
                kind=kind,
                expression=e.expression,
                source_columns=[],
                source_line=e.source_line,
            )
            groups[key] = pred
        if e.source_col not in pred.source_columns:
            pred.source_columns.append(e.source_col)
    return list(groups.values())


def _joins_from_synthetic_edges(edges: list[LineageEdge]) -> list[TraceStepJoin]:
    """Same shape as predicate grouping but for __joinkey__ edges."""
    groups: dict[tuple[str | None, int | None], TraceStepJoin] = {}
    for e in edges:
        key = (e.expression, e.source_line)
        join = groups.get(key)
        if join is None:
            join = TraceStepJoin(
                expression=e.expression,
                source_columns=[],
                source_line=e.source_line,
            )
            groups[key] = join
        if e.source_col not in join.source_columns:
            join.source_columns.append(e.source_col)
    return list(groups.values())


def lineage_trace(
    graph: nx.DiGraph,
    raw_graph: nx.DiGraph,
    table: str,
    column: str,
    max_steps: int = 50,
) -> list[TraceStep]:
    """Return Lineage Trace Steps for the column `{table}.{column}`.

    Walks `raw_graph` upstream from the column's immediate writer edges,
    groups them by source-table boundary, attaches sibling __filter__ /
    __qualify__ / __having__ / __joinkey__ edges that target the writer's
    target table, and rolls up predicates from collapsed temp-view / CTE
    writers (annotated in `via_temp_views`).

    Returns `[]` when the column does not exist in the resolved `graph`
    (route translates to 404) or when the column has no writers (source-table
    column). Truncates at `max_steps` Trace Steps, sorted by `(source_file,
    source_line)`.
    """
    col_id = f"{table}.{column}"
    if col_id not in graph:
        return []

    by_target, by_source = _index_raw_graph(raw_graph)

    writer_edges = list(by_target.get(col_id, []))
    if not writer_edges:
        return []

    # Group writer edges by (source_table, source_file, source_cell). One group → one Trace Step.
    StepKey = tuple[str, str, int | None]
    grouped: dict[StepKey, list[LineageEdge]] = {}
    for e in writer_edges:
        if "." not in e.source_col:
            continue
        src_tbl, _ = split_column_id(e.source_col)
        key: StepKey = (src_tbl, e.source_file or "", e.source_cell)
        grouped.setdefault(key, []).append(e)

    steps: list[TraceStep] = []
    for (src_tbl, src_file, src_cell), edges in grouped.items():
        target_table = split_column_id(edges[0].target_col)[0]
        step_lines = [e.source_line for e in edges if e.source_line is not None]
        step_line = min(step_lines) if step_lines else None

        writes = [
            TraceStepWrite(
                column_id=e.target_col,
                expression=e.expression,
                transform_type=e.transform_type,
                source_line=e.source_line,
            )
            for e in edges
        ]

        filters: list[TraceStepPredicate] = []
        joins: list[TraceStepJoin] = []
        # Pull synthetic predicate / join edges that target the consuming target_table.
        for suffix, kind in _FILTER_SUFFIXES.items():
            filters.extend(
                _predicate_from_synthetic_edges(
                    _table_synthetic_targets(by_target, target_table, suffix),
                    kind,
                )
            )
        joins.extend(
            _joins_from_synthetic_edges(
                _table_synthetic_targets(by_target, target_table, _JOIN_SUFFIX)
            )
        )

        # Walk upstream through any temp-view source tables to roll up their predicates.
        via_temp_views: list[str] = []
        seen_views: set[str] = set()
        frontier: list[str] = [src_tbl]
        depth = 0
        while frontier and depth < _TRACE_TEMP_VIEW_DEPTH_CAP:
            next_frontier: list[str] = []
            for view in frontier:
                if view in seen_views:
                    continue
                seen_views.add(view)
                if not _is_temp_view_node(view, by_target, by_source, graph):
                    continue
                # This source IS a temp view: pull its predicates and joins.
                via_temp_views.append(view)
                for suffix, kind in _FILTER_SUFFIXES.items():
                    filters.extend(
                        _predicate_from_synthetic_edges(
                            _table_synthetic_targets(by_target, view, suffix),
                            kind,
                        )
                    )
                joins.extend(
                    _joins_from_synthetic_edges(
                        _table_synthetic_targets(by_target, view, _JOIN_SUFFIX)
                    )
                )
                # Walk upstream one hop: the temp view's own writer source tables.
                for tgt, view_edges in by_target.items():
                    if not tgt.startswith(f"{view}."):
                        continue
                    if any(view_edges[0].target_col.endswith(s) for s in (*_FILTER_SUFFIXES, _JOIN_SUFFIX)):
                        continue
                    for ve in view_edges:
                        if "." in ve.source_col:
                            up_tbl, _ = split_column_id(ve.source_col)
                            if up_tbl not in seen_views:
                                next_frontier.append(up_tbl)
            frontier = next_frontier
            depth += 1

        # upstream_columns: deduped real-column source IDs from writes only.
        upstream_columns: list[str] = []
        seen_up: set[str] = set()
        for e in edges:
            if e.source_col == col_id:
                continue
            if _is_synthetic_column(e.source_col):
                continue
            if e.source_col not in seen_up:
                seen_up.add(e.source_col)
                upstream_columns.append(e.source_col)

        steps.append(TraceStep(
            kind=_kind_from_source_file(src_file),
            source_file=src_file,
            source_cell=src_cell,
            source_line=step_line,
            target_table=target_table,
            writes=writes,
            filters=filters,
            joins=joins,
            via_temp_views=via_temp_views,
            upstream_columns=upstream_columns,
        ))

    steps.sort(key=lambda s: (s.source_file, s.source_line if s.source_line is not None else 0))
    return steps[:max_steps]
