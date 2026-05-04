"""Lineage engine: builds and queries a NetworkX DAG from FileRecords."""
from __future__ import annotations
import networkx as nx
from lineage.ids import split_column_id
from lineage.models import FileRecord, GraphResult, LineageEdge, ParseResult, ParseWarning
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
