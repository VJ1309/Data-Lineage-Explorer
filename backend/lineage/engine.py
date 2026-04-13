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
            if edge.source_col not in graph:
                graph.add_node(edge.source_col)
            if edge.target_col not in graph:
                graph.add_node(edge.target_col)
            graph.add_edge(edge.source_col, edge.target_col, data=edge)

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

    return graph, all_warnings


def build_graph(records: list[FileRecord]) -> nx.DiGraph:
    """Parse all FileRecords and return a lineage DAG (warnings discarded)."""
    graph, _ = build_graph_with_warnings(records)
    return graph


def upstream(graph: nx.DiGraph, col_id: str) -> list[LineageEdge]:
    """Return all LineageEdge objects on paths leading TO col_id (BFS backwards)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited: set[str] = set()
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
    """Return all LineageEdge objects on paths leading FROM col_id (BFS forwards)."""
    if col_id not in graph:
        return []
    edges: list[LineageEdge] = []
    visited: set[str] = set()
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
