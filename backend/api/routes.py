"""FastAPI route handlers for the DataLineage Explorer API."""
from __future__ import annotations
import uuid
import networkx as nx
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from lineage.engine import build_graph_with_warnings
from lineage.engine import upstream as engine_upstream
from lineage.engine import downstream as engine_downstream
from lineage.engine import trace_paths as engine_trace_paths
from lineage.ids import split_column_id
from lineage.models import ParseWarning
from ingestion.upload import ingest_zip
from api.models import SourceEntry
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
        "confidence": edge.confidence,
        "qualified": edge.qualified,
    }


def _remove_source_files(files: set[str]) -> None:
    """Remove all edges contributed by `files` from both graphs, then drop orphan nodes."""
    if not files:
        return
    edges = [(u, v) for u, v, d in state.lineage_graph.edges(data=True)
             if d.get("data") and d["data"].source_file in files]
    state.lineage_graph.remove_edges_from(edges)
    state.lineage_graph.remove_nodes_from(
        [n for n in state.lineage_graph.nodes() if state.lineage_graph.degree(n) == 0]
    )
    raw_edges = [(u, v) for u, v, d in state.raw_graph.edges(data=True)
                 if d.get("data") and d["data"].source_file in files]
    state.raw_graph.remove_edges_from(raw_edges)
    state.raw_graph.remove_nodes_from(
        [n for n in state.raw_graph.nodes() if state.raw_graph.degree(n) == 0]
    )


def _graph_to_payload(graph: nx.DiGraph, col_id: str) -> dict:
    """Return nodes/edges subgraph reachable from col_id in either direction."""
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
    return [entry.to_public_dict() for entry in state.source_registry.values()]


@router.post("/sources")
async def register_source(
    source_type: str = Form(...),
    url: str = Form(default=""),
    token: str = Form(default=""),
    file: UploadFile | None = File(default=None),
):
    source_id = str(uuid.uuid4())[:8]
    entry = SourceEntry(id=source_id, source_type=source_type, url=url, token=token)

    if source_type != "upload":
        raise HTTPException(status_code=400, detail="source_type must be 'upload'")

    if file is None:
        raise HTTPException(status_code=400, detail="file is required for upload source")
    zip_bytes = await file.read()
    MAX_ZIP_BYTES = 50 * 1024 * 1024  # 50 MB
    if len(zip_bytes) > MAX_ZIP_BYTES:
        raise HTTPException(status_code=413, detail="ZIP file exceeds 50 MB limit")
    try:
        entry.records = ingest_zip(zip_bytes, source_ref=source_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    entry.url = file.filename or "upload"

    state.source_registry[source_id] = entry
    return entry.to_public_dict()


@router.delete("/sources/{source_id}")
def delete_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")
    entry = state.source_registry.get(source_id)
    if entry:
        _remove_source_files(entry.parsed_files)
        state.parse_warnings = [
            w for w in state.parse_warnings if w.get("source_id") != source_id
        ]
    del state.source_registry[source_id]
    return {"ok": True}


@router.get("/sources/{source_id}/files")
def list_source_files(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")
    entry = state.source_registry[source_id]
    file_stats = entry.file_stats
    error_files = entry.error_files

    result = []
    for fname, stats in sorted(file_stats.items()):
        if fname in error_files:
            confidence = "low"
        elif stats.get("approximate_count", 0) > 0 or stats.get("warning_count", 0) > 0:
            confidence = "medium"
        else:
            confidence = "high"
        result.append({
            "file": fname,
            "edge_count": stats["edge_count"],
            "confidence": confidence,
        })
    return result


@router.post("/sources/{source_id}/refresh")
def refresh_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")

    entry = state.source_registry[source_id]
    result = build_graph_with_warnings(entry.records)

    _remove_source_files(entry.parsed_files)
    state.lineage_graph = nx.compose(state.lineage_graph, result.graph)
    state.raw_graph = nx.compose(state.raw_graph, result.raw_graph)
    state.parse_warnings = [w for w in state.parse_warnings if w.get("source_id") != source_id]
    state.parse_warnings.extend(
        {"file": w.file, "error": w.error, "severity": w.severity, "source_id": source_id}
        for w in result.warnings
    )

    entry.parsed_files = set(result.file_stats.keys())
    entry.file_stats = result.file_stats
    entry.error_files = result.error_files
    entry.status = "parsed"
    entry.file_count = len(entry.records)
    entry.warning_count = len(result.warnings)

    return {"ok": True, "file_count": len(entry.records), "edge_count": result.graph.number_of_edges()}


# ── Tables / Columns ─────────────────────────────────────────────────────────

@router.get("/tables")
def list_tables():
    tables: dict[str, int] = {}
    for node in state.lineage_graph.nodes():
        if "." in node:
            table, _ = split_column_id(node)
            tables[table] = tables.get(table, 0) + 1

    # Classify each table's role based on edge directions
    target_tables: set[str] = set()  # tables written to (appear as edge target)
    source_tables: set[str] = set()  # tables read from (appear as edge source)
    for u, v, data in state.lineage_graph.edges(data=True):
        if "." in u:
            source_tables.add(split_column_id(u)[0])
        if "." in v:
            target_tables.add(split_column_id(v)[0])

    result = []
    for t, c in sorted(tables.items()):
        is_target = t in target_tables
        is_source = t in source_tables
        if t == "result":
            role = "result"
        elif is_target and is_source:
            role = "intermediate"   # both read and written
        elif is_target:
            role = "target"         # only written to (final output)
        else:
            role = "source"         # only read from (external source)
        result.append({"table": t, "column_count": c, "role": role})
    return result


@router.get("/tables/{table}/columns")
def list_columns(table: str):
    cols = []
    for node in state.lineage_graph.nodes():
        if "." in node:
            t, col = split_column_id(node)
            if t == table:
                preds = list(state.lineage_graph.predecessors(node))
                edge_data = None
                source_tables: list[str] = []
                seen_exprs: list[str] = []
                if preds:
                    edge_data = state.lineage_graph.edges[preds[0], node].get("data")
                    # Collect all distinct source tables and expressions for this column
                    for pred in preds:
                        if "." in pred:
                            st = split_column_id(pred)[0]
                            if st not in source_tables:
                                source_tables.append(st)
                        ed = state.lineage_graph.edges[pred, node].get("data")
                        if ed and ed.expression and ed.expression not in seen_exprs:
                            seen_exprs.append(ed.expression)
                combined_expression = "\n".join(seen_exprs) if seen_exprs else None
                cols.append({
                    "id": node,
                    "table": t,
                    "column": col,
                    "source_tables": source_tables,
                    "source_file": edge_data.source_file if edge_data else None,
                    "source_cell": edge_data.source_cell if edge_data else None,
                    "source_line": edge_data.source_line if edge_data else None,
                    "transform_type": edge_data.transform_type if edge_data else None,
                    "expression": combined_expression,
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


@router.get("/lineage/paths")
def get_lineage_paths(table: str, column: str):
    col_id = f"{table}.{column}"
    paths, truncated = engine_trace_paths(state.raw_graph, col_id)
    return {"target": col_id, "paths": [{"steps": p} for p in paths], "truncated": truncated}


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
        if q_lower in node.lower() and "." in node:
            table, col = split_column_id(node)
            results.append({"id": node, "table": table, "column": col})
    return results


# ── Warnings ─────────────────────────────────────────────────────────────────

@router.get("/warnings")
def get_warnings():
    return state.parse_warnings
