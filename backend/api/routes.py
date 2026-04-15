"""FastAPI route handlers for the DataLineage Explorer API."""
from __future__ import annotations
import uuid
import networkx as nx
import git
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from lineage.engine import build_graph_with_warnings
from lineage.engine import upstream as engine_upstream
from lineage.engine import downstream as engine_downstream
from lineage.models import ParseWarning
from ingestion.upload import ingest_zip
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
    }


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
    return [
        {k: v for k, v in entry.items() if not k.startswith("_")}
        for entry in state.source_registry.values()
    ]


@router.post("/sources")
async def register_source(
    source_type: str = Form(...),
    url: str = Form(default=""),
    token: str = Form(default=""),
    file: UploadFile | None = File(default=None),
):
    source_id = str(uuid.uuid4())[:8]
    entry: dict = {
        "id": source_id,
        "source_type": source_type,
        "url": url,
        "_token": token,  # stored with _ prefix so it is filtered from public responses
        "status": "registered",
        "file_count": 0,
    }

    VALID_SOURCE_TYPES = {"upload", "git", "databricks"}
    if source_type not in VALID_SOURCE_TYPES:
        raise HTTPException(status_code=400, detail=f"source_type must be one of: {sorted(VALID_SOURCE_TYPES)}")

    if source_type == "upload":
        if file is None:
            raise HTTPException(status_code=400, detail="file is required for upload source")
        zip_bytes = await file.read()
        MAX_ZIP_BYTES = 50 * 1024 * 1024  # 50 MB
        if len(zip_bytes) > MAX_ZIP_BYTES:
            raise HTTPException(status_code=413, detail="ZIP file exceeds 50 MB limit")
        entry["_zip_bytes"] = zip_bytes
        entry["url"] = file.filename or "upload"

    elif source_type == "git" and url:
        # Lightweight auth check: ls-remote avoids a full clone
        if token:
            auth_url = url.replace("https://", f"https://{token}@", 1) if url.startswith("https://") else url
        else:
            auth_url = url
        try:
            git.cmd.Git().ls_remote(auth_url)
        except git.GitCommandError as exc:
            # Sanitize: never echo back the token-embedded URL
            raise HTTPException(
                status_code=400,
                detail=f"Git authentication failed: {exc.stderr.strip() if exc.stderr else str(exc)}",
            )

    elif source_type == "databricks" and url:
        from databricks.sdk import WorkspaceClient
        try:
            client = WorkspaceClient(host=url, token=token)
            list(client.workspace.list(path="/"))
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Databricks authentication failed: invalid host or token",
            )

    state.source_registry[source_id] = entry
    return {k: v for k, v in entry.items() if not k.startswith("_")}


@router.delete("/sources/{source_id}")
def delete_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")
    # Remove this source's contributions from lineage graph
    entry = state.source_registry.get(source_id)
    if entry:
        old_files = entry.get("_parsed_files", set())
        if old_files:
            edges_to_remove = [
                (u, v) for u, v, d in state.lineage_graph.edges(data=True)
                if d.get("data") and d["data"].source_file in old_files
            ]
            state.lineage_graph.remove_edges_from(edges_to_remove)
            orphan_nodes = [n for n in state.lineage_graph.nodes() if state.lineage_graph.degree(n) == 0]
            state.lineage_graph.remove_nodes_from(orphan_nodes)
    del state.source_registry[source_id]
    return {"ok": True}


@router.post("/sources/{source_id}/refresh")
def refresh_source(source_id: str):
    if source_id not in state.source_registry:
        raise HTTPException(status_code=404, detail="Source not found")

    entry = state.source_registry[source_id]
    source_type = entry["source_type"]
    records = []

    if source_type == "upload":
        zip_bytes = entry.get("_zip_bytes", b"")
        records = ingest_zip(zip_bytes, source_ref=source_id)

    elif source_type == "git":
        from ingestion.git import ingest_git
        try:
            records = ingest_git(
                url=entry["url"],
                token=entry.get("_token") or None,
                source_ref=source_id,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    elif source_type == "databricks":
        from ingestion.databricks import ingest_databricks
        try:
            records = ingest_databricks(
                host=entry["url"],
                token=entry.get("_token", ""),
                source_ref=source_id,
            )
        except Exception:
            raise HTTPException(status_code=400, detail="Databricks ingestion failed: check host and token")

    new_graph, new_warnings = build_graph_with_warnings(records)

    # Remove old contributions from this source before adding new ones
    old_files = entry.get("_parsed_files", set())
    if old_files:
        edges_to_remove = [
            (u, v) for u, v, d in state.lineage_graph.edges(data=True)
            if d.get("data") and d["data"].source_file in old_files
        ]
        state.lineage_graph.remove_edges_from(edges_to_remove)
        orphan_nodes = [n for n in state.lineage_graph.nodes() if state.lineage_graph.degree(n) == 0]
        state.lineage_graph.remove_nodes_from(orphan_nodes)

    state.lineage_graph = nx.compose(state.lineage_graph, new_graph)
    state.parse_warnings.extend(
        {"file": w.file, "error": w.error} for w in new_warnings
    )

    # Track which files this source contributed
    entry["_parsed_files"] = {
        d["data"].source_file
        for _, _, d in new_graph.edges(data=True)
        if d.get("data") and d["data"].source_file
    }

    entry["status"] = "parsed"
    entry["file_count"] = len(records)

    return {"ok": True, "file_count": len(records), "edge_count": new_graph.number_of_edges()}


# ── Tables / Columns ─────────────────────────────────────────────────────────

@router.get("/tables")
def list_tables():
    tables: dict[str, int] = {}
    for node in state.lineage_graph.nodes():
        if "." in node:
            table, _ = node.rsplit(".", 1)
            tables[table] = tables.get(table, 0) + 1

    # Classify each table's role based on edge directions
    target_tables: set[str] = set()  # tables written to (appear as edge target)
    source_tables: set[str] = set()  # tables read from (appear as edge source)
    for u, v, data in state.lineage_graph.edges(data=True):
        if "." in u:
            source_tables.add(u.rsplit(".", 1)[0])
        if "." in v:
            target_tables.add(v.rsplit(".", 1)[0])

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
            t, col = node.rsplit(".", 1)
            if t == table:
                preds = list(state.lineage_graph.predecessors(node))
                edge_data = None
                source_tables: list[str] = []
                if preds:
                    edge_data = state.lineage_graph.edges[preds[0], node].get("data")
                    # Collect all distinct source tables for this column
                    for pred in preds:
                        if "." in pred:
                            st = pred.rsplit(".", 1)[0]
                            if st not in source_tables:
                                source_tables.append(st)
                cols.append({
                    "id": node,
                    "table": t,
                    "column": col,
                    "source_tables": source_tables,
                    "source_file": edge_data.source_file if edge_data else None,
                    "source_cell": edge_data.source_cell if edge_data else None,
                    "source_line": edge_data.source_line if edge_data else None,
                    "transform_type": edge_data.transform_type if edge_data else None,
                    "expression": edge_data.expression if edge_data else None,
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
            table, col = node.rsplit(".", 1)
            results.append({"id": node, "table": table, "column": col})
    return results


# ── Warnings ─────────────────────────────────────────────────────────────────

@router.get("/warnings")
def get_warnings():
    return state.parse_warnings
