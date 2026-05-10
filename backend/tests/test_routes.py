import io
import zipfile
import pytest
from fastapi.testclient import TestClient
from main import app
import state

client = TestClient(app)


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def reset_state():
    """Reset in-memory state before each test."""
    import networkx as nx
    state.source_registry.clear()
    state.lineage_graph = nx.DiGraph()
    state.raw_graph = nx.DiGraph()
    state.parse_warnings.clear()
    yield


def test_list_sources_empty():
    resp = client.get("/sources")
    assert resp.status_code == 200
    assert resp.json() == []


def test_register_upload_source_and_refresh():
    zip_bytes = _make_zip({
        "query.sql": "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    })
    # Register source via upload
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    assert resp.status_code == 200
    source_id = resp.json()["id"]

    # Refresh (parse)
    resp = client.post(f"/sources/{source_id}/refresh")
    assert resp.status_code == 200
    refresh_data = resp.json()
    assert refresh_data["file_count"] > 0
    assert refresh_data["edge_count"] >= 0

    # Tables should now have data
    resp = client.get("/tables")
    assert resp.status_code == 200
    tables = resp.json()
    assert len(tables) > 0


def test_register_upload_rejects_invalid_zip():
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", b"not a zip", "application/zip")},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid ZIP file"


def test_get_columns_for_table():
    zip_bytes = _make_zip({
        "query.sql": "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/tables/result/columns")
    assert resp.status_code == 200
    cols = resp.json()
    col_names = [c["column"] for c in cols]
    assert len(col_names) > 0  # at least one column found


def test_lineage_endpoint():
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "result", "column": "total"})
    assert resp.status_code == 200
    data = resp.json()
    assert "upstream" in data
    assert "downstream" in data
    assert "graph" in data


def test_impact_endpoint():
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/impact", params={"table": "raw_orders", "column": "amount"})
    assert resp.status_code == 200
    data = resp.json()
    assert "downstream" in data
    for edge in data["downstream"]:
        assert "confidence" in edge, f"Impact edge missing confidence: {edge}"
        assert edge["confidence"] in ("certain", "approximate")


def test_search_endpoint():
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/search", params={"q": "amount"})
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) > 0


def test_delete_source():
    zip_bytes = _make_zip({"q.sql": "SELECT 1"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    resp = client.delete(f"/sources/{source_id}")
    assert resp.status_code == 200
    resp = client.get("/sources")
    assert all(s["id"] != source_id for s in resp.json())


def test_delete_source_removes_warnings():
    zip_bytes = _make_zip({"broken.py": "def f(:\n    pass"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/warnings")
    assert any(w["source_id"] == source_id for w in resp.json())

    resp = client.delete(f"/sources/{source_id}")
    assert resp.status_code == 200

    resp = client.get("/warnings")
    assert all(w["source_id"] != source_id for w in resp.json())


def test_warnings_endpoint():
    resp = client.get("/warnings")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_token_not_in_source_response():
    """Tokens must never appear in GET /sources response."""
    # Upload source (no token, but verify structure)
    zip_bytes = _make_zip({"q.sql": "SELECT 1"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload", "token": "secret-token-123"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    assert resp.status_code == 200
    source_data = resp.json()
    assert "secret-token-123" not in str(source_data)

    source_id = source_data["id"]
    resp = client.get("/sources")
    sources_str = str(resp.json())
    assert "secret-token-123" not in sources_str


def test_lineage_edge_has_confidence_field():
    """Every edge returned by /lineage must include a confidence field."""
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "result", "column": "total"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["upstream"]) > 0, "expected at least one upstream edge"
    for edge in data["upstream"] + data["downstream"] + data["graph"]["edges"]:
        assert "confidence" in edge, f"Edge missing confidence: {edge}"
        assert edge["confidence"] in ("certain", "approximate")


def test_approximate_edge_for_struct_field():
    """Struct field access must produce an approximate edge in the API."""
    zip_bytes = _make_zip({
        "q.sql": "INSERT INTO summary SELECT info.city AS city FROM customers"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "summary", "column": "city"})
    assert resp.status_code == 200
    upstream_edges = resp.json()["upstream"]
    assert len(upstream_edges) == 1
    assert upstream_edges[0]["confidence"] == "approximate"


def test_warning_count_on_source_after_refresh():
    """Source entry must include warning_count after refresh."""
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/sources")
    src = next(s for s in resp.json() if s["id"] == source_id)
    assert "warning_count" in src
    assert src["warning_count"] == 0


def test_warnings_include_source_id():
    """Warnings in GET /warnings must include source_id field."""
    # A .py file with broken syntax triggers a ParseWarning in the engine
    zip_bytes = _make_zip({"broken.py": "def f(:\n    pass"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/warnings")
    assert resp.status_code == 200
    assert len(resp.json()) > 0, "expected at least one parse warning from broken input"
    for w in resp.json():
        assert "source_id" in w, f"Warning missing source_id: {w}"
        assert w["source_id"] == source_id
        assert "severity" in w, f"Warning missing severity: {w}"
        assert w["severity"] in ("info", "warn", "error")


def test_joinkey_columns_aggregate_all_on_expressions():
    """list_columns must return all distinct JOIN ON expressions for __joinkey__ nodes.

    Two JOINs with different ON columns produce two distinct expressions. Both must
    appear in the expression field — not just the one from preds[0].
    """
    sql = (
        "INSERT INTO result "
        "SELECT a.id, b.name, c.code "
        "FROM table_a a "
        "JOIN table_b b ON a.id = b.a_id "
        "JOIN table_c c ON a.ref = c.ref_id"
    )
    zip_bytes = _make_zip({"q.sql": sql})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/tables/result/columns")
    assert resp.status_code == 200
    cols = resp.json()

    jk_col = next((c for c in cols if c["column"] == "__joinkey__"), None)
    assert jk_col is not None, f"no __joinkey__ column; got: {[c['column'] for c in cols]}"

    expr = jk_col["expression"]
    assert expr is not None, "__joinkey__ expression must not be None when join keys are present"
    # Each distinct ON clause must appear — b.a_id is only in the first, c.ref_id only in the second
    assert "a_id" in expr, f"first JOIN ON expression missing from result; got: {expr!r}"
    assert "ref_id" in expr, f"second JOIN ON expression missing from result; got: {expr!r}"
    # Both join source tables must be listed
    assert "table_b" in jk_col["source_tables"]
    assert "table_c" in jk_col["source_tables"]


def test_lineage_edges_expose_qualified_field():
    """GET /lineage edges must include a 'qualified' boolean per Tier 1 spec."""
    zip_bytes = _make_zip({
        "q.sql": "INSERT INTO result SELECT id FROM t JOIN s ON t.id = s.id"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "result", "column": "id"})
    assert resp.status_code == 200
    edges = resp.json()["upstream"] + resp.json()["downstream"]
    assert edges, "expected edges for result.id"
    for e in edges:
        assert "qualified" in e, f"edge missing qualified field: {e}"
        assert isinstance(e["qualified"], bool)


# ---------------------------------------------------------------------------
# /tables synthetic flag (U7) — surfaces parser-internal placeholders so the
# frontend can render them distinctly from real catalog tables.
# ---------------------------------------------------------------------------


def test_tables_endpoint_marks_call_placeholder_as_synthetic():
    """A CALL to an unresolved procedure produces a __call_<proc>__ placeholder
    table; /tables must flag it `synthetic: true`."""
    zip_bytes = _make_zip({
        "q.sql": "CALL my_catalog.my_schema.run_etl('raw', 'silver')"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/tables")
    assert resp.status_code == 200
    by_name = {row["table"]: row for row in resp.json()}
    name = "__call_my_catalog_my_schema_run_etl__"
    assert name in by_name, f"missing synthetic CALL table; got {list(by_name)}"
    assert by_name[name]["synthetic"] is True


def test_tables_endpoint_marks_dynamic_sql_placeholder_as_synthetic():
    """Non-foldable EXECUTE IMMEDIATE produces a __dynamic_sql__ placeholder
    table; /tables must flag it synthetic."""
    zip_bytes = _make_zip({
        "q.sql": "BEGIN EXECUTE IMMEDIATE current_query(); END",
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    by_name = {row["table"]: row for row in client.get("/tables").json()}
    assert "__dynamic_sql__" in by_name, f"missing __dynamic_sql__; got {list(by_name)}"
    assert by_name["__dynamic_sql__"]["synthetic"] is True


def test_tables_endpoint_real_tables_not_synthetic():
    """Regression: real catalog tables continue to set synthetic=False."""
    zip_bytes = _make_zip({
        "q.sql": "INSERT INTO target SELECT amount FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    rows = client.get("/tables").json()
    by_name = {row["table"]: row for row in rows}
    assert by_name["raw_orders"]["synthetic"] is False
    assert by_name["target"]["synthetic"] is False


def test_tables_endpoint_excludes_collapsed_for_cursor_view():
    """A FOR cursor's __for_<id>__ synthetic temp view is collapsed by
    resolve_temp_views — it must not appear in /tables at all (real source
    tables take its place)."""
    sql = """
    BEGIN
      FOR row AS SELECT order_id, amount FROM orders DO
        INSERT INTO summary SELECT row.order_id, row.amount;
      END FOR;
    END
    """
    zip_bytes = _make_zip({"q.sql": sql})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    table_names = {row["table"] for row in client.get("/tables").json()}
    assert not any(t.startswith("__for_") for t in table_names), (
        f"__for_*__ leaked into /tables: {sorted(table_names)}"
    )
    # Real source tables present, end-to-end lineage holds
    assert "orders" in table_names
    assert "summary" in table_names


# ---------------------------------------------------------------------------
# Warning fidelity (R4) — procedural files no longer produce a wholesale
# file-level "Unexpected token" warning. Per-statement warnings only fire
# for genuinely unparseable embedded statements.
# ---------------------------------------------------------------------------


def test_warning_fidelity_procedural_file_no_unexpected_token():
    """A BEGIN…END block with valid embedded DML must not produce an
    'Unexpected token' file-level warning. Pre-U2 this would emit one such
    warning and zero edges; now it produces edges and zero warnings."""
    sql = """
    BEGIN
      DECLARE v STRING DEFAULT 'unused';
      INSERT INTO target SELECT amount FROM raw_orders;
    END
    """
    zip_bytes = _make_zip({"proc.sql": sql})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    warnings = client.get("/warnings").json()
    proc_warnings = [w for w in warnings if w.get("file") == "proc.sql"]
    assert not any("Unexpected token" in w["error"] for w in proc_warnings), (
        f"file-level Unexpected token warning leaked: {proc_warnings}"
    )

    # End-to-end edge survived
    resp = client.get(
        "/lineage", params={"table": "target", "column": "amount"}
    )
    assert resp.status_code == 200
    upstream = resp.json()["upstream"]
    assert any(
        e["source_col"].startswith("raw_orders.") for e in upstream
    ), f"expected raw_orders upstream of target.amount; got {upstream}"


# ---------------------------------------------------------------------------
# /lineage/trace — Lineage Trace endpoint
# ---------------------------------------------------------------------------


def _setup_with_files(files: dict[str, str]) -> str:
    zip_bytes = _make_zip(files)
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")
    return source_id


def test_lineage_trace_happy_path_with_filter():
    _setup_with_files({
        "agg.sql": (
            "INSERT INTO agg_revenue\n"
            "SELECT customer_id, SUM(amount) AS total_revenue\n"
            "FROM raw_orders WHERE status = 'completed' GROUP BY customer_id"
        )
    })
    resp = client.get(
        "/lineage/trace",
        params={"table": "agg_revenue", "column": "total_revenue"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["target"] == "agg_revenue.total_revenue"
    assert len(body["steps"]) == 1
    s = body["steps"][0]
    assert set(s.keys()) == {
        "kind", "source_file", "source_cell", "source_line", "target_table",
        "writes", "filters", "joins", "via_temp_views", "upstream_columns",
    }
    assert s["target_table"] == "agg_revenue"
    assert s["kind"] == "sql"
    assert len(s["filters"]) == 1
    assert s["filters"][0]["kind"] == "where"
    assert "raw_orders.status" in s["filters"][0]["source_columns"]
    assert s["upstream_columns"] == ["raw_orders.amount"]


def test_lineage_trace_unknown_column_returns_404():
    _setup_with_files({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.get(
        "/lineage/trace",
        params={"table": "no_table", "column": "no_col"},
    )
    assert resp.status_code == 404
    assert "no_table.no_col" in resp.json()["detail"]


def test_lineage_trace_source_column_returns_empty_steps():
    _setup_with_files({
        "agg.sql": "INSERT INTO mart SELECT amount FROM raw_orders",
    })
    # raw_orders.amount IS in the resolved graph but has no writers — empty steps.
    resp = client.get(
        "/lineage/trace",
        params={"table": "raw_orders", "column": "amount"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["target"] == "raw_orders.amount"
    assert body["steps"] == []


def test_lineage_trace_existing_endpoints_do_not_leak_trace_fields():
    """Targeted assertion: Trace-only field names must not appear in the JSON
    bodies of the unchanged endpoints. Smaller and more durable than full-body
    snapshots which trip on incidental ordering changes."""
    _setup_with_files({
        "q.sql": (
            "INSERT INTO target\n"
            "SELECT customer_id, amount FROM raw_orders WHERE status = 'paid'"
        )
    })
    trace_only_fields = {
        "via_temp_views",
        "target_table",
    }
    endpoints = [
        ("/tables", {}),
        ("/tables/target/columns", {}),
        ("/lineage", {"table": "target", "column": "amount"}),
        ("/lineage/paths", {"table": "target", "column": "amount"}),
        ("/impact", {"table": "raw_orders", "column": "amount"}),
        ("/search", {"q": "amount"}),
        ("/warnings", {}),
    ]
    import json as _json
    for url, params in endpoints:
        resp = client.get(url, params=params)
        assert resp.status_code == 200, f"{url} returned {resp.status_code}"
        body = _json.dumps(resp.json())
        for field in trace_only_fields:
            assert field not in body, (
                f"trace-only field {field!r} leaked into {url} response: {body[:200]}"
            )
