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
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/warnings")
    assert resp.status_code == 200
    for w in resp.json():
        assert "source_id" in w, f"Warning missing source_id: {w}"
