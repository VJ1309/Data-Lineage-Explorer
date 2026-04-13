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
    assert "total" in col_names or "customer_id" in col_names


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
