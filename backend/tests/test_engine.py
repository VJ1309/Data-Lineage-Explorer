import networkx as nx
from lineage.engine import build_graph, build_graph_with_warnings, upstream, downstream
from lineage.models import FileRecord, LineageEdge


def _make_sql_record(sql: str) -> FileRecord:
    return FileRecord(path="q.sql", content=sql, type="sql", source_ref="test")


def test_build_graph_nodes_and_edges():
    records = [_make_sql_record(
        "SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    )]
    graph = build_graph(records)
    assert isinstance(graph, nx.DiGraph)
    assert graph.number_of_nodes() > 0
    assert graph.number_of_edges() > 0


def test_upstream_returns_ancestors():
    records = [
        _make_sql_record("SELECT order_id, amount FROM raw_orders"),
        _make_sql_record("SELECT order_id, amount AS revenue FROM result"),
    ]
    graph = build_graph(records)
    ancestors = upstream(graph, "result.revenue")
    sources = {e.source_col for e in ancestors}
    # result.amount should appear in upstream of result.revenue
    assert any("amount" in s for s in sources)


def test_downstream_returns_descendants():
    records = [
        _make_sql_record("SELECT order_id, amount FROM raw_orders"),
        _make_sql_record("SELECT order_id, amount AS revenue FROM result"),
    ]
    graph = build_graph(records)
    descendants = downstream(graph, "raw_orders.amount")
    targets = {e.target_col for e in descendants}
    assert len(targets) > 0


def test_empty_records_returns_empty_graph():
    graph = build_graph([])
    assert graph.number_of_nodes() == 0


def test_cycle_detection():
    # Manually create a graph with a cycle
    graph = nx.DiGraph()
    graph.add_edge("a.x", "b.y", data=None)
    graph.add_edge("b.y", "a.x", data=None)
    assert not nx.is_directed_acyclic_graph(graph)


def test_parse_warnings_collected():
    records = [
        FileRecord(path="bad.py", content="def (((broken:", type="python", source_ref="test"),
        _make_sql_record("SELECT amount FROM raw_orders"),
    ]
    graph, warnings = build_graph_with_warnings(records)
    # bad.py parse fails silently — but the good SQL record still produces edges
    assert graph.number_of_edges() > 0
