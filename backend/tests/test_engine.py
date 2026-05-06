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
    result = build_graph_with_warnings(records)
    graph, warnings = result.graph, result.warnings
    # bad.py parse fails silently — but the good SQL record still produces edges
    assert graph.number_of_edges() > 0


def test_sql_parse_error_surfaces_as_warning():
    """A file whose SQL cannot be parsed must produce a ParseWarning, not silent empty."""
    record = FileRecord(
        path="bad.sql",
        content="THIS IS NOT SQL !!!###",
        type="sql",
        source_ref="test",
    )
    result = build_graph_with_warnings([record])
    warnings = result.warnings
    assert any("bad.sql" in w.file for w in warnings), (
        "parse error in bad.sql must produce a ParseWarning"
    )


def test_ambiguous_short_name_not_merged_and_warns():
    """If 'orders' matches both 'staging.orders' and 'prod.orders', do not merge — warn."""
    content = """
    INSERT INTO staging.orders SELECT id FROM raw_source;
    INSERT INTO prod.orders SELECT id FROM raw_other;
    INSERT INTO downstream SELECT id FROM orders;
    """
    rec = FileRecord(path="f.sql", content=content, type="sql", source_ref="t")
    result = build_graph_with_warnings([rec])
    graph, warnings = result.graph, result.warnings
    nodes = set(graph.nodes())
    # Both fully-qualified forms must survive as distinct nodes
    assert "staging.orders.id" in nodes, f"staging.orders merged away; nodes={sorted(nodes)}"
    assert "prod.orders.id" in nodes, f"prod.orders merged away; nodes={sorted(nodes)}"
    assert any("ambiguous" in w.error.lower() for w in warnings), (
        f"expected ambiguity warning; got {[w.error for w in warnings]}"
    )


def test_unambiguous_short_name_still_merges():
    """If 'orders' appears only as 'uc.prod.orders', still merge the short form."""
    content = """
    INSERT INTO uc.prod.orders SELECT id FROM raw_source;
    INSERT INTO downstream SELECT id FROM orders;
    """
    rec = FileRecord(path="f.sql", content=content, type="sql", source_ref="t")
    result = build_graph_with_warnings([rec])
    graph = result.graph
    nodes = set(graph.nodes())
    # 'orders.id' short form should have been merged into 'uc.prod.orders.id'
    assert "uc.prod.orders.id" in nodes
    assert "orders.id" not in nodes, f"short name not merged when unambiguous; nodes={sorted(nodes)}"


# ---------------------------------------------------------------------------
# Warning fidelity (R4) — procedural files no longer fail wholesale; per-file
# stats reflect per-statement warnings only.
# ---------------------------------------------------------------------------


def test_procedural_file_with_valid_dml_produces_no_warnings():
    """Pre-U2 a BEGIN…END wrapper around valid DML produced one file-level
    'Unexpected token' warning and zero edges. With the normaliser in place the
    warning is gone, edges are produced, and the file is not flagged in error_files."""
    content = """
    BEGIN
      DECLARE v STRING DEFAULT 'unused';
      INSERT INTO target SELECT amount FROM raw_orders;
    END
    """
    rec = FileRecord(path="proc.sql", content=content, type="sql", source_ref="t")
    result = build_graph_with_warnings([rec])

    file_warnings = [w for w in result.warnings if w.file == "proc.sql"]
    assert not any("Unexpected token" in w.error for w in file_warnings), (
        f"file-level Unexpected token leaked: {file_warnings}"
    )
    assert "proc.sql" not in result.error_files, (
        f"valid procedural file flagged as error: {result.error_files}"
    )
    # Edge survives end-to-end
    assert ("raw_orders.amount", "target.amount") in {
        (u, v) for u, v in result.graph.edges()
    }, f"edge missing; nodes={sorted(result.graph.nodes())}"


def test_procedural_file_with_dynamic_sql_emits_per_statement_warning():
    """Non-foldable EXECUTE IMMEDIATE produces ONE per-statement warning, not a
    wholesale file failure. file_stats.warning_count reflects that single warning."""
    content = """
    BEGIN
      EXECUTE IMMEDIATE current_query();
      INSERT INTO good_target SELECT a FROM s;
    END
    """
    rec = FileRecord(path="dyn.sql", content=content, type="sql", source_ref="t")
    result = build_graph_with_warnings([rec])

    file_warnings = [w for w in result.warnings if w.file == "dyn.sql"]
    non_foldable = [w for w in file_warnings if "Non-foldable EXECUTE IMMEDIATE" in w.error]
    assert len(non_foldable) == 1, f"expected 1 dynamic-SQL warning; got {file_warnings}"
    # The valid INSERT in the same block still produced edges — partial failure
    # does NOT cascade into a wholesale file drop.
    assert ("s.a", "good_target.a") in {
        (u, v) for u, v in result.graph.edges()
    }, f"valid DML lost when dynamic SQL was non-foldable; nodes={sorted(result.graph.nodes())}"
    # File is not in error_files (per-statement warning is severity=warn, not error)
    assert "dyn.sql" not in result.error_files
