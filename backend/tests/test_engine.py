import networkx as nx
from lineage.engine import (
    build_graph,
    build_graph_with_warnings,
    downstream,
    lineage_trace,
    upstream,
)
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


# ---------------------------------------------------------------------------
# lineage_trace — Lineage Trace Steps for the column inspector
# ---------------------------------------------------------------------------


def _build_graphs(*sql_blocks: tuple[str, str]) -> tuple[nx.DiGraph, nx.DiGraph]:
    records = [
        FileRecord(path=path, content=sql, type="sql", source_ref="t")
        for path, sql in sql_blocks
    ]
    result = build_graph_with_warnings(records)
    return result.graph, result.raw_graph


def test_lineage_trace_immediate_step_with_filter():
    sql = (
        "INSERT INTO agg_revenue\n"
        "SELECT customer_id, SUM(amount) AS total_revenue\n"
        "FROM raw_orders\n"
        "WHERE status = 'completed'\n"
        "GROUP BY customer_id"
    )
    graph, raw = _build_graphs(("a.sql", sql))
    steps = lineage_trace(graph, raw, "agg_revenue", "total_revenue")
    assert len(steps) == 1
    s = steps[0]
    assert s.target_table == "agg_revenue"
    assert s.kind == "sql"
    assert s.source_file == "a.sql"
    assert len(s.writes) == 1
    assert s.writes[0].column_id == "agg_revenue.total_revenue"
    assert len(s.filters) == 1
    f = s.filters[0]
    assert f.kind == "where"
    assert "status" in (f.expression or "")
    assert "raw_orders.status" in f.source_columns
    assert s.upstream_columns == ["raw_orders.amount"]
    assert s.via_temp_views == []


def test_lineage_trace_join_keys_appear_on_step():
    sql = (
        "INSERT INTO joined_t\n"
        "SELECT a.id, b.name FROM left_t a JOIN right_t b ON a.id = b.id"
    )
    graph, raw = _build_graphs(("j.sql", sql))
    steps = lineage_trace(graph, raw, "joined_t", "id")
    assert len(steps) == 1
    s = steps[0]
    assert len(s.joins) == 1
    j = s.joins[0]
    assert "id" in (j.expression or "")
    # Both sides of the JOIN keyed on id
    assert "left_t.id" in j.source_columns
    assert "right_t.id" in j.source_columns


def test_lineage_trace_multi_writer_returns_multiple_steps():
    sql_a = "INSERT INTO mart SELECT amount FROM src_one WHERE region = 'us'"
    sql_b = "INSERT INTO mart SELECT amount FROM src_two WHERE region = 'eu'"
    graph, raw = _build_graphs(("a.sql", sql_a), ("b.sql", sql_b))
    steps = lineage_trace(graph, raw, "mart", "amount")
    assert len(steps) == 2
    sources = {s.upstream_columns[0] for s in steps}
    assert sources == {"src_one.amount", "src_two.amount"}
    # Sorted by source_file
    assert steps[0].source_file == "a.sql"
    assert steps[1].source_file == "b.sql"


def test_lineage_trace_temp_view_predicates_roll_up():
    sql = (
        "CREATE TEMP VIEW paid_orders AS\n"
        "SELECT customer_id, amount FROM raw_orders WHERE status = 'paid';\n"
        "INSERT INTO mart SELECT customer_id, amount FROM paid_orders;"
    )
    graph, raw = _build_graphs(("v.sql", sql))
    steps = lineage_trace(graph, raw, "mart", "amount")
    assert len(steps) == 1
    s = steps[0]
    # The CREATE TEMP VIEW path is collapsed and its WHERE rolled up.
    assert "paid_orders" in s.via_temp_views
    assert any("status" in (f.expression or "") for f in s.filters), (
        f"expected WHERE rolled up via temp view; filters={s.filters}"
    )


def test_lineage_trace_source_column_returns_empty():
    sql = "INSERT INTO agg SELECT amount FROM raw_orders"
    graph, raw = _build_graphs(("s.sql", sql))
    # raw_orders.amount is a source column with no writers.
    assert lineage_trace(graph, raw, "raw_orders", "amount") == []


def test_lineage_trace_unknown_column_returns_empty():
    sql = "INSERT INTO agg SELECT amount FROM raw_orders"
    graph, raw = _build_graphs(("s.sql", sql))
    assert lineage_trace(graph, raw, "no_table", "no_col") == []


def test_lineage_trace_upstream_columns_excludes_synthetics():
    sql = (
        "INSERT INTO joined_t\n"
        "SELECT a.id FROM left_t a JOIN right_t b ON a.id = b.id WHERE a.id > 0"
    )
    graph, raw = _build_graphs(("j.sql", sql))
    steps = lineage_trace(graph, raw, "joined_t", "id")
    assert len(steps) == 1
    s = steps[0]
    # No __filter__/__joinkey__/__qualify__/__having__ in upstream_columns.
    for c in s.upstream_columns:
        assert "__" not in c.split(".")[-1], (
            f"synthetic column leaked into upstream: {c}"
        )


def test_lineage_trace_writes_dedup_when_expression_has_multiple_source_columns():
    # `a / NULLIF(b, 0)` references two source columns, so the parser emits
    # one writer edge per source. The Trace Step's `writes` should collapse
    # those into a single TraceStepWrite — `source_col` is not part of the
    # write's identity.
    sql = (
        "INSERT INTO mart\n"
        "SELECT a / NULLIF(b, 0) AS ratio FROM src"
    )
    graph, raw = _build_graphs(("r.sql", sql))
    steps = lineage_trace(graph, raw, "mart", "ratio")
    assert len(steps) == 1
    s = steps[0]
    assert len(s.writes) == 1, f"expected single write, got {len(s.writes)}: {s.writes}"
    assert s.writes[0].column_id == "mart.ratio"
    # Both source columns still show up as distinct upstream chips.
    assert set(s.upstream_columns) == {"src.a", "src.b"}


def test_lineage_trace_max_steps_truncates():
    blocks = [
        (f"f{i}.sql", f"INSERT INTO mart SELECT amount FROM src{i}")
        for i in range(5)
    ]
    graph, raw = _build_graphs(*blocks)
    steps = lineage_trace(graph, raw, "mart", "amount", max_steps=2)
    assert len(steps) == 2
    # Truncated deterministically: the lowest-sorting source files come first.
    assert steps[0].source_file == "f0.sql"
    assert steps[1].source_file == "f1.sql"


def test_lineage_trace_pyspark_kind_inferred_from_extension():
    # Build a tiny PySpark fixture: a write whose source is a real table.
    py = (
        "from pyspark.sql.functions import col\n"
        "df = spark.read.table('raw_orders').select('amount')\n"
        "df.write.saveAsTable('staging')\n"
    )
    rec = FileRecord(path="t.py", content=py, type="python", source_ref="t")
    result = build_graph_with_warnings([rec])
    steps = lineage_trace(result.graph, result.raw_graph, "staging", "amount")
    assert len(steps) == 1
    assert steps[0].kind == "pyspark"
    assert steps[0].source_file == "t.py"
