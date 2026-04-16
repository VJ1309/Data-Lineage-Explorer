from parsers.sql import parse_sql
from lineage.models import LineageEdge


def test_simple_select_passthrough():
    sql = "SELECT order_id, amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    assert "raw_orders.order_id" not in targets  # source table columns don't get edges
    # passthrough: output col linked to input col
    edge = next(e for e in edges if e.target_col == "result.order_id")
    assert edge.source_col == "raw_orders.order_id"
    assert edge.transform_type == "passthrough"


def test_aggregation_sum():
    sql = "SELECT customer_id, SUM(amount) AS total_revenue FROM raw_orders GROUP BY customer_id"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    agg_edge = next(e for e in edges if e.target_col == "result.total_revenue")
    assert agg_edge.source_col == "raw_orders.amount"
    assert agg_edge.transform_type == "aggregation"
    assert "SUM" in (agg_edge.expression or "")


def test_cte_resolution():
    sql = """
    WITH base AS (
        SELECT order_id, amount FROM raw_orders
    )
    SELECT order_id, amount FROM base
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    # Final output should trace back to raw_orders, not the CTE alias
    sources = {e.source_col for e in edges}
    assert any("raw_orders" in s for s in sources)


def test_cast_transform():
    sql = "SELECT CAST(amount AS STRING) AS amount_str FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    cast_edge = next(e for e in edges if e.target_col == "result.amount_str")
    assert cast_edge.transform_type == "cast"


def test_window_function():
    sql = "SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    win_edge = next((e for e in edges if e.target_col == "result.rn"), None)
    assert win_edge is not None
    assert win_edge.transform_type == "window"


def test_bad_sql_returns_empty_not_raises():
    edges = parse_sql("THIS IS NOT SQL !!!###", source_file="bad.sql", source_line=1)
    assert isinstance(edges, list)
    assert len(edges) == 0


def test_multi_statement_sql():
    sql = """
    INSERT INTO staging_orders
    SELECT order_id, amount FROM raw_orders;

    INSERT INTO agg_revenue
    SELECT customer_id, SUM(amount) AS total FROM staging_orders GROUP BY customer_id;
    """
    edges = parse_sql(sql, source_file="multi.sql", source_line=1)
    # Should have edges for both statements
    targets = {e.target_col for e in edges}
    assert "staging_orders.order_id" in targets
    assert "staging_orders.amount" in targets
    assert "agg_revenue.total" in targets
    assert "agg_revenue.customer_id" in targets
    # Check second statement references staging_orders as source
    agg_edge = next(e for e in edges if e.target_col == "agg_revenue.total")
    assert agg_edge.source_col == "staging_orders.amount"
    assert agg_edge.transform_type == "aggregation"


def test_schema_qualified_target():
    sql = "INSERT INTO analytics.revenue_summary SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    assert "analytics.revenue_summary.total" in targets
    assert "analytics.revenue_summary.customer_id" in targets


def test_schema_qualified_source():
    sql = "SELECT o.order_id, o.amount FROM staging.raw_orders o"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "staging.raw_orders.order_id" in sources
    assert "staging.raw_orders.amount" in sources


def test_schema_qualified_join():
    sql = """
    INSERT INTO analytics.mart_orders
    SELECT o.order_id, c.customer_name
    FROM staging.orders o
    JOIN staging.customers c ON o.customer_id = c.customer_id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "analytics.mart_orders.order_id" in targets
    assert "analytics.mart_orders.customer_name" in targets
    assert "staging.orders.order_id" in sources
    assert "staging.customers.customer_name" in sources


def test_multi_statement_with_empty_statements():
    sql = "SELECT a FROM t1; ; SELECT b FROM t2;"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    assert "result.a" in targets
    assert "result.b" in targets


def test_catalog_schema_table():
    sql = "SELECT col1 FROM my_catalog.my_schema.my_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "my_catalog.my_schema.my_table.col1" in sources


def test_databricks_sql_notebook():
    sql = """-- Databricks notebook source
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW stg AS
SELECT order_id, amount FROM raw_orders
-- COMMAND ----------
INSERT INTO agg_revenue
SELECT customer_id, SUM(amount) AS total
FROM stg
GROUP BY customer_id
"""
    edges = parse_sql(sql, source_file="nb.sql", source_line=None)
    targets = {e.target_col for e in edges}
    # stg is a temp view — its edges should be resolved through
    assert "stg.order_id" not in targets
    assert "stg.amount" not in targets
    # Final edges should trace back to raw_orders directly
    assert "agg_revenue.customer_id" in targets
    assert "agg_revenue.total" in targets
    agg_edge = next(e for e in edges if e.target_col == "agg_revenue.total")
    assert "raw_orders" in agg_edge.source_col


def test_databricks_sql_notebook_cell_index():
    sql = """-- Databricks notebook source
-- COMMAND ----------
SELECT a FROM t1
-- COMMAND ----------
-- just a comment
-- COMMAND ----------
SELECT b FROM t2
"""
    edges = parse_sql(sql, source_file="nb.sql", source_line=None)
    t1_edges = [e for e in edges if "t1" in e.source_col]
    t2_edges = [e for e in edges if "t2" in e.source_col]
    assert all(e.source_cell == 1 for e in t1_edges)
    assert all(e.source_cell == 3 for e in t2_edges)


def test_databricks_sql_notebook_skips_comment_cells():
    sql = """-- Databricks notebook source
-- COMMAND ----------
-- This is a comment-only cell
-- Another comment
-- COMMAND ----------
SELECT x FROM src
"""
    edges = parse_sql(sql, source_file="nb.sql", source_line=None)
    assert len(edges) == 1
    assert edges[0].source_col == "src.x"


def test_temp_view_resolution():
    """Temp views should be resolved through — not appear as separate tables."""
    sql = """
    CREATE OR REPLACE TEMP VIEW staging AS
    SELECT order_id, amount FROM raw_orders;

    INSERT INTO final_output
    SELECT order_id, amount FROM staging;
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    # Temp view edges should be resolved away
    assert not any("staging" in t for t in targets)
    # Final output should trace directly to raw_orders
    assert "final_output.order_id" in targets
    assert "final_output.amount" in targets
    assert "raw_orders.order_id" in sources
    assert "raw_orders.amount" in sources


def test_temp_view_chained():
    """Chained temp views should resolve all the way through."""
    sql = """-- Databricks notebook source
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW step1 AS
SELECT id, val FROM source_table
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW step2 AS
SELECT id, val FROM step1
-- COMMAND ----------
INSERT INTO final_table
SELECT id, val FROM step2
"""
    edges = parse_sql(sql, source_file="nb.sql", source_line=None)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    # Both temp views should be resolved away
    assert not any("step1" in t for t in targets)
    assert not any("step2" in t for t in targets)
    assert not any("step1" in s for s in sources)
    assert not any("step2" in s for s in sources)
    # Should trace all the way back to source_table
    assert "final_table.id" in targets
    assert "source_table.id" in sources


def test_struct_field_access_no_phantom_table():
    """Struct field access (struct_col.field) must not create a phantom table.

    When a column like `info.city` is referenced where `info` is a struct column
    on `customers` (not a table alias), SQLGlot parses it as Column(table='info',
    name='city').  The parser must fall back to the real source table instead of
    registering 'info' as a phantom table.
    """
    sql = """
    INSERT INTO summary
    SELECT info.city AS city, score
    FROM customers
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    # 'info' is a struct column on customers — must NOT appear as a table
    assert not any(s.startswith("info.") for s in sources), (
        f"Phantom table 'info' found in sources: {sources}"
    )
    # Both columns should be attributed to the real source table
    assert any("customers" in s for s in sources)


def test_multi_table_cte_no_phantom_table():
    """CTE that joins multiple tables must not create a phantom table.

    _resolve_ctes only handles single-table CTEs.  When a CTE joins two tables,
    the alias is absent from cte_map, so _resolve_table_hint used to return the
    raw alias string, creating a phantom node.  The parser must fall back to the
    first real source table instead.
    """
    sql = """
    WITH joined AS (
        SELECT a.id, b.val
        FROM table_a a
        JOIN table_b b ON a.id = b.id
    )
    INSERT INTO final_table
    SELECT id, val FROM joined
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # 'joined' must NOT appear as a phantom table
    assert not any(s.startswith("joined.") for s in sources), (
        f"Phantom table 'joined' found in sources: {sources}"
    )
    assert "final_table.id" in targets
    assert "final_table.val" in targets


def test_struct_field_fallback_is_approximate():
    """Struct field access falling back to default_table must produce an approximate edge.

    The adjacent unqualified column (score) must remain certain — this test
    proves the logic discriminates rather than marking everything approximate.
    """
    sql = """
    INSERT INTO summary
    SELECT info.city AS city, score
    FROM customers
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    city_edges = [e for e in edges if e.target_col.endswith(".city")]
    assert len(city_edges) == 1
    assert city_edges[0].confidence == "approximate", (
        f"Expected approximate, got {city_edges[0].confidence!r}"
    )
    # Adjacent unqualified column must remain certain
    score_edges = [e for e in edges if e.target_col.endswith(".score")]
    assert len(score_edges) == 1
    assert score_edges[0].confidence == "certain", (
        f"score column should be certain, got {score_edges[0].confidence!r}"
    )


def test_certain_table_alias_is_certain():
    """Column resolved via a known alias must produce a certain edge."""
    sql = "SELECT o.order_id FROM staging.orders o"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].confidence == "certain"


def test_certain_no_qualifier_is_certain():
    """Column with no table qualifier (default_table path) must be certain."""
    sql = "SELECT amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].confidence == "certain"


def test_chained_ctes_resolve_to_source():
    """CTE2 referencing CTE1 must resolve all the way to the base table."""
    sql = """
    WITH cte1 AS (SELECT id, val FROM source_table),
         cte2 AS (SELECT id, val FROM cte1)
    INSERT INTO final SELECT id, val FROM cte2
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert any("source_table" in s for s in sources), "must trace back to source_table"
    assert not any(s.startswith("cte1.") for s in sources), "cte1 must be resolved away"
    assert not any(s.startswith("cte2.") for s in sources), "cte2 must be resolved away"


def test_union_all_both_branches_produce_edges():
    """Both branches of UNION ALL must emit edges to the same target."""
    sql = """
    INSERT INTO combined
    SELECT id, val FROM table_a
    UNION ALL
    SELECT id, val FROM table_b
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "table_a.id" in sources, "first UNION branch missing"
    assert "table_b.id" in sources, "second UNION branch missing"
    assert "combined.id" in targets


def test_union_standalone_result():
    """UNION without INSERT uses 'result' as synthetic target."""
    sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "t1.a" in sources
    assert "t2.a" in sources
    assert any(t.startswith("result.") for t in targets), "standalone UNION must use 'result' target"


def test_subquery_in_from_traces_to_base_table():
    """Columns from an inline subquery must trace back to the base table, not 'subquery'."""
    sql = """
    INSERT INTO result
    SELECT id, val FROM (SELECT id, val FROM source_table) sub
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "source_table.id" in sources, "must trace through subquery to base table"
    assert "source_table.val" in sources
    assert "result.id" in targets
    assert "result.val" in targets
    assert not any(s.startswith("subquery.") for s in sources), "phantom 'subquery' table found"


def test_subquery_with_alias_join():
    """Subquery in JOIN must also trace through to its source table."""
    sql = """
    INSERT INTO result
    SELECT a.id, sub.metric
    FROM base_table a
    JOIN (SELECT id, SUM(val) AS metric FROM detail_table GROUP BY id) sub
      ON a.id = sub.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    assert "base_table.id" in sources
    assert "detail_table.val" in sources


def test_subquery_with_inner_join_no_leak():
    """Inner JOIN tables inside a subquery trace through to outer target after alias resolution."""
    sql = """
    INSERT INTO result
    SELECT sub.y
    FROM (SELECT a.x, b.y FROM t1 a JOIN t2 b ON a.id = b.id) sub
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # sub alias should be resolved away — real sources trace directly to result
    source_tables = {s.rsplit(".", 1)[0] for s in sources}
    assert "sub" not in source_tables, f"subquery alias 'sub' not resolved: {sources}"
    # t2.y should now trace directly to result.y (correct lineage after alias resolution)
    assert "t2.y" in sources
    assert "result.y" in targets


def test_subquery_alias_not_in_graph_nodes():
    """Subquery aliases must be resolved away — not appear as intermediate table nodes."""
    from lineage.engine import build_graph
    from lineage.models import FileRecord
    sql = "INSERT INTO result SELECT id, val FROM (SELECT id, val FROM source_table) sub"
    record = FileRecord(path="q.sql", content=sql, type="sql", source_ref="test")
    graph = build_graph([record])
    nodes = set(graph.nodes())
    node_tables = {n.rsplit(".", 1)[0] for n in nodes if "." in n}
    assert "sub" not in node_tables, f"subquery alias 'sub' appeared as a table node: {node_tables}"
    assert "source_table" in node_tables
    assert "result" in node_tables


def test_select_star_emits_wildcard_edge():
    """SELECT * must emit a source.* -> target.* wildcard edge, not silence."""
    sql = "INSERT INTO target SELECT * FROM source_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].source_col == "source_table.*"
    assert edges[0].target_col == "target.*"


def test_bad_sql_collects_warning():
    """SQLGlot parse failure must surface in _warnings when caller passes the list."""
    warnings_list: list[str] = []
    edges = parse_sql(
        "THIS IS NOT SQL !!!###",
        source_file="bad.sql",
        source_line=1,
        _warnings=warnings_list,
    )
    assert edges == []
    assert len(warnings_list) == 1
    assert warnings_list[0]  # non-empty error message
