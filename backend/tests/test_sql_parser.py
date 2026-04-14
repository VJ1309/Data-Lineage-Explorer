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
