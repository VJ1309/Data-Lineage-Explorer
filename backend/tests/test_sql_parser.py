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


def test_multi_table_cte_correct_source_attribution():
    """Joined CTE columns must trace back to the correct source tables.

    Previously all columns from a multi-source CTE were attributed to the first
    FROM table. After the fix, explicit table aliases (a.id → table_a, b.val →
    table_b) must be preserved through CTE resolution.
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
    assert "table_a.id" in sources, f"Expected table_a.id in sources: {sources}"
    assert "table_b.val" in sources, f"Expected table_b.val in sources: {sources}"
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


def test_create_table_as_with_cte():
    """CREATE TABLE AS WITH cte AS (...) SELECT must resolve CTE to base table."""
    sql = """
    CREATE TABLE output_table AS
    WITH base AS (SELECT col_a, col_b FROM source_table)
    SELECT col_a, col_b FROM base
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "output_table.col_a" in targets
    assert "output_table.col_b" in targets
    assert "source_table.col_a" in sources, "CTE 'base' must resolve to source_table"
    assert "source_table.col_b" in sources
    assert not any(s.startswith("base.") for s in sources), "CTE alias must not leak"


def test_create_temp_view_as_with_cte_consumer():
    """CREATE TEMP VIEW with CTE; downstream consumer must trace to base table."""
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW staging AS
    WITH raw AS (SELECT id, amount FROM orders)
    SELECT id, amount FROM raw;

    INSERT INTO summary SELECT id, amount FROM staging
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "summary.id" in targets
    assert "orders.id" in sources, "must trace through CTE and temp view to orders"
    assert not any(s.startswith("raw.") for s in sources), "CTE alias must not leak"
    assert not any(s.startswith("staging.") for s in sources), "temp view must not leak"


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


def test_temp_view_wildcard_named_chain():
    """Wildcard temp view consumed by named column must resolve to base table.

    Chain: real_table (SELECT *) -> view_a.* ; INSERT uses view_a.col
    The named-column reference must fall back to the wildcard entry in tv_sources.
    """
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW view_a AS
    SELECT * FROM real_table;

    INSERT INTO final_table SELECT col_x, col_y FROM view_a
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "final_table.col_x" in targets
    assert "final_table.col_y" in targets
    # view_a must be fully resolved — no temp view leakage
    assert not any("view_a" in s for s in sources), "temp view must not appear as source"
    # Must trace back to real_table (wildcard fallback)
    assert any("real_table" in s for s in sources), "must resolve through wildcard to real_table"


def test_temp_view_wildcard_chain_two_hops():
    """Two-hop wildcard chain: real -> view_a (SELECT *) -> view_b (SELECT *) -> final."""
    sql = """
    CREATE OR REPLACE TEMPORARY VIEW view_a AS SELECT * FROM real_table;
    CREATE OR REPLACE TEMPORARY VIEW view_b AS SELECT * FROM view_a;
    INSERT INTO final_table SELECT * FROM view_b
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "final_table.*" in targets
    assert not any("view_a" in s for s in sources), "view_a must not leak as source"
    assert not any("view_b" in s for s in sources), "view_b must not leak as source"
    assert "real_table.*" in sources, "must resolve two-hop wildcard chain to real_table"


def test_merge_into_matched_update_emits_edges():
    """MERGE ... WHEN MATCHED THEN UPDATE SET col = source.col must emit edges."""
    sql = """
    MERGE INTO target_table t
    USING source_table s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val, t.status = s.status
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "target_table.val" in targets, f"MERGE UPDATE target missing; targets={targets}"
    assert "target_table.status" in targets
    assert "source_table.val" in sources
    assert "source_table.status" in sources


def test_merge_into_not_matched_insert_emits_edges():
    """MERGE ... WHEN NOT MATCHED THEN INSERT (cols) VALUES (s.cols) must emit edges."""
    sql = """
    MERGE INTO target_table t
    USING source_table s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1)
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "target_table.id" in targets
    assert "target_table.val" in targets
    assert "source_table.id" in sources
    assert "source_table.val" in sources


def test_one_bad_statement_does_not_drop_good_statements():
    """A single malformed statement must not lose the surrounding good ones."""
    sql = """
    INSERT INTO good_table SELECT id FROM source_table;
    THIS IS NOT VALID SQL !!!###;
    INSERT INTO other_good SELECT name FROM source_table2;
    """
    warnings_list: list[str] = []
    edges = parse_sql(sql, source_file="mixed.sql", source_line=1,
                     _warnings=warnings_list)
    targets = {e.target_col for e in edges}
    assert "good_table.id" in targets, "first good statement lost"
    assert "other_good.name" in targets, "third good statement lost"
    assert warnings_list, "bad statement must surface a warning"


def test_unqualified_column_in_join_is_marked_unqualified():
    """Ambiguous bare column in a multi-source SELECT must set qualified=False."""
    sql = """
    INSERT INTO result
    SELECT id FROM table_a JOIN table_b ON table_a.id = table_b.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    result_edges = [e for e in edges if e.target_col == "result.id"]
    assert result_edges, "should emit at least one edge for result.id"
    assert all(e.qualified is False for e in result_edges), (
        "ambiguous column in JOIN must be flagged qualified=False"
    )


def test_qualified_column_in_join_stays_qualified():
    """Explicit table.col reference in a JOIN must stay qualified=True."""
    sql = """
    INSERT INTO result
    SELECT table_a.id FROM table_a JOIN table_b ON table_a.id = table_b.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    e = next(e for e in edges if e.target_col == "result.id")
    assert e.qualified is True


def test_single_source_unqualified_column_stays_qualified():
    """Unqualified column against a single FROM is unambiguous — keep qualified=True."""
    sql = "INSERT INTO result SELECT id FROM only_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    e = next(e for e in edges if e.target_col == "result.id")
    assert e.qualified is True


def test_lateral_view_explode_links_output_to_source_array():
    """LATERAL VIEW EXPLODE(t.items) e AS item — e.item must link back to orders.items."""
    sql = """
    INSERT INTO result
    SELECT t.id, e.item
    FROM orders t
    LATERAL VIEW EXPLODE(t.items) e AS item
    """
    edges = parse_sql(sql, source_file="l.sql", source_line=1)
    item_edges = [e for e in edges if e.target_col == "result.item"]
    assert item_edges, f"no edge for result.item; edges={[(e.source_col, e.target_col) for e in edges]}"
    sources = {e.source_col for e in item_edges}
    assert "orders.items" in sources, (
        f"exploded output must trace to orders.items; got {sources}"
    )


def test_lateral_view_posexplode_links_both_columns():
    """POSEXPLODE yields (pos, value) — value column must trace to the array source."""
    sql = """
    INSERT INTO result
    SELECT t.id, e.pos, e.val
    FROM orders t
    LATERAL VIEW POSEXPLODE(t.items) e AS pos, val
    """
    edges = parse_sql(sql, source_file="l.sql", source_line=1)
    val_sources = {e.source_col for e in edges if e.target_col == "result.val"}
    assert "orders.items" in val_sources, (
        f"posexplode val must trace to orders.items; got {val_sources}"
    )


def test_pivot_output_columns_trace_to_aggregated_source():
    """PIVOT SUM(amount) FOR category IN ('A' AS cat_a) — cat_a/cat_b must exist and trace to sales.amount."""
    sql = """
    INSERT INTO result
    SELECT cat_a, cat_b FROM (SELECT year, amount, category FROM sales) p
    PIVOT (SUM(amount) FOR category IN ('A' AS cat_a, 'B' AS cat_b))
    """
    edges = parse_sql(sql, source_file="p.sql", source_line=1)
    cat_a_sources = {e.source_col for e in edges if e.target_col == "result.cat_a"}
    cat_b_sources = {e.source_col for e in edges if e.target_col == "result.cat_b"}
    assert "sales.amount" in cat_a_sources, (
        f"result.cat_a must trace to sales.amount; got {cat_a_sources}"
    )
    assert "sales.amount" in cat_b_sources, (
        f"result.cat_b must trace to sales.amount; got {cat_b_sources}"
    )


def test_where_clause_emits_filter_edges():
    """WHERE active = 1 AND region = 'US' must emit filter edges to target.__filter__."""
    sql = """
    INSERT INTO result
    SELECT id FROM users WHERE active = 1 AND region = 'US'
    """
    edges = parse_sql(sql, source_file="w.sql", source_line=1)
    filter_edges = [e for e in edges if e.target_col == "result.__filter__"]
    assert filter_edges, f"no filter edges emitted; edges={[(e.source_col, e.target_col, e.transform_type) for e in edges]}"
    sources = {e.source_col for e in filter_edges}
    assert "users.active" in sources, f"missing users.active in filter sources; got {sources}"
    assert "users.region" in sources, f"missing users.region in filter sources; got {sources}"
    for e in filter_edges:
        assert e.transform_type == "filter", f"filter edges must have transform_type=filter; got {e.transform_type}"


def test_where_clause_without_columns_emits_no_filter_edge():
    """WHERE 1 = 1 (no column refs) must not emit phantom filter edges."""
    sql = "INSERT INTO result SELECT id FROM users WHERE 1 = 1"
    edges = parse_sql(sql, source_file="w.sql", source_line=1)
    assert not any(e.target_col == "result.__filter__" for e in edges)


def test_join_on_emits_joinkey_edges():
    """JOIN t ON t.id = s.id must emit joinkey edges from both sides to target.__joinkey__."""
    sql = """
    INSERT INTO result
    SELECT t.val FROM t JOIN s ON t.id = s.id
    """
    edges = parse_sql(sql, source_file="j.sql", source_line=1)
    jk_edges = [e for e in edges if e.target_col == "result.__joinkey__"]
    assert jk_edges, f"no join_key edges; edges={[(e.source_col, e.target_col) for e in edges]}"
    sources = {e.source_col for e in jk_edges}
    assert "t.id" in sources, f"missing t.id in joinkey sources; got {sources}"
    assert "s.id" in sources, f"missing s.id in joinkey sources; got {sources}"
    for e in jk_edges:
        assert e.transform_type == "join_key", f"joinkey edges must have transform_type=join_key; got {e.transform_type}"


def test_join_without_on_clause_emits_no_joinkey():
    """CROSS JOIN has no ON — must not emit phantom joinkey edges."""
    sql = "INSERT INTO result SELECT t.val FROM t CROSS JOIN s"
    edges = parse_sql(sql, source_file="j.sql", source_line=1)
    assert not any(e.target_col == "result.__joinkey__" for e in edges)


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


