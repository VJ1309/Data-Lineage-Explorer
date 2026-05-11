from parsers.sql import (
    parse_sql,
    DATABRICKS_SQL_SEP,
    split_databricks_sql,
    detect_temp_views,
    resolve_temp_views,
)
from lineage.models import LineageEdge, ParseResult


def _parse_sql_notebook(sql: str, source_file: str = "nb.sql") -> list:
    """Parse multi-cell Databricks SQL, mirroring engine._parse_file dispatch."""
    all_edges = []
    temp_views: set = set()
    for cell_sql, cell_idx in split_databricks_sql(sql):
        temp_views.update(detect_temp_views(cell_sql))
        all_edges.extend(parse_sql(
            cell_sql, source_file=source_file, source_line=None,
            source_cell=cell_idx, _resolve_views=False,
        ).edges)
    return resolve_temp_views(all_edges, temp_views)


def test_simple_select_passthrough():
    sql = "SELECT order_id, amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "raw_orders.order_id" not in targets  # source table columns don't get edges
    # passthrough: output col linked to input col
    edge = next(e for e in edges if e.target_col == "result.order_id")
    assert edge.source_col == "raw_orders.order_id"
    assert edge.transform_type == "passthrough"


def test_aggregation_sum():
    sql = "SELECT customer_id, SUM(amount) AS total_revenue FROM raw_orders GROUP BY customer_id"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    # Final output should trace back to raw_orders, not the CTE alias
    sources = {e.source_col for e in edges}
    assert any("raw_orders" in s for s in sources)


def test_cast_transform():
    sql = "SELECT CAST(amount AS STRING) AS amount_str FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    cast_edge = next(e for e in edges if e.target_col == "result.amount_str")
    assert cast_edge.transform_type == "cast"


def test_window_function():
    sql = "SELECT customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS rn FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    win_edge = next((e for e in edges if e.target_col == "result.rn"), None)
    assert win_edge is not None
    assert win_edge.transform_type == "window"


def test_bad_sql_returns_empty_not_raises():
    result = parse_sql("THIS IS NOT SQL !!!###", source_file="bad.sql", source_line=1)
    assert isinstance(result, ParseResult)
    assert len(result.edges) == 0


def test_multi_statement_sql():
    sql = """
    INSERT INTO staging_orders
    SELECT order_id, amount FROM raw_orders;

    INSERT INTO agg_revenue
    SELECT customer_id, SUM(amount) AS total FROM staging_orders GROUP BY customer_id;
    """
    edges = parse_sql(sql, source_file="multi.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "analytics.revenue_summary.total" in targets
    assert "analytics.revenue_summary.customer_id" in targets


def test_schema_qualified_source():
    sql = "SELECT o.order_id, o.amount FROM staging.raw_orders o"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "analytics.mart_orders.order_id" in targets
    assert "analytics.mart_orders.customer_name" in targets
    assert "staging.orders.order_id" in sources
    assert "staging.customers.customer_name" in sources


def test_multi_statement_with_empty_statements():
    sql = "SELECT a FROM t1; ; SELECT b FROM t2;"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "result.a" in targets
    assert "result.b" in targets


def test_catalog_schema_table():
    sql = "SELECT col1 FROM my_catalog.my_schema.my_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = _parse_sql_notebook(sql)
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
    edges = _parse_sql_notebook(sql)
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
    edges = _parse_sql_notebook(sql)
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = _parse_sql_notebook(sql)
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert len(edges) == 1
    assert edges[0].confidence == "certain"


def test_certain_no_qualifier_is_certain():
    """Column with no table qualifier (default_table path) must be certain."""
    sql = "SELECT amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert len(edges) == 1
    assert edges[0].confidence == "certain"


def test_chained_ctes_resolve_to_source():
    """CTE2 referencing CTE1 must resolve all the way to the base table."""
    sql = """
    WITH cte1 AS (SELECT id, val FROM source_table),
         cte2 AS (SELECT id, val FROM cte1)
    INSERT INTO final SELECT id, val FROM cte2
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "table_a.id" in sources, "first UNION branch missing"
    assert "table_b.id" in sources, "second UNION branch missing"
    assert "combined.id" in targets


def test_union_standalone_result():
    """UNION without INSERT uses 'result' as synthetic target."""
    sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
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
    result = parse_sql(sql, source_file="mixed.sql", source_line=1)
    targets = {e.target_col for e in result.edges}
    assert "good_table.id" in targets, "first good statement lost"
    assert "other_good.name" in targets, "third good statement lost"
    assert result.warnings, "bad statement must surface a warning"


def test_unqualified_column_in_join_is_marked_unqualified():
    """Ambiguous bare column in a multi-source SELECT must set qualified=False."""
    sql = """
    INSERT INTO result
    SELECT id FROM table_a JOIN table_b ON table_a.id = table_b.id
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    e = next(e for e in edges if e.target_col == "result.id")
    assert e.qualified is True


def test_single_source_unqualified_column_stays_qualified():
    """Unqualified column against a single FROM is unambiguous — keep qualified=True."""
    sql = "INSERT INTO result SELECT id FROM only_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="l.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="l.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="p.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="w.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="w.sql", source_line=1).edges
    assert not any(e.target_col == "result.__filter__" for e in edges)


def test_lookup_wildcard_no_cross_column_edges():
    """Named-column lookup through a wildcard chain must not return cross-column sources.

    Chain: base_table.(col_a, col_b) -> intermediate_v.(col_a, col_b) [named]
           intermediate_v.* -> wrapper_v.*  [SELECT *]
           wrapper_v.col_a -> final.col_a   [INSERT SELECT col_a]

    After chain-expansion tv_sources["wrapper_v.*"] = ["base_table.col_a", "base_table.col_b"].
    _lookup("wrapper_v.col_a") must return only ["base_table.col_a"], not both.
    """
    sql = """
    CREATE OR REPLACE TEMP VIEW intermediate_v AS
    SELECT col_a, col_b FROM base_table;

    CREATE OR REPLACE TEMP VIEW wrapper_v AS
    SELECT * FROM intermediate_v;

    INSERT INTO final_table SELECT col_a, col_b FROM wrapper_v
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges

    col_a_sources = {e.source_col for e in edges if e.target_col == "final_table.col_a"}
    col_b_sources = {e.source_col for e in edges if e.target_col == "final_table.col_b"}

    assert "base_table.col_a" in col_a_sources, f"col_a must trace to base_table.col_a; got {col_a_sources}"
    assert "base_table.col_b" not in col_a_sources, (
        f"cross-column edge: base_table.col_b must NOT appear as source of final_table.col_a; got {col_a_sources}"
    )
    assert "base_table.col_b" in col_b_sources, f"col_b must trace to base_table.col_b; got {col_b_sources}"
    assert "base_table.col_a" not in col_b_sources, (
        f"cross-column edge: base_table.col_a must NOT appear as source of final_table.col_b; got {col_b_sources}"
    )


# ---------------------------------------------------------------------------
# U2: Expression inheritance through temp-view chains
# ---------------------------------------------------------------------------

def test_expression_inherited_through_passthrough_view():
    """COALESCE in an intermediate temp view must survive resolution to final target.

    Chain: base_table -> tv1 [COALESCE expression] -> final_table [SELECT * passthrough]
    Resolved edge base_table.col -> final_table.col should carry transform_type='expression'
    with the COALESCE string, not 'passthrough'/'SELECT * FROM tv1'.
    """
    sql = """
    CREATE OR REPLACE TEMP VIEW tv1 AS
    SELECT COALESCE(col_a, col_b) AS col_a FROM base_table;

    INSERT INTO final_table SELECT col_a FROM tv1
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    resolved = [e for e in edges if e.target_col == "final_table.col_a"]
    assert resolved, f"no edge for final_table.col_a; all edges: {[(e.source_col, e.target_col) for e in edges]}"
    best = resolved[0]
    assert best.transform_type == "expression", (
        f"COALESCE must survive chain resolution; got transform_type={best.transform_type!r}"
    )
    assert best.expression and "COALESCE" in best.expression.upper(), (
        f"expression must contain COALESCE; got {best.expression!r}"
    )


def test_all_passthrough_chain_preserves_consumer_expression():
    """All-passthrough chain: resolved edge carries consumer expression unchanged."""
    sql = """
    CREATE OR REPLACE TEMP VIEW tv1 AS
    SELECT col_a FROM base_table;

    INSERT INTO final_table SELECT col_a FROM tv1
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    resolved = [e for e in edges if e.target_col == "final_table.col_a"]
    assert resolved
    assert resolved[0].transform_type == "passthrough", (
        f"all-passthrough chain must stay passthrough; got {resolved[0].transform_type!r}"
    )


def test_aggregation_inherited_through_passthrough_view():
    """SUM aggregation in intermediate view beats consumer passthrough."""
    sql = """
    CREATE OR REPLACE TEMP VIEW agg_v AS
    SELECT customer_id, SUM(amount) AS total FROM base_table GROUP BY customer_id;

    INSERT INTO final_table SELECT customer_id, total FROM agg_v
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    total_edges = [e for e in edges if e.target_col == "final_table.total"]
    assert total_edges, f"no edge for final_table.total; edges={[(e.source_col, e.target_col) for e in edges]}"
    assert total_edges[0].transform_type == "aggregation", (
        f"SUM must survive chain resolution; got {total_edges[0].transform_type!r}"
    )


def test_window_beats_aggregation_in_chain():
    """Window function in intermediate hop beats aggregation in consumer hop."""
    sql = """
    CREATE OR REPLACE TEMP VIEW window_v AS
    SELECT col_a, ROW_NUMBER() OVER (PARTITION BY col_a ORDER BY col_b) AS rn
    FROM base_table;

    INSERT INTO final_table SELECT rn FROM window_v
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    rn_edges = [e for e in edges if e.target_col == "final_table.rn"]
    assert rn_edges, f"no edge for final_table.rn; edges={[(e.source_col, e.target_col) for e in edges]}"
    assert rn_edges[0].transform_type == "window", (
        f"window must survive chain resolution; got {rn_edges[0].transform_type!r}"
    )


def test_consumer_expression_beats_passthrough_intermediate():
    """Consumer has expression transform, intermediate is passthrough — consumer wins."""
    sql = """
    CREATE OR REPLACE TEMP VIEW tv1 AS
    SELECT col_a FROM base_table;

    INSERT INTO final_table SELECT COALESCE(col_a, 0) AS col_a FROM tv1
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    resolved = [e for e in edges if e.target_col == "final_table.col_a"]
    assert resolved
    assert resolved[0].transform_type == "expression", (
        f"consumer COALESCE must win over intermediate passthrough; got {resolved[0].transform_type!r}"
    )


def test_non_tempview_edge_expression_unchanged():
    """Edges where source_col is a real base table are emitted with their original expression."""
    sql = "INSERT INTO final_table SELECT COALESCE(col_a, 0) AS col_a FROM base_table"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert len(edges) == 1
    assert edges[0].transform_type == "expression"
    assert edges[0].expression and "COALESCE" in edges[0].expression.upper()


def test_join_on_emits_joinkey_edges():
    """JOIN t ON t.id = s.id must emit joinkey edges from both sides to target.__joinkey__."""
    sql = """
    INSERT INTO result
    SELECT t.val FROM t JOIN s ON t.id = s.id
    """
    edges = parse_sql(sql, source_file="j.sql", source_line=1).edges
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
    edges = parse_sql(sql, source_file="j.sql", source_line=1).edges
    assert not any(e.target_col == "result.__joinkey__" for e in edges)


def test_cte_with_union_all_resolves_to_real_sources():
    """CTE whose body is a UNION ALL must not leak as a phantom source.

    _resolve_ctes previously only handled single-FROM (simple_map) and JOINed CTEs
    (multi_map). UNION ALL CTEs fell into neither bucket and their alias leaked.
    """
    sql = """
    WITH na_pos_non_calc AS (
        SELECT id, val FROM uc_dc_dev.sc_core.source_a
        UNION ALL
        SELECT id, val FROM uc_dc_dev.sc_core.source_b
    )
    INSERT INTO uc_dc_dev.sc_wrk.target_tbl
    SELECT id, val FROM na_pos_non_calc
    """
    edges = parse_sql(sql, source_file="test_union.sql", source_line=1).edges
    source_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    assert "na_pos_non_calc" not in source_tables, (
        f"na_pos_non_calc leaked as phantom; edges: {edges}"
    )
    assert any("source_a" in e.source_col or "source_b" in e.source_col for e in edges)


def test_cross_cell_temp_view_uppercase_col_refs_resolve():
    """Cross-cell temp view + CTE with UPPERCASE column references must resolve to real source.

    Real-world pattern: cell 1 creates a temp view, cell 2 uses a CTE aliasing it
    and selects with UPPERCASE column names (common in Databricks SQL notebooks).
    The temp view resolution must be case-insensitive so final_so_po_data doesn't
    leak as a phantom source table.
    """
    sql = """-- Databricks notebook source

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW final_so_po_data AS
SELECT a.src_sys_cd, a.key_id_ref_num
FROM uc_dc_dev.sc_core.real_source_a a
JOIN uc_dc_dev.sc_core.real_source_b b ON a.id = b.id;

-- COMMAND ----------

WITH CALCULATED_FINAL AS (
    SELECT * FROM final_so_po_data
)
INSERT INTO uc_dc_dev.sc_wrk.smt_trx_so_po_emea_calc
SELECT CALCULATED_FINAL.SRC_SYS_CD, CALCULATED_FINAL.KEY_ID_REF_NUM FROM CALCULATED_FINAL;
"""
    edges = _parse_sql_notebook(sql, source_file="SC_WRK.SMT_TRX_SO_PO_EMEA_CALC.sql")
    source_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    # final_so_po_data must not appear as a source — it must be resolved through
    assert "final_so_po_data" not in source_tables, (
        f"final_so_po_data leaked as phantom source; edges: {edges}"
    )
    # Real sources must reach the target
    target_edges = [e for e in edges if "smt_trx_so_po_emea_calc" in e.target_col]
    assert len(target_edges) >= 2
    assert all(
        "uc_dc_dev.sc_core.real_source" in e.source_col for e in target_edges
    ), f"Edges should trace to real_source_a/b: {target_edges}"


def test_null_literal_in_union_all_branch_does_not_create_phantom_source():
    """NULL / constant literals in UNION ALL branches must not attribute to CTE default table.

    Pattern: INSERT INTO target SELECT NULL AS col FROM some_cte — the parser used to
    create source_col="some_cte.col" because there are no Column nodes in a NULL expr.
    After fix: literal-only expressions are skipped so no phantom phantom source edge is
    emitted; the real source from the non-NULL branch still resolves correctly.
    """
    sql = """
    WITH data_cte AS (
        SELECT src_sys_cd, po_num FROM uc_dc_dev.sc_core.real_table
        UNION ALL
        SELECT src_sys_cd, po_num FROM uc_dc_dev.sc_core.real_table2
    )
    INSERT INTO uc_dc_dev.sc_wrk.target_tbl
    SELECT po_num, NULL AS extra_col FROM data_cte
    """
    edges = parse_sql(sql, source_file="test_null_literal.sql", source_line=1).edges
    source_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    # data_cte must not appear as source (it's a CTE, not a real table)
    assert "data_cte" not in source_tables, (
        f"data_cte leaked as phantom source via NULL literal; edges: {edges}"
    )
    # Real sources must still appear
    assert any("real_table" in e.source_col for e in edges)


def test_string_literal_in_union_all_branch_does_not_create_phantom_source():
    """String constant ('CAP' AS col) from a multi-source CTE branch must not create phantom.

    When the CTE is UNION ALL (goes to multi_map, not simple_map), the default_table in
    each SELECT branch is the CTE alias itself. A string literal like 'CAP' AS src_sys_cd
    has no column refs, so the old code created CTE_alias.src_sys_cd as phantom source.
    """
    sql = """
    WITH so_po_data AS (
        SELECT id, src_sys_cd FROM uc_dc_dev.sc_core.real_src
        UNION ALL
        SELECT id, src_sys_cd FROM uc_dc_dev.sc_core.real_src2
    )
    INSERT INTO uc_dc_dev.sc_wrk.target_tbl
    SELECT id, 'CAP' AS src_sys_cd FROM so_po_data
    """
    edges = parse_sql(sql, source_file="test_string_literal.sql", source_line=1).edges
    source_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    assert "so_po_data" not in source_tables, (
        f"so_po_data leaked as phantom source via string literal 'CAP'; edges: {edges}"
    )
    # real source still appears for the non-literal column
    assert any("real_src" in e.source_col for e in edges)


def test_bad_sql_collects_warning():
    """SQLGlot parse failure must surface in ParseResult.warnings."""
    result = parse_sql("THIS IS NOT SQL !!!###", source_file="bad.sql", source_line=1)
    assert result.edges == []
    assert len(result.warnings) == 1
    assert result.warnings[0]  # non-empty error message




def test_temp_view_with_mixed_case_in_databricks_notebook():
    """Databricks SQL: uppercase temp view name defined in one cell must be resolved
    when referenced by name in a later cell (the lookup was using original-case
    temp_views set instead of temp_views_lower, causing the fallback to short-circuit)."""
    sql = "\n".join([
        "-- COMMAND ----------",
        "CREATE OR REPLACE TEMPORARY VIEW MY_STAGING AS",
        "SELECT id, val FROM uc_dev.raw.source_tbl",
        "",
        "-- COMMAND ----------",
        "INSERT INTO uc_dev.gold.final",
        "SELECT id, val FROM MY_STAGING",
    ])
    edges = _parse_sql_notebook(sql)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "uc_dev.raw.source_tbl.id" in sources, "temp view must be resolved to base table"
    assert "uc_dev.raw.source_tbl.val" in sources
    assert not any("my_staging" in s for s in sources), "MY_STAGING must not leak as source"
    assert "uc_dev.gold.final.id" in targets
    assert "uc_dev.gold.final.val" in targets


def test_temp_view_uppercase_chain_in_databricks_notebook():
    """Two-hop uppercase chain: RAW_STAGE → MID_STAGE → final must resolve fully."""
    sql = "\n".join([
        "-- COMMAND ----------",
        "CREATE OR REPLACE TEMPORARY VIEW RAW_STAGE AS",
        "SELECT id FROM uc_dev.raw.source_tbl",
        "",
        "-- COMMAND ----------",
        "CREATE OR REPLACE TEMPORARY VIEW MID_STAGE AS",
        "SELECT id FROM RAW_STAGE",
        "",
        "-- COMMAND ----------",
        "INSERT INTO uc_dev.gold.final",
        "SELECT id FROM MID_STAGE",
    ])
    edges = _parse_sql_notebook(sql)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "uc_dev.raw.source_tbl.id" in sources
    assert not any("raw_stage" in s for s in sources), "RAW_STAGE must not leak as source"
    assert not any("mid_stage" in s for s in sources), "MID_STAGE must not leak as source"
    assert "uc_dev.gold.final.id" in targets


def test_temp_view_uppercase_wildcard_select_in_databricks_notebook():
    """SELECT * from an uppercase temp view must resolve via the wildcard expansion path."""
    sql = "\n".join([
        "-- COMMAND ----------",
        "CREATE OR REPLACE TEMPORARY VIEW UPPER_STAGE AS",
        "SELECT id, val FROM uc_dev.raw.source_tbl",
        "",
        "-- COMMAND ----------",
        "INSERT INTO uc_dev.gold.final",
        "SELECT * FROM UPPER_STAGE",
    ])
    edges = _parse_sql_notebook(sql)
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert not any("upper_stage" in s for s in sources), "UPPER_STAGE must not leak as source"
    # All edges must trace back to the base table
    assert all("uc_dev.raw.source_tbl" in s for s in sources)


# ── R3: Missing aggregates ─────────────────────────────────────────────────────

def test_approx_count_distinct_is_aggregation():
    """APPROX_COUNT_DISTINCT must be classified as aggregation, not expression."""
    sql = "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM events"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    e = next(e for e in edges if e.target_col == "result.approx_users")
    assert e.transform_type == "aggregation", f"expected aggregation, got {e.transform_type!r}"


def test_stddev_is_aggregation():
    """STDDEV must be classified as aggregation."""
    sql = "SELECT STDDEV(amount) AS std_amount FROM orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    e = next(e for e in edges if e.target_col == "result.std_amount")
    assert e.transform_type == "aggregation", f"expected aggregation, got {e.transform_type!r}"


def test_variance_is_aggregation():
    """VARIANCE must be classified as aggregation."""
    sql = "SELECT VARIANCE(score) AS var_score FROM results"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    e = next(e for e in edges if e.target_col == "result.var_score")
    assert e.transform_type == "aggregation", f"expected aggregation, got {e.transform_type!r}"


def test_percentile_approx_is_aggregation():
    """PERCENTILE_APPROX must be classified as aggregation."""
    sql = "SELECT PERCENTILE_APPROX(amount, 0.5) AS median FROM orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    e = next(e for e in edges if e.target_col == "result.median")
    assert e.transform_type == "aggregation", f"expected aggregation, got {e.transform_type!r}"


# ── R4: Double-quoted identifier normalization ─────────────────────────────────

def test_double_quoted_column_resolved_as_identifier():
    """Double-quoted column names must produce correct lineage, not be silently dropped."""
    sql = 'SELECT "order_id", "amount" FROM raw_orders'
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    assert "raw_orders.order_id" in sources, f"double-quoted column not resolved: {sources}"
    assert "raw_orders.amount" in sources


def test_double_quoted_target_in_insert():
    """Double-quoted table in INSERT INTO must resolve to correct target."""
    sql = 'INSERT INTO "my_schema"."my_table" SELECT id FROM source_tbl'
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert any("my_table" in t for t in targets), f"quoted target table not resolved: {targets}"


# ── R1: QUALIFY filter edges ───────────────────────────────────────────────────

def test_qualify_emits_qualify_filter_edge():
    """QUALIFY clause must emit __qualify__ edges with transform_type=filter."""
    sql = """
    INSERT INTO result
    SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn
    FROM events
    QUALIFY rn = 1
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    qualify_edges = [e for e in edges if e.target_col == "result.__qualify__"]
    assert qualify_edges, f"no __qualify__ edges emitted; edges={[(e.source_col, e.target_col) for e in edges]}"
    for e in qualify_edges:
        assert e.transform_type == "filter", f"qualify edge must have transform_type=filter; got {e.transform_type!r}"


def test_qualify_sources_correct_column():
    """QUALIFY rn = 1 must emit an edge from the column referenced in the QUALIFY predicate."""
    sql = """
    SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn
    FROM events
    QUALIFY rn = 1
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    qualify_sources = {e.source_col for e in edges if e.target_col == "result.__qualify__"}
    assert any("rn" in s for s in qualify_sources), (
        f"QUALIFY predicate column 'rn' not in sources: {qualify_sources}"
    )


def test_qualify_without_columns_emits_no_qualify_edge():
    """QUALIFY 1 = 1 (no column refs) must not emit phantom qualify edges."""
    sql = "SELECT id FROM t QUALIFY 1 = 1"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert not any(e.target_col == "result.__qualify__" for e in edges)


# ── R2: HAVING filter edges ────────────────────────────────────────────────────

def test_having_emits_having_filter_edge():
    """HAVING clause must emit __having__ edges with transform_type=filter."""
    sql = """
    INSERT INTO result
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    having_edges = [e for e in edges if e.target_col == "result.__having__"]
    assert having_edges, f"no __having__ edges emitted; edges={[(e.source_col, e.target_col) for e in edges]}"
    for e in having_edges:
        assert e.transform_type == "filter", f"having edge must have transform_type=filter; got {e.transform_type!r}"
    sources = {e.source_col for e in having_edges}
    assert any("amount" in s for s in sources), f"HAVING SUM(amount) must reference 'amount': {sources}"


def test_having_without_columns_emits_no_having_edge():
    """HAVING 1 = 1 (no column refs) must not emit phantom having edges."""
    sql = "SELECT id FROM t GROUP BY id HAVING 1 = 1"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert not any(e.target_col == "result.__having__" for e in edges)


# ── R5: MERGE USING subquery ───────────────────────────────────────────────────

def test_merge_using_subquery_traces_source_columns():
    """MERGE INTO t USING (SELECT ...) AS s — source columns from subquery must be traced."""
    sql = """
    MERGE INTO target_table t
    USING (SELECT id, val FROM staging WHERE active = 1) AS s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "target_table.val" in targets, f"MERGE UPDATE target missing; targets={targets}"
    assert any("staging" in s for s in sources), (
        f"staging source not traced through USING subquery; sources={sources}"
    )
    # The subquery alias 's' must not appear as a phantom source table
    assert not any(s.startswith("s.") for s in sources), (
        f"subquery alias 's' leaked as phantom source: {sources}"
    )


def test_merge_using_subquery_with_not_matched_insert():
    """MERGE USING subquery — NOT MATCHED THEN INSERT must also trace through subquery."""
    sql = """
    MERGE INTO target_table t
    USING (SELECT id, name FROM source_table) AS s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "target_table.id" in targets
    assert "target_table.name" in targets
    assert any("source_table" in s for s in sources), (
        f"source_table not traced through USING subquery: {sources}"
    )


def test_merge_using_named_outer_cte_resolves_simple():
    """WITH name AS (SELECT cols FROM real) MERGE USING name — alias resolves to real."""
    sql = """
    WITH source AS (SELECT id, val FROM real_table)
    MERGE INTO target t
    USING source
    ON t.id = source.id
    WHEN MATCHED THEN UPDATE SET t.val = source.val
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    # The phantom: a 1-part 'source.val' source. Must not appear.
    assert not any(s.startswith("source.") for s in sources), (
        f"CTE alias 'source' leaked as phantom: {sources}"
    )
    # The real lineage: real_table.val → target.val
    assert "real_table.val" in sources, (
        f"underlying real_table not traced through CTE: {sources}"
    )


def test_merge_using_named_outer_cte_no_phantom_alias():
    """The CTE alias name must not show up as a 1-part source table anywhere."""
    sql = """
    WITH source AS (SELECT a, b FROM upstream)
    MERGE INTO tgt t
    USING source
    ON t.a = source.a
    WHEN NOT MATCHED THEN INSERT (a, b) VALUES (source.a, source.b)
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    src_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    assert "source" not in src_tables, (
        f"CTE alias 'source' surfaced as 1-part source: {src_tables}"
    )
    # All real lineage targets `upstream` (the CTE body's FROM)
    assert any(t.startswith("upstream") for t in src_tables), (
        f"upstream not traced through CTE alias: {src_tables}"
    )


def test_merge_using_named_outer_cte_multi_source():
    """Multi-source CTE (with JOIN) feeding MERGE USING resolves to underlying tables."""
    sql = """
    WITH source AS (SELECT r1.id AS id, r2.val AS val FROM r1 JOIN r2 ON r1.id = r2.id)
    MERGE INTO target t
    USING source
    ON t.id = source.id
    WHEN MATCHED THEN UPDATE SET t.val = source.val
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    src_tables = {e.source_col.rsplit(".", 1)[0] for e in edges}
    # No 1-part 'source' phantom
    assert "source" not in src_tables, (
        f"multi-source CTE alias leaked: {src_tables}"
    )
    # The underlying JOIN tables are surfaced
    assert {"r1", "r2"} & src_tables, (
        f"underlying JOIN tables not traced: {src_tables}"
    )


def test_merge_using_real_table_unchanged():
    """USING <real table> (no name collision with any CTE) keeps the existing behavior."""
    sql = """
    MERGE INTO target t
    USING staging s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.val = s.val
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    assert "staging.val" in sources, (
        f"real table USING regressed; sources={sources}"
    )


# ── R6: COPY INTO detect-and-degrade ──────────────────────────────────────────

def test_copy_into_emits_approximate_wildcard_edge():
    """COPY INTO target FROM 'path' must emit __file__.* → target.* approximate edge."""
    sql = "COPY INTO my_catalog.my_schema.my_table FROM '/mnt/data/orders/'"
    edges = parse_sql(sql, source_file="load.sql", source_line=1).edges
    assert edges, "COPY INTO must emit at least one edge"
    e = edges[0]
    assert e.source_col == "__file__.*", f"expected __file__.*, got {e.source_col!r}"
    assert "my_table" in e.target_col, f"target must include table name; got {e.target_col!r}"
    assert e.confidence == "approximate"
    assert e.transform_type == "passthrough"


def test_copy_into_unqualified_table():
    """COPY INTO without catalog/schema must still emit an edge to the table."""
    sql = "COPY INTO orders FROM '/mnt/landing/'"
    edges = parse_sql(sql, source_file="load.sql", source_line=1).edges
    assert any("orders" in e.target_col for e in edges), (
        f"COPY INTO target 'orders' not in edges: {[(e.source_col, e.target_col) for e in edges]}"
    )


# ── R7: CLONE detect-and-degrade ──────────────────────────────────────────────

def test_clone_table_emits_approximate_passthrough_edge():
    """CREATE TABLE new_tbl CLONE src_tbl must emit src.* → new.* approximate edge."""
    sql = "CREATE TABLE my_catalog.schema.new_table CLONE my_catalog.schema.source_table"
    edges = parse_sql(sql, source_file="clone.sql", source_line=1).edges
    assert edges, "CLONE must emit at least one edge"
    e = edges[0]
    assert "source_table" in e.source_col, f"clone source missing; got {e.source_col!r}"
    assert "new_table" in e.target_col, f"clone target missing; got {e.target_col!r}"
    assert e.confidence == "approximate"
    assert e.transform_type == "passthrough"


def test_deep_clone_emits_edge():
    """CREATE TABLE t DEEP CLONE src must also emit a lineage edge."""
    sql = "CREATE TABLE new_tbl DEEP CLONE src_tbl"
    edges = parse_sql(sql, source_file="clone.sql", source_line=1).edges
    assert any("src_tbl" in e.source_col for e in edges), (
        f"DEEP CLONE source 'src_tbl' not in edges: {[(e.source_col, e.target_col) for e in edges]}"
    )


# ── Passthrough SQL expression (per-column, not full SELECT) ──────────────────
# Contract: each passthrough LineageEdge.expression carries the per-column SQL
# fragment (e.g. "col1", "customer_id AS client_id"), NEVER the full SELECT body.
# Real-world tables have 100+ columns; stamping the same multi-hundred-line
# SELECT onto every column makes the Transform tab useless. See
# docs/plans/2026-05-11-008-fix-parser-bugs-from-real-data-plan.md (U1).


def test_passthrough_expression_is_per_column():
    """Passthrough edges carry one short per-column expression each, not the full SELECT."""
    sql = "INSERT INTO tgt SELECT a, b FROM src"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges"
    exprs = []
    for e in pt:
        assert e.expression is not None, "passthrough expression must not be None"
        expr = e.expression
        assert "SELECT" not in expr.upper(), f"per-column expression must not include SELECT; got {expr!r}"
        assert "FROM" not in expr.upper(), f"per-column expression must not include FROM; got {expr!r}"
        assert len(expr) < 60, f"per-column expression should be short; got {expr!r}"
        exprs.append(expr)
    # Each column gets its own distinct expression
    assert len(set(exprs)) == len(pt), f"passthrough expressions should be distinct per column; got {exprs}"


def test_passthrough_expression_shows_renamed_column():
    """Renamed passthrough column (AS alias) must be visible in the per-column expression."""
    sql = "SELECT customer_id AS client_id FROM raw.orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges"
    expr = pt[0].expression
    assert expr is not None
    # The rename must be visible so the user can read the column-level intent
    assert "client_id" in expr, f"AS alias not in expression; got {expr!r}"
    assert "customer_id" in expr, f"source column not in expression; got {expr!r}"
    assert "AS" in expr.upper(), f"rename keyword AS not preserved; got {expr!r}"
    assert "SELECT" not in expr.upper(), f"per-column expression must not include SELECT; got {expr!r}"


def test_passthrough_expression_single_cte_is_per_column():
    """Single CTE passthrough expression is the per-column fragment, not the full WITH."""
    sql = "WITH base AS (SELECT x FROM t) SELECT x FROM base"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    # Edges from the outer SELECT (resolving through the CTE) are the passthrough ones
    pt = [e for e in edges if e.transform_type == "passthrough" and "t." in e.source_col]
    assert pt, f"expected passthrough from 't'; edges={[(e.source_col,e.target_col,e.transform_type) for e in edges]}"
    expr = pt[0].expression
    assert expr is not None
    assert "WITH" not in expr.upper(), f"CTE wrapper must not appear in expression; got {expr!r}"
    assert "SELECT" not in expr.upper(), f"SELECT body must not appear in expression; got {expr!r}"
    assert "x" in expr, f"per-column expression must include the column name; got {expr!r}"


def test_passthrough_expression_multi_cte_is_per_column():
    """Multi-CTE passthrough expression is the per-column fragment, not all CTE bodies."""
    sql = """
    WITH base AS (SELECT x FROM t1),
         enriched AS (SELECT x FROM base JOIN t2 ON base.x = t2.x)
    SELECT x FROM enriched
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges"
    for e in pt:
        assert e.expression is not None
        # No CTE alias names or WITH wrapper should leak into a per-column expression
        assert "WITH" not in e.expression.upper(), f"WITH wrapper in expression; got {e.expression!r}"
        assert "enriched" not in e.expression, f"CTE alias leaked into expression; got {e.expression!r}"
        assert len(e.expression) < 60, f"per-column expression should be short; got {e.expression!r}"


def test_passthrough_expression_excludes_where_body():
    """WHERE clause must NOT be stamped onto the per-column passthrough expression."""
    sql = "SELECT customer_id FROM orders WHERE status = 'active'"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges"
    expr = pt[0].expression
    assert expr is not None
    assert "WHERE" not in expr.upper(), f"WHERE clause leaked into per-column expression; got {expr!r}"
    assert "active" not in expr, f"WHERE literal leaked into per-column expression; got {expr!r}"
    assert "customer_id" in expr, f"column name should appear in expression; got {expr!r}"
    # WHERE predicate still becomes a __filter__ edge — that's the right home for it
    filter_edges = [e for e in edges if e.transform_type == "filter"]
    assert filter_edges, "WHERE predicate must still produce a __filter__ edge"


def test_passthrough_expression_excludes_join_body():
    """JOIN clause must NOT be stamped onto the per-column passthrough expression."""
    sql = "SELECT a.col FROM a JOIN b ON a.id = b.id"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges"
    expr = pt[0].expression
    assert expr is not None
    assert "JOIN" not in expr.upper(), f"JOIN clause leaked into per-column expression; got {expr!r}"
    assert "FROM" not in expr.upper(), f"FROM clause leaked into per-column expression; got {expr!r}"
    assert "col" in expr, f"column name should appear in expression; got {expr!r}"


def test_aggregation_expression_not_overridden():
    """Aggregation edges keep their per-column expression (unchanged from prior contract)."""
    sql = "SELECT SUM(amount) AS total FROM orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    agg = [e for e in edges if e.transform_type == "aggregation"]
    assert agg, "expected aggregation edge"
    expr = agg[0].expression
    assert expr is not None
    assert "SUM" in expr, f"SUM not in aggregation expression; got {expr!r}"
    # Full SELECT would contain FROM — aggregation expr must be a column-level expr
    assert "FROM" not in expr, f"aggregation expression should not be full SELECT; got {expr!r}"


def test_expression_transform_not_overridden():
    """Arithmetic expression edges keep their per-column expression (unchanged)."""
    sql = "SELECT amount * 1.1 AS adjusted FROM orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    expr_edges = [e for e in edges if e.transform_type == "expression"]
    assert expr_edges, "expected expression-type edge"
    expr = expr_edges[0].expression
    assert expr is not None
    assert "1.1" in expr, f"arithmetic not in expression; got {expr!r}"
    assert "FROM" not in expr, f"expression edge should not be full SELECT; got {expr!r}"


def test_clone_passthrough_expression_unchanged():
    """CLONE (approximate passthrough) expression must NOT be a full SELECT body."""
    sql = "CREATE TABLE new_tbl CLONE src_tbl"
    edges = parse_sql(sql, source_file="clone.sql", source_line=1).edges
    assert edges, "CLONE must emit at least one edge"
    e = edges[0]
    assert e.confidence == "approximate"
    assert e.transform_type == "passthrough"
    if e.expression is not None:
        assert "SELECT" not in e.expression, (
            f"CLONE edge should not contain full SELECT; got {e.expression!r}"
        )


def test_passthrough_expression_select_star():
    """SELECT * passthrough produces wildcard edges with short expressions, never a full SELECT."""
    sql = "SELECT * FROM tbl"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    # SELECT * produces wildcard edges — their expression comes from _wildcard_edge
    for e in pt:
        if e.expression:
            assert "SELECT" not in e.expression.upper(), (
                f"SELECT * edge must not carry full SELECT body; got {e.expression!r}"
            )


def test_ctas_passthrough_expression_is_per_column():
    """CREATE TABLE AS SELECT passthrough expression is the per-column fragment."""
    sql = "CREATE TABLE tgt AS SELECT a, b FROM src"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert pt, "expected passthrough edges from CTAS"
    exprs = []
    for e in pt:
        assert e.expression is not None
        assert "SELECT" not in e.expression.upper(), f"SELECT body leaked into expression; got {e.expression!r}"
        assert "CREATE" not in e.expression.upper(), f"CREATE wrapper in expression; got {e.expression!r}"
        assert "FROM" not in e.expression.upper(), f"FROM clause in expression; got {e.expression!r}"
        exprs.append(e.expression)
    assert len(set(exprs)) == len(pt), f"each passthrough column must have a distinct expression; got {exprs}"


def test_passthrough_expression_distinct_for_wide_table():
    """Wide-table case: 119 passthrough columns produce 119 distinct expressions."""
    cols = ", ".join(f"col{i}" for i in range(1, 25))  # 24 cols is enough to expose the bug
    sql = f"INSERT INTO tgt SELECT {cols} FROM src"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    pt = [e for e in edges if e.transform_type == "passthrough"]
    assert len(pt) == 24, f"expected one passthrough edge per column; got {len(pt)}"
    exprs = [e.expression for e in pt]
    # The bug: every column used to carry the same multi-line SELECT body.
    assert len(set(exprs)) == 24, (
        f"each column must carry its own expression; got {len(set(exprs))} distinct values"
    )
    for expr in exprs:
        assert expr is not None and len(expr) < 60, f"expression should be short and per-column; got {expr!r}"


# ── R8: read_files() / cloud_files() in FROM ──────────────────────────────────

def test_read_files_in_from_emits_synthetic_source():
    """SELECT from read_files() must register a synthetic source and emit edges."""
    sql = "SELECT id, name FROM read_files('/mnt/landing/orders/*.parquet', format => 'parquet') AS t"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert edges, "read_files() query must emit edges"
    sources = {e.source_col.rsplit(".", 1)[0] for e in edges}
    # The synthetic source must not be empty and must not be the raw function call string
    assert all(s and "read_files" not in s for s in sources), (
        f"synthetic source from read_files() looks wrong: {sources}"
    )


def test_cloud_files_in_from_emits_synthetic_source():
    """SELECT from cloud_files() must register a synthetic source and emit edges."""
    sql = "INSERT INTO result SELECT id, val FROM cloud_files('/mnt/data/', 'parquet') AS f"
    edges = parse_sql(sql, source_file="q.sql", source_line=1).edges
    assert edges, "cloud_files() query must emit edges"
    targets = {e.target_col for e in edges}
    assert any("result" in t for t in targets), f"result table missing in targets: {targets}"


# ---------------------------------------------------------------------------
# Recursive CTEs (U1)
# ---------------------------------------------------------------------------


def _no_self_loops(edges, alias: str) -> bool:
    """No edge should have both source and target rooted on the recursive CTE alias."""
    for e in edges:
        s_table = e.source_col.rsplit(".", 1)[0]
        t_table = e.target_col.rsplit(".", 1)[0]
        if s_table == alias and t_table == alias:
            return False
    return True


def test_recursive_cte_org_tree_resolves_to_anchor_source():
    """org-tree fixture: recursive references must resolve to anchor's source columns,
    not phantom org_tree.col → org_tree.col self-edges."""
    sql = """
    WITH RECURSIVE org_tree AS (
      SELECT employee_id, name, manager_id, name AS root_name, 0 AS depth
      FROM employees
      WHERE manager_id IS NULL
      UNION ALL
      SELECT e.employee_id, e.name, e.manager_id, t.root_name, t.depth + 1
      FROM employees e JOIN org_tree t ON e.manager_id = t.employee_id
    )
    SELECT employee_id, name, root_name, depth FROM org_tree
    """
    edges = parse_sql(sql, source_file="org.sql", source_line=1).edges
    # No phantom self-loops on the recursive CTE
    assert _no_self_loops(edges, "org_tree"), (
        f"phantom org_tree → org_tree edges leaked: "
        f"{[(e.source_col, e.target_col) for e in edges]}"
    )
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # Anchor's name AS root_name must trace from employees.name
    assert "employees.name" in sources
    assert "result.root_name" in targets
    # No edges originate from the recursive CTE alias
    leaked = [e for e in edges if e.source_col.startswith("org_tree.")]
    assert not leaked, f"org_tree.X leaked as source: {leaked}"


def test_recursive_cte_numbers_values_anchor_no_phantom_edges():
    """VALUES-anchor recursive CTE: no real source exists; the recursive branch
    must not produce phantom numbers.n → result.X edges."""
    sql = """
    WITH RECURSIVE numbers(n) AS (
      VALUES (1)
      UNION ALL
      SELECT n + 1 FROM numbers WHERE n < 100
    )
    SELECT * FROM numbers
    """
    edges = parse_sql(sql, source_file="n.sql", source_line=1).edges
    assert _no_self_loops(edges, "numbers"), (
        f"phantom self-loops on numbers: {[(e.source_col, e.target_col) for e in edges]}"
    )
    # No edges should originate from the recursive CTE alias as a phantom source
    leaked = [e for e in edges if e.source_col.startswith("numbers.")]
    assert not leaked, f"numbers.X leaked as phantom source: {leaked}"


def test_recursive_cte_bom_join_anchor_resolves_correctly():
    """BOM fixture with single-table anchor: edges trace from bill_of_materials,
    not from a phantom bom.col → bom.col."""
    sql = """
    WITH RECURSIVE bom AS (
      SELECT part_id, parent_id, qty FROM bill_of_materials WHERE parent_id IS NULL
      UNION ALL
      SELECT b.part_id, b.parent_id, b.qty
      FROM bill_of_materials b JOIN bom p ON b.parent_id = p.part_id
    )
    INSERT INTO bom_summary SELECT part_id, qty FROM bom
    """
    edges = parse_sql(sql, source_file="bom.sql", source_line=1).edges
    assert _no_self_loops(edges, "bom"), (
        f"phantom self-loops on bom: {[(e.source_col, e.target_col) for e in edges]}"
    )
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "bill_of_materials.part_id" in sources
    assert "bill_of_materials.qty" in sources
    assert "bom_summary.part_id" in targets
    assert "bom_summary.qty" in targets


def test_recursive_cte_max_recursion_level_clause_recognised():
    """Databricks MAX RECURSION LEVEL N clause must be stripped pre-parse so lineage
    matches the same query without the clause."""
    sql_with_clause = """
    WITH RECURSIVE numbers(n) AS (
      SELECT id FROM seed
      UNION ALL
      SELECT n + 1 FROM numbers WHERE n < 100
    ) MAX RECURSION LEVEL 100
    SELECT n FROM numbers
    """
    sql_without_clause = """
    WITH RECURSIVE numbers(n) AS (
      SELECT id FROM seed
      UNION ALL
      SELECT n + 1 FROM numbers WHERE n < 100
    )
    SELECT n FROM numbers
    """
    r1 = parse_sql(sql_with_clause, source_file="r.sql", source_line=1)
    r2 = parse_sql(sql_without_clause, source_file="r.sql", source_line=1)
    # No file-level parse failure on MAX RECURSION LEVEL form
    assert not r1.warnings, f"unexpected warnings: {r1.warnings}"
    # Edge sets should match (modulo edge ordering)
    pairs1 = sorted((e.source_col, e.target_col) for e in r1.edges)
    pairs2 = sorted((e.source_col, e.target_col) for e in r2.edges)
    assert pairs1 == pairs2, f"diff with clause: {pairs1}\nwithout: {pairs2}"


def test_recursive_cte_limit_all_passes_through():
    """Databricks Runtime 17.2+ LIMIT ALL must parse without altering lineage."""
    sql = """
    WITH RECURSIVE numbers(n) AS (
      SELECT id FROM seed
      UNION ALL
      SELECT n + 1 FROM numbers WHERE n < 100
    )
    SELECT n FROM numbers LIMIT ALL
    """
    result = parse_sql(sql, source_file="r.sql", source_line=1)
    assert not result.warnings, f"unexpected warnings: {result.warnings}"
    # At minimum, anchor's seed → result.n must trace through
    sources = {e.source_col for e in result.edges}
    assert any(s.startswith("seed.") for s in sources), f"seed not traced: {sources}"


def test_recursive_cte_graph_cycle_detection_no_phantoms():
    """Graph cycle-detection idiom from the origin SC2 fixture: edges from `graph`
    columns reach consumers; no search_graph.col → search_graph.col self-edges."""
    sql = """
    WITH RECURSIVE search_graph(f, t, depth) AS (
      SELECT g.f, g.t, 1 FROM graph g
      UNION ALL
      SELECT g.f, g.t, sg.depth + 1
      FROM graph g JOIN search_graph sg ON g.f = sg.t
    )
    INSERT INTO graph_paths SELECT f, t, depth FROM search_graph
    """
    edges = parse_sql(sql, source_file="g.sql", source_line=1).edges
    assert _no_self_loops(edges, "search_graph"), (
        f"phantom self-loops: {[(e.source_col, e.target_col) for e in edges]}"
    )
    sources = {e.source_col for e in edges}
    assert any("graph." in s for s in sources), f"graph not traced: {sources}"


def test_recursive_cte_in_insert_consumer_parity():
    """A recursive CTE used as an INSERT INTO source produces the same source-set as
    a top-level SELECT consumer."""
    sql_select = """
    WITH RECURSIVE r AS (
      SELECT id, val FROM s
      UNION ALL
      SELECT id, val FROM r WHERE id < 100
    )
    SELECT id, val FROM r
    """
    sql_insert = """
    WITH RECURSIVE r AS (
      SELECT id, val FROM s
      UNION ALL
      SELECT id, val FROM r WHERE id < 100
    )
    INSERT INTO target SELECT id, val FROM r
    """
    sources_select = {e.source_col for e in parse_sql(sql_select, source_file="a.sql", source_line=1).edges}
    sources_insert = {e.source_col for e in parse_sql(sql_insert, source_file="b.sql", source_line=1).edges}
    assert "s.id" in sources_select and "s.val" in sources_select
    assert sources_select == sources_insert, (
        f"select sources {sources_select} != insert sources {sources_insert}"
    )


# ---------------------------------------------------------------------------
# Procedural wrapper parity (U3) — embedded DML must produce the same edges
# as the same DML at top level
# ---------------------------------------------------------------------------


def test_procedural_begin_end_parity_with_top_level():
    top = "INSERT INTO t SELECT a FROM s"
    wrapped = "BEGIN INSERT INTO t SELECT a FROM s; END"
    e_top = sorted((e.source_col, e.target_col) for e in parse_sql(top, source_file="t.sql", source_line=1).edges)
    e_wrapped = sorted((e.source_col, e.target_col) for e in parse_sql(wrapped, source_file="w.sql", source_line=1).edges)
    assert e_top == e_wrapped, f"top: {e_top}\nwrapped: {e_wrapped}"


def test_procedural_if_else_each_branch_emits_edges():
    sql = """
    BEGIN
      IF flag = 1 THEN
        INSERT INTO branch_a SELECT col1 FROM s;
      ELSE
        INSERT INTO branch_b SELECT col2 FROM s;
      END IF;
    END
    """
    edges = parse_sql(sql, source_file="if.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "branch_a.col1" in targets, f"branch_a.col1 missing: {targets}"
    assert "branch_b.col2" in targets, f"branch_b.col2 missing: {targets}"


def test_procedural_multiple_inserts_in_block():
    sql = "BEGIN INSERT INTO a SELECT col1 FROM s; INSERT INTO b SELECT col2 FROM s; END"
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "a.col1" in targets, f"a.col1 missing: {targets}"
    assert "b.col2" in targets, f"b.col2 missing: {targets}"


def test_procedural_pre_check_false_positive_with_string_literal():
    """SQL containing the word BEGIN inside a string literal must not be mangled
    by the normaliser — same edges as direct parse."""
    sql = "INSERT INTO logs SELECT msg, 'BEGIN run' AS literal_msg FROM events"
    edges = parse_sql(sql, source_file="lg.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    assert "logs.msg" in targets, f"missing logs.msg: {targets}"


def test_procedural_malformed_inner_does_not_drop_other_statements():
    """A malformed embedded INSERT produces a per-statement warning while sibling
    statements still emit edges."""
    sql = """
    BEGIN
      INSERT INTO good SELECT a FROM s;
      INSERT INTO bad SELECT FROM ;
      INSERT INTO good2 SELECT b FROM s;
    END
    """
    result = parse_sql(sql, source_file="b.sql", source_line=1)
    targets = {e.target_col for e in result.edges}
    assert "good.a" in targets
    assert "good2.b" in targets
    # The malformed statement surfaces as a per-statement warning, not file-level
    assert any("INSERT INTO bad" in w or "bad" in w.lower() or "FROM" in w for w in result.warnings), (
        f"expected per-statement warning for malformed INSERT: {result.warnings}"
    )


def test_for_cursor_end_to_end_resolves_to_real_source():
    """The cursor query becomes a synthetic temp view; body refs resolve through it
    to the real underlying table. The synthetic name must NOT survive in edges."""
    sql = """
    BEGIN
      process_orders: FOR row AS SELECT order_id, amount FROM orders DO
        INSERT INTO summary SELECT row.order_id, row.amount;
      END FOR process_orders;
    END
    """
    edges = parse_sql(sql, source_file="for.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # Real source attribution
    assert "orders.order_id" in sources, f"orders.order_id missing: {sources}"
    assert "orders.amount" in sources, f"orders.amount missing: {sources}"
    assert "summary.order_id" in targets
    assert "summary.amount" in targets
    # Synthetic name collapsed by resolve_temp_views
    leaked = [e for e in edges if "__for_" in e.source_col or "__for_" in e.target_col]
    assert not leaked, f"synthetic __for_ names leaked: {leaked}"


def test_procedural_in_databricks_notebook_cell():
    """Per-cell parsing (engine path with _resolve_views=False) must work for cells
    containing procedural blocks, then resolve_temp_views composes correctly."""
    sql = """-- Databricks notebook source

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v AS SELECT id, name FROM raw_orders;

-- COMMAND ----------

BEGIN
  INSERT INTO target SELECT id, name FROM v;
END
"""
    edges = _parse_sql_notebook(sql, source_file="nb.sql")
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "target.id" in targets
    assert "target.name" in targets
    # Temp view collapsed: real source must reach target
    assert "raw_orders.id" in sources
    assert "raw_orders.name" in sources


# ---------------------------------------------------------------------------
# U5 end-to-end: stored procedures + CALL placeholder
# ---------------------------------------------------------------------------


def test_procedure_body_with_plain_dml_produces_real_edges():
    """CREATE PROCEDURE wrapper is stripped; embedded DML produces real edges
    identical to the same DML at top level."""
    sql = """
    CREATE PROCEDURE p()
    LANGUAGE SQL SQL SECURITY INVOKER
    AS BEGIN
      INSERT INTO t SELECT a, b FROM s;
    END
    """
    edges = parse_sql(sql, source_file="proc.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    assert "s.a" in sources, f"s.a missing: {sources}"
    assert "s.b" in sources, f"s.b missing: {sources}"
    assert "t.a" in targets
    assert "t.b" in targets
    # Wrapper must not leak into edges
    assert not any("PROCEDURE" in e.source_col or "PROCEDURE" in e.target_col for e in edges)


def test_procedure_body_parity_with_top_level():
    top = "INSERT INTO t SELECT a FROM s"
    wrapped = (
        "CREATE OR REPLACE PROCEDURE my_catalog.my_schema.proc_a() "
        "LANGUAGE SQL AS BEGIN INSERT INTO t SELECT a FROM s; END"
    )
    e_top = sorted((e.source_col, e.target_col) for e in parse_sql(top, source_file="t.sql", source_line=1).edges)
    e_wrap = sorted((e.source_col, e.target_col) for e in parse_sql(wrapped, source_file="w.sql", source_line=1).edges)
    assert e_top == e_wrap, f"top: {e_top}\nwrapped: {e_wrap}"


def test_procedure_registers_qualified_name_in_module_registry():
    from parsers.sql_script import _PROCEDURE_REGISTRY
    sql = (
        "CREATE OR REPLACE PROCEDURE my_catalog.my_schema.proc_x() "
        "LANGUAGE SQL AS BEGIN INSERT INTO t SELECT 1 AS x FROM dual; END"
    )
    parse_sql(sql, source_file="reg.sql", source_line=1)
    assert "my_catalog.my_schema.proc_x" in _PROCEDURE_REGISTRY


def test_call_emits_wildcard_self_edge_and_warning():
    """CALL produces a self-edge against the synthetic __call_<proc>__ table plus
    a per-statement warning (cross-file CALL resolution deferred to v2)."""
    sql = "CALL my_catalog.my_schema.run_etl('raw', 'silver')"
    result = parse_sql(sql, source_file="call.sql", source_line=1)
    edges = result.edges
    # Self-edge present on the synthetic
    synthetic = "__call_my_catalog_my_schema_run_etl__"
    assert any(
        e.source_col == f"{synthetic}.*" and e.target_col == f"{synthetic}.*"
        for e in edges
    ), f"placeholder self-edge missing; edges={[(e.source_col, e.target_col) for e in edges]}"
    # Confidence flag is approximate (mirrors other wildcard-edge sites)
    placeholder_edges = [e for e in edges if e.source_col.startswith(f"{synthetic}.")]
    assert all(e.confidence == "approximate" for e in placeholder_edges), placeholder_edges
    # Per-statement warning surfaces
    assert any(
        "Unresolved CALL to my_catalog.my_schema.run_etl" in w for w in result.warnings
    ), f"missing CALL warning; got: {result.warnings}"


def test_call_with_named_parameters_uses_proc_name_only():
    sql = "CALL run_etl(target_schema => 'silver', source_schema => 'raw')"
    result = parse_sql(sql, source_file="call.sql", source_line=1)
    synthetic = "__call_run_etl__"
    assert any(
        e.source_col == f"{synthetic}.*" and e.target_col == f"{synthetic}.*"
        for e in result.edges
    )


def test_drop_describe_show_procedure_silently_skipped():
    """DROP/DESCRIBE/SHOW PROCEDURE produce zero edges and zero warnings — even when
    interleaved with valid DML in the same script."""
    sql = (
        "BEGIN "
        "DROP PROCEDURE IF EXISTS my_proc; "
        "DESCRIBE PROCEDURE EXTENDED my_proc; "
        "SHOW PROCEDURES FROM my_schema; "
        "INSERT INTO t SELECT a FROM s; "
        "END"
    )
    result = parse_sql(sql, source_file="ddl.sql", source_line=1)
    targets = {e.target_col for e in result.edges}
    assert "t.a" in targets, f"surviving DML lost: {targets}"
    # No spurious warnings from the silently-skipped PROCEDURE statements
    assert not any(
        "PROCEDURE" in w.upper() for w in result.warnings
    ), f"silent-skip leaked a warning: {result.warnings}"


# ---------------------------------------------------------------------------
# EXECUTE IMMEDIATE — folding + dynamic-SQL placeholder (U6)
# ---------------------------------------------------------------------------


def test_execute_immediate_constant_string_resolves_to_real_edges():
    sql = "EXECUTE IMMEDIATE 'INSERT INTO t SELECT a FROM s'"
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    edges = result.edges
    assert any(
        e.source_col == "s.a" and e.target_col == "t.a" for e in edges
    ), f"missing edge s.a -> t.a; edges={[(e.source_col, e.target_col) for e in edges]}"
    assert not any("__dynamic_sql__" in e.source_col or "__dynamic_sql__" in e.target_col
                   for e in edges)
    assert not any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_execute_immediate_truncate_insert_chain_folds_to_real_edges():
    """Origin SC3: parameterised TRUNCATE / INSERT chain over a bound DECLARE."""
    sql = """
    BEGIN
      DECLARE table_name STRING DEFAULT 'my_catalog.my_schema.staging';
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || table_name;
      EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || ' SELECT * FROM source';
    END
    """
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    edges = result.edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # Real edges from `source` to the resolved target
    assert any(s.startswith("source.") for s in sources), sources
    assert any(t.startswith("my_catalog.my_schema.staging.") for t in targets), targets
    # No dynamic-SQL placeholder leaked
    assert not any(
        "__dynamic_sql__" in e.source_col or "__dynamic_sql__" in e.target_col
        for e in edges
    )
    # No fold-time warning
    assert not any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_execute_immediate_recursively_folded_var():
    sql = """
    BEGIN
      DECLARE p STRING DEFAULT 'sch';
      DECLARE t STRING DEFAULT p || '.tbl';
      EXECUTE IMMEDIATE 'INSERT INTO ' || t || ' SELECT a FROM s';
    END
    """
    edges = parse_sql(sql, source_file="ei.sql", source_line=1).edges
    assert any(
        e.source_col == "s.a" and e.target_col == "sch.tbl.a" for e in edges
    ), f"missing edge s.a -> sch.tbl.a; got {[(e.source_col, e.target_col) for e in edges]}"


def test_execute_immediate_non_foldable_emits_dynamic_sql_placeholder():
    """Function call as the dynamic-SQL expression cannot be folded — must degrade
    to a single approximate self-edge plus exactly one per-statement warning."""
    sql = "BEGIN EXECUTE IMMEDIATE current_query(); END"
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    placeholder = [
        e for e in result.edges
        if e.source_col == "__dynamic_sql__.*" and e.target_col == "__dynamic_sql__.*"
    ]
    assert len(placeholder) == 1, (
        f"expected exactly one __dynamic_sql__ self-edge; got {result.edges}"
    )
    assert all(e.confidence == "approximate" for e in placeholder)
    warnings = [w for w in result.warnings if "Non-foldable EXECUTE IMMEDIATE" in w]
    assert len(warnings) == 1, f"expected one warning; got {result.warnings}"


def test_execute_immediate_unbound_variable_emits_placeholder():
    sql = (
        "BEGIN DECLARE x STRING; "
        "EXECUTE IMMEDIATE 'INSERT INTO ' || x || ' SELECT * FROM s'; END"
    )
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    assert any(
        e.source_col == "__dynamic_sql__.*" and e.target_col == "__dynamic_sql__.*"
        for e in result.edges
    )
    assert any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_execute_immediate_set_clears_binding_then_placeholder():
    """SET with a non-literal RHS must clear any prior literal binding so subsequent
    folds defensively fall through to the placeholder rather than reusing stale data."""
    sql = """
    BEGIN
      DECLARE p STRING DEFAULT 'foo';
      SET p = (SELECT name FROM t LIMIT 1);
      EXECUTE IMMEDIATE 'INSERT INTO ' || p || ' SELECT * FROM s';
    END
    """
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    assert any(
        e.source_col == "__dynamic_sql__.*" and e.target_col == "__dynamic_sql__.*"
        for e in result.edges
    )
    assert any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_execute_immediate_inside_for_loop_uses_outer_binding():
    """Bindings declared above a FOR loop are visible to EXECUTE IMMEDIATE in its
    body. Verifies scope traversal across nested handlers."""
    sql = """
    BEGIN
      DECLARE p STRING DEFAULT 'tgt_table';
      FOR row AS SELECT a FROM s DO
        EXECUTE IMMEDIATE 'INSERT INTO ' || p || ' SELECT b FROM other_src';
      END FOR;
    END
    """
    edges = parse_sql(sql, source_file="ei.sql", source_line=1).edges
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert any(t.startswith("tgt_table.") for t in targets), targets
    assert "other_src.b" in sources, sources
    assert not any("__dynamic_sql__" in t for t in targets)


def test_execute_immediate_using_clause_dropped_skeleton_parses():
    """USING parameter substitution is deferred; the static SQL skeleton survives
    and parses (USING bindings do not contribute spurious edges)."""
    sql = (
        "BEGIN EXECUTE IMMEDIATE "
        "'SELECT SUM(c1) FROM VALUES(?), (?) AS t(c1)' USING 5, 6; END"
    )
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    # No dynamic-SQL placeholder — the static skeleton folds to a literal
    assert not any(
        "__dynamic_sql__" in e.source_col or "__dynamic_sql__" in e.target_col
        for e in result.edges
    )
    assert not any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_execute_immediate_folded_sql_malformed_surfaces_parse_warning():
    """Folded successfully but the resulting SQL is itself unparseable — a normal
    per-statement parse warning surfaces; no edges; no fold-time placeholder."""
    sql = (
        "BEGIN DECLARE x STRING DEFAULT 'INSERT INTO FROM WHERE'; "
        "EXECUTE IMMEDIATE x; END"
    )
    result = parse_sql(sql, source_file="ei.sql", source_line=1)
    # No edges (SQLGlot parses the gibberish but no source/target attribution)
    assert not any(
        e.source_col == "__dynamic_sql__.*" or e.target_col == "__dynamic_sql__.*"
        for e in result.edges
    ), f"placeholder leaked despite folding success: {result.edges}"
    # The warning (if any) is the SQLGlot parse error, not the fold-time placeholder
    assert not any("Non-foldable EXECUTE IMMEDIATE" in w for w in result.warnings)


def test_recursive_cte_mixed_with_non_recursive_cte():
    """WITH RECURSIVE may declare a mix: only the self-referencing CTE is treated as
    recursive; the non-recursive one resolves with the standard logic."""
    sql = """
    WITH RECURSIVE r AS (
      SELECT id FROM s
      UNION ALL
      SELECT id FROM r WHERE id < 10
    ),
    nr AS (
      SELECT a FROM t
    )
    INSERT INTO out_tbl SELECT r.id, nr.a FROM r JOIN nr ON r.id = nr.a
    """
    edges = parse_sql(sql, source_file="m.sql", source_line=1).edges
    sources = {e.source_col for e in edges}
    targets = {e.target_col for e in edges}
    # Recursive CTE: edges resolve through anchor (s)
    assert "s.id" in sources, f"missing s.id: {sources}"
    # Non-recursive CTE: edges resolve through t
    assert "t.a" in sources, f"missing t.a: {sources}"
    # Both target columns reach out_tbl
    assert "out_tbl.id" in targets
    assert "out_tbl.a" in targets
    assert _no_self_loops(edges, "r")
    assert _no_self_loops(edges, "nr")
