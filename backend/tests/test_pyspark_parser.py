import pytest
from parsers.pyspark import parse_pyspark
from lineage.models import LineageEdge


SIMPLE_SELECT = """\
df = spark.read.table("raw_orders")
df2 = df.select("order_id", "amount")
df2.write.saveAsTable("staging_orders")
"""

WITHCOLUMN = """\
df = spark.read.table("raw_orders")
df2 = df.withColumn("total", F.col("amount") * F.col("tax_rate"))
df2.write.saveAsTable("enriched_orders")
"""

AGG = """\
df = spark.read.table("raw_orders")
df2 = df.groupBy("customer_id").agg(F.sum("amount").alias("total_revenue"))
df2.write.saveAsTable("agg_revenue")
"""

CHAINED = """\
df = spark.read.table("raw_orders") \
    .filter(F.col("status") == "active") \
    .withColumn("revenue", F.col("amount") * 1.1)
df.write.saveAsTable("active_orders")
"""


def test_simple_select_passthrough():
    edges = parse_pyspark(SIMPLE_SELECT, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "staging_orders.order_id" in targets
    assert "staging_orders.amount" in targets
    passthrough = [e for e in edges if e.transform_type == "passthrough"]
    assert len(passthrough) >= 2


def test_withcolumn_expression():
    edges = parse_pyspark(WITHCOLUMN, source_file="pipeline.py")
    edge = next((e for e in edges if e.target_col == "enriched_orders.total"), None)
    assert edge is not None
    assert edge.transform_type in ("expression", "aggregation")


def test_agg_sum():
    edges = parse_pyspark(AGG, source_file="pipeline.py")
    edge = next((e for e in edges if e.target_col == "agg_revenue.total_revenue"), None)
    assert edge is not None
    assert edge.transform_type == "aggregation"
    assert edge.source_col == "raw_orders.amount"


def test_chained_operations():
    edges = parse_pyspark(CHAINED, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "active_orders.revenue" in targets


def test_bad_python_raises_syntax_error():
    with pytest.raises(SyntaxError):
        parse_pyspark("def (((broken:", source_file="bad.py")


def test_source_line_attached():
    edges = parse_pyspark(AGG, source_file="pipeline.py")
    for edge in edges:
        assert edge.source_line is not None
        assert edge.source_line > 0


JOIN_SIMPLE = """\
orders = spark.read.table("raw_orders")
customers = spark.read.table("customer_dim")
joined = orders.join(customers, "customer_id")
result = joined.select("order_id", "customer_id", "customer_name")
result.write.saveAsTable("enriched_orders")
"""

JOIN_LIST_KEYS = """\
orders = spark.read.table("raw_orders")
customers = spark.read.table("customer_dim")
joined = orders.join(customers, ["customer_id", "region_id"])
result = joined.select("order_id", "customer_id", "region_id", "customer_name")
result.write.saveAsTable("enriched_orders")
"""

JOIN_ON_KEYWORD = """\
orders = spark.read.table("raw_orders")
customers = spark.read.table("customer_dim")
joined = orders.join(customers, on="customer_id")
result = joined.select("order_id", "customer_id", "customer_name")
result.write.saveAsTable("enriched_orders")
"""


def test_join_simple_key():
    edges = parse_pyspark(JOIN_SIMPLE, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "enriched_orders.order_id" in targets
    assert "enriched_orders.customer_id" in targets
    assert "enriched_orders.customer_name" in targets
    # Join key should have join_key transform from both tables
    jk_edges = [e for e in edges if e.transform_type == "join_key"]
    assert len(jk_edges) >= 1
    jk_sources = {e.source_col for e in jk_edges}
    assert "raw_orders.customer_id" in jk_sources or "customer_dim.customer_id" in jk_sources


def test_join_list_keys():
    edges = parse_pyspark(JOIN_LIST_KEYS, source_file="pipeline.py")
    jk_edges = [e for e in edges if e.transform_type == "join_key"]
    jk_target_cols = {e.target_col.split(".")[-1] for e in jk_edges}
    assert "customer_id" in jk_target_cols
    assert "region_id" in jk_target_cols


def test_join_on_keyword():
    edges = parse_pyspark(JOIN_ON_KEYWORD, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    assert "enriched_orders.order_id" in targets
    assert "enriched_orders.customer_name" in targets
    jk_edges = [e for e in edges if e.transform_type == "join_key"]
    assert len(jk_edges) >= 1


SPARK_SQL_CREATE_VIEW = '''\
spark.sql("""
CREATE OR REPLACE TEMP VIEW Cost_To_Recv AS (
    WITH CTR_DTLS AS (
        SELECT TRK_NUM, CARR_CD FROM raw_shipments
    )
    SELECT TRK_NUM, CARR_CD FROM CTR_DTLS
)
""")
'''

SPARK_SQL_INSERT = '''\
spark.sql("""
INSERT INTO agg_orders
SELECT customer_id, SUM(amount) AS total
FROM raw_orders
GROUP BY customer_id
""")
'''

SPARK_SQL_ASSIGN = '''\
df = spark.sql("""
SELECT order_id, amount FROM raw_orders
""")
df.write.saveAsTable("staging_orders")
'''


def test_spark_sql_create_view():
    edges = parse_pyspark(SPARK_SQL_CREATE_VIEW, source_file="pipeline.py")
    # A standalone temp view with no downstream consumer produces no lineage edges
    # after resolution (temp view edges are internal/intermediate).
    assert len(edges) == 0


def test_spark_sql_insert():
    edges = parse_pyspark(SPARK_SQL_INSERT, source_file="pipeline.py")
    assert len(edges) >= 2
    targets = {e.target_col for e in edges}
    assert "agg_orders.customer_id" in targets
    assert "agg_orders.total" in targets
    agg_edge = next(e for e in edges if e.target_col == "agg_orders.total")
    assert agg_edge.transform_type == "aggregation"


def test_spark_sql_assign():
    """df = spark.sql('SELECT ...') should emit edges from the SQL."""
    edges = parse_pyspark(SPARK_SQL_ASSIGN, source_file="pipeline.py")
    # The spark.sql() SELECT produces edges with target "result"
    sql_edges = [e for e in edges if "result." in e.target_col or "raw_orders." in e.source_col]
    assert len(sql_edges) >= 2


def test_spark_sql_source_line():
    edges = parse_pyspark(SPARK_SQL_CREATE_VIEW, source_file="pipeline.py")
    for edge in edges:
        assert edge.source_line is not None


DATABRICKS_NOTEBOOK = '''# Databricks notebook source
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW stg_orders AS (
# MAGIC   SELECT order_id, customer_id, amount
# MAGIC   FROM raw_orders
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO agg_revenue
# MAGIC SELECT customer_id, SUM(amount) AS total
# MAGIC FROM stg_orders
# MAGIC GROUP BY customer_id

# COMMAND ----------

df = spark.read.table("enrichment")
result = df.select("id", "value")
result.write.saveAsTable("final_output")
'''

DATABRICKS_NOTEBOOK_MIXED_MAGIC = '''# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # This is a markdown cell - should be skipped

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT col_a, col_b FROM src_table

# COMMAND ----------

# MAGIC %python
# MAGIC print("this is python magic - should be skipped")
'''


def test_databricks_notebook_sql_cells():
    edges = parse_pyspark(DATABRICKS_NOTEBOOK, source_file="nb.py")
    targets = {e.target_col for e in edges}
    # stg_orders is a temp view — its edges should be resolved through
    assert "stg_orders.order_id" not in targets
    assert "stg_orders.customer_id" not in targets
    # Final targets should trace back to raw_orders directly
    assert "agg_revenue.total" in targets
    assert "agg_revenue.customer_id" in targets
    agg_edge = next(e for e in edges if e.target_col == "agg_revenue.total")
    assert "raw_orders" in agg_edge.source_col


def test_databricks_notebook_pyspark_cell():
    edges = parse_pyspark(DATABRICKS_NOTEBOOK, source_file="nb.py")
    targets = {e.target_col for e in edges}
    assert "final_output.id" in targets
    assert "final_output.value" in targets


def test_databricks_notebook_cell_index():
    edges = parse_pyspark(DATABRICKS_NOTEBOOK, source_file="nb.py")
    # stg_orders is a temp view, so its edges are resolved through.
    # SQL cell 2 (INSERT INTO) should be cell_idx 2
    agg_edges = [e for e in edges if "agg_revenue" in e.target_col]
    assert all(e.source_cell == 2 for e in agg_edges)
    # PySpark cell should be cell_idx 3
    py_edges = [e for e in edges if "final_output" in e.target_col]
    assert all(e.source_cell == 3 for e in py_edges)


def test_databricks_notebook_skips_markdown():
    edges = parse_pyspark(DATABRICKS_NOTEBOOK_MIXED_MAGIC, source_file="nb.py")
    # Only the %sql cell should produce edges
    targets = {e.target_col for e in edges}
    assert "result.col_a" in targets
    assert "result.col_b" in targets
    assert len(edges) == 2


def test_regular_python_not_treated_as_databricks():
    """Files without the Databricks header should parse normally."""
    code = 'df = spark.read.table("t1")\ndf.write.saveAsTable("t2")'
    edges = parse_pyspark(code, source_file="regular.py")
    # Should not crash or misbehave
    assert isinstance(edges, list)


SPARK_SQL_CROSS_CALL_TEMP_VIEW = '''\
spark.sql("CREATE OR REPLACE TEMP VIEW staging AS SELECT id, val FROM source_table")
spark.sql("INSERT INTO final SELECT id, val FROM staging")
'''

def test_plain_py_spark_sql_cross_call_temp_view():
    """Temp view created in one spark.sql() call must be resolved in a later call."""
    edges = parse_pyspark(SPARK_SQL_CROSS_CALL_TEMP_VIEW, source_file="pipeline.py")
    targets = {e.target_col for e in edges}
    sources = {e.source_col for e in edges}
    assert "staging.id" not in targets, "temp view must not appear as a target"
    assert "staging.val" not in targets
    assert "final.id" in targets
    assert "final.val" in targets
    assert "source_table.id" in sources
    assert "source_table.val" in sources
