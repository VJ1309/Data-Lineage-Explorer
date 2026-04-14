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


def test_bad_python_returns_empty():
    edges = parse_pyspark("def (((broken:", source_file="bad.py")
    assert isinstance(edges, list)
    assert len(edges) == 0


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
