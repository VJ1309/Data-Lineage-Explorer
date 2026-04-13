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
