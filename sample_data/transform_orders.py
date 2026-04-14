from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, to_date, when

spark = SparkSession.builder.appName("OrderTransform").getOrCreate()

# Load raw source
raw = spark.read.table("raw_orders")
customers = spark.read.table("customer_dim")

# Clean and enrich orders
cleaned = (
    raw
    .withColumn("customer_id", upper(col("customer_id")))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("status", when(col("status").isNull(), "unknown").otherwise(col("status")))
    .select("order_id", "customer_id", "amount", "order_date", "status")
)

# Write to staging table
cleaned.write.saveAsTable("stg_orders")

# Join with customer dimension and aggregate
enriched = cleaned.join(customers, "customer_id")
result = enriched.select("order_id", "customer_id", "customer_name", "amount", "region")
result.write.saveAsTable("enriched_orders")

# Aggregate by customer
agg = (
    cleaned
    .groupBy("customer_id")
    .agg(
        {"amount": "sum", "order_id": "count"}
    )
)

agg.write.saveAsTable("customer_order_summary")
