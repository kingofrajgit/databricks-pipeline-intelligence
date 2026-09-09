"""Code fixture demonstrating high scalability risks: cross join, collect, distinct."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_a = spark.read.table("catalog.schema.orders")
df_b = spark.read.table("catalog.schema.customers")

# SCALABILITY-007 / CODE-PYSPARK-004: Cartesian product
cartesian = df_a.crossJoin(df_b)

# SCALABILITY-005 / CODE-PYSPARK-001: Driver-side materialization
collected = cartesian.collect()

# SCALABILITY-009 / CODE-SQL-006: Distinct aggregation
distinct_orders = df_a.select("customer_id").distinct()

# SCALABILITY-004: Unpartitioned write of massive table
distinct_orders.write.format("delta").mode("append").save("/mnt/lake/orders_output")
