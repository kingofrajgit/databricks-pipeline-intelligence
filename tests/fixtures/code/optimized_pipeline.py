"""Genuinely reasonable PySpark pipeline: filter early, project columns,
keyed join, single distributed write. No driver materialization."""

from pyspark.sql import functions as F

try:
    orders = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/orders/")
    customers = spark.read.format("delta").load(
        "abfss://landing@acct.dfs.core.windows.net/customers/"
    )
    recent = orders.filter(F.col("event_date") >= F.lit("2026-09-01"))
    projected = recent.select("order_id", "customer_id", "order_total")
    joined = projected.join(customers, on="customer_id", how="inner")
    (
        joined.write.format("delta")
        .mode("append")
        .option("checkpointLocation", "abfss://checkpoints@acct.dfs.core.windows.net/orders/")
        .save("abfss://curated@acct.dfs.core.windows.net/orders/")
    )
except Exception:
    import logging

    logging.getLogger(__name__).exception("incremental write failed")
    raise
