"""Healthy incremental PySpark pipeline fixture (no driver collects)."""

from pyspark.sql import functions as F

try:
    df = spark.read.format("parquet").load(
        "abfss://landing@acct.dfs.core.windows.net/customer_daily/"
    )
    watermarked = df.filter(F.col("event_date") >= F.lit("2026-09-01"))
    deduped = watermarked.dropDuplicates(["order_id"])
    (
        deduped.write.format("delta")
        .mode("append")
        .option(
            "checkpointLocation", "abfss://checkpoints@acct.dfs.core.windows.net/customer_daily/"
        )
        .save("abfss://curated@acct.dfs.core.windows.net/customer_daily/")
    )
except Exception:
    import logging

    logging.getLogger(__name__).exception("incremental write failed")
    raise
