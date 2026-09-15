"""M5F fixture: Unprotected APPEND write with retry, concurrency, and partial failure risk."""

from pyspark.sql import functions as F

incoming = spark.read.format("parquet").load(
    "abfss://landing@acct.dfs.core.windows.net/events_stream/"
)

# Step 1: Write raw records using append without deduplication or partition isolation
incoming.write.mode("append").saveAsTable("prod.raw.events_unprotected")

# Step 2: Complex subsequent stage that may fail partially
summary = incoming.groupBy("event_type").agg(F.count("*").alias("cnt"))
summary.write.mode("overwrite").saveAsTable("prod.marts.events_summary")
