"""Single-partition coalesce before a write (CODE-PYSPARK-010 at scale)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
df.coalesce(1).write.format("parquet").mode("overwrite").save(
    "abfss://curated@acct.dfs.core.windows.net/out/"
)
