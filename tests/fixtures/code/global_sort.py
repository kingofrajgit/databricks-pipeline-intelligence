"""Global ordering over the full frame (CODE-PYSPARK-013 at scale)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
ordered = df.orderBy("event_time")
ordered.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out/")
