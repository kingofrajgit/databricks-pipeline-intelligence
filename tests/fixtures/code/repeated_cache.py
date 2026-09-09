"""Cache with no demonstrated reuse (CODE-PYSPARK-011)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
df.cache()
df.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out/")
