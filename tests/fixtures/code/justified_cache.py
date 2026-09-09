"""Cache followed by multiple actions (no CODE-PYSPARK-011 finding)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
df.cache()
print(df.count())
df.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out_a/")
df.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out_b/")
