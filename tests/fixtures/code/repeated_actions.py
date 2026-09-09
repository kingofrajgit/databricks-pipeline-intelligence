"""Two actions on one frame (CODE-PYSPARK-012)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
print(df.count())
df.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out/")
