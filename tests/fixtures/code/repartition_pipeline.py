"""Repeated repartition on one frame (CODE-PYSPARK-009)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
staged = df.repartition(200)
ready = staged.repartition(50)
ready.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out/")
