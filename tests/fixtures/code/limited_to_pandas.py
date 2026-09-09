"""Bounded toPandas() after limit() (CODE-PYSPARK-007, calmer variant)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
sample = df.limit(1000).toPandas()
print(sample.head())
