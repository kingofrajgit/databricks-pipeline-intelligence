"""Unrestricted collect() over a distributed frame (CODE-PYSPARK-006)."""

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
rows = df.collect()
print(f"fetched {len(rows)} rows")
