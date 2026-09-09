"""Tiny driver collect over a small lookup table (acceptable with small-data context)."""

lookup = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/dim_country/")
small = lookup.limit(50).collect()
print(f"loaded {len(small)} countries")
