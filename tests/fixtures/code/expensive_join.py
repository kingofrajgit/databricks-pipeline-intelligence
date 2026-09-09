"""Large fact-to-fact join on a plain key (shuffle risk context)."""

left = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events_a/")
right = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events_b/")
joined = left.join(right, on="session_id", how="inner")
joined.write.format("delta").mode("append").save(
    "abfss://curated@acct.dfs.core.windows.net/joined/"
)
