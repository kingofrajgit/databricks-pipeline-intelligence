"""Broadcast of a dimension table; suitability depends on its size."""

from pyspark.sql.functions import broadcast

facts = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/facts/")
customer_df = spark.read.format("parquet").load(
    "abfss://landing@acct.dfs.core.windows.net/dim_customer/"
)
joined = facts.join(broadcast(customer_df), on="customer_id", how="inner")
joined.write.format("delta").mode("append").save(
    "abfss://curated@acct.dfs.core.windows.net/joined/"
)
