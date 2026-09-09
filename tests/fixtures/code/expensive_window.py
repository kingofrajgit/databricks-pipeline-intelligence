"""Unbounded window specification (CODE-PYSPARK-014)."""

import pyspark.sql.functions as F
from pyspark.sql import Window

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")
spec = Window.partitionBy("customer_id").orderBy("event_time")
ranked = df.withColumn("rn", F.row_number().over(spec))
ranked.write.format("delta").mode("append").save("abfss://curated@acct.dfs.core.windows.net/out/")
