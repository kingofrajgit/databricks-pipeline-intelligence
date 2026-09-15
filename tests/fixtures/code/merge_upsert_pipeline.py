"""M5F fixture: Delta MERGE / UPSERT pipeline with stable primary key."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

source_df = (
    spark.read.format("delta")
    .load("abfss://landing@acct.dfs.core.windows.net/customers/")
    .filter(F.col("updated_at") >= F.current_date() - F.expr("INTERVAL 3 DAYS"))
    .dropDuplicates(["customer_id"])
)

target_table = DeltaTable.forName(spark, "prod.marts.customers")

(
    target_table.alias("target")
    .merge(source_df.alias("source"), "target.customer_id = source.customer_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
