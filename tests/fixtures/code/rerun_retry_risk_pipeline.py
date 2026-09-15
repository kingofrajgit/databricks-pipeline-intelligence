"""Offline M5F fixture: append-only write with checkpoint evidence, no upsert.

Analysed statically by DPIF (never executed), this fixture deterministically
demonstrates the M5F rerun / idempotency / duplicate-data signals:

- ``mode("append")`` with no deduplication and no MERGE
  -> duplicate-data risk on rerun, retry, overlapping input, and partial-failure recovery
- ``checkpointLocation`` configured alongside an incremental read boundary
  -> INCREMENTAL_INPUT is a real PASS backed by checkpoint evidence
- no watermark and no upsert
  -> late-arriving data handling stays an honest UNKNOWN, never a false PASS

Keep this docstring free of the keywords the M5F late-data heuristic scans raw
source for. Keyword matching cannot tell prose from pipeline logic, so naming one
of those keywords here would fabricate late-data evidence for this fixture.
"""

from pyspark.sql import functions as F

incoming = (
    spark.read.format("delta")
    .option("startingVersion", 0)
    .load("abfss://landing@acct.dfs.core.windows.net/payment_events/")
    .filter(F.col("event_time") >= F.lit("2026-09-01"))
)

(
    incoming.write.format("delta")
    .mode("append")
    .option(
        "checkpointLocation",
        "abfss://checkpoints@acct.dfs.core.windows.net/payment_events/",
    )
    .save("abfss://curated@acct.dfs.core.windows.net/payment_events/")
)
