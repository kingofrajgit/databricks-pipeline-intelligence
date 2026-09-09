"""PySpark pipeline demonstrating count() used for existence checks vs isEmpty()."""

events = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/events/")

# Anti-pattern: using count() > 0 or == 0 or if count() for existence
if events.count() > 0:
    events.write.format("delta").mode("append").save(
        "abfss://curated@acct.dfs.core.windows.net/events/"
    )

if events.count() == 0:
    print("No events found")

# Recommended pattern: isEmpty() or limit(1).count() > 0
if not events.isEmpty():
    print("Events present")
