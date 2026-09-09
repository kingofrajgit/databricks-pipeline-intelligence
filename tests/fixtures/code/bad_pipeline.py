"""Deliberately bad production pipeline fixture: full reload + driver ops, no retries."""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

df = spark.read.format("parquet").load("abfss://landing@acct.dfs.core.windows.net/bad_production/")

# Driver-side collection of a distributed dataset (CODE-PYSPARK-001, blocking)
rows = df.collect()

# Pandas conversion on the driver (large-data risk)
pdf = df.toPandas()

# Python UDF instead of native Spark SQL (CODE-PYSPARK-002)
shout = udf(lambda s: s.upper(), StringType())
df2 = df.withColumn("name_upper", shout("name"))

# Debug action left in production path (CODE-PYSPARK-003)
df2.show()
first = df2.take(5)

# Cartesian product without a join key (CODE-PYSPARK-004)
pairs = df.crossJoin(df.limit(100))

# Hard-coded local path (CODE-PYSPARK-005)
pairs.write.parquet("/tmp/bad_production_output")

# Hard-coded credential (CP-020 must flag; value must be masked in reports)
password = "Sup3rSecret-DoNotLog-12345"

# Unnecessary repeated repartition (CODE-PYSPARK-009)
huge = df.repartition(200)
huge = huge.repartition(50)

# Global sort over the huge frame (CODE-PYSPARK-013)
sorted_huge = huge.orderBy("name")

# Repeated actions on one frame (CODE-PYSPARK-012)
print(df.count())
print(df.count())

# Cache with no demonstrated reuse (CODE-PYSPARK-011)
df.cache()
df.write.parquet("/tmp/bad_production_output2")

# No try/except, no restart config, no retries/timeout (see contract).
