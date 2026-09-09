
df = spark.sql('SELECT * FROM a CROSS JOIN b')
df.collect()
