from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [(1, "Virat"), (2, "Rohit")]
df = spark.createDataFrame(data, ["serial_number", "name"])

df.write.mode("overwrite").saveAsTable("asset_bundle_catalog.asset_bundle_schema.ingestion_table")

print("Ingestion is completed...!")
