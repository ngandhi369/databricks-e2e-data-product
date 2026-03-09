from pyspark.sql.functions import col, upper

df = spark.table("asset_bundle_catalog.asset_bundle_schema.ingestion_table")

df = df.withColumn("name_upper", upper(col("name")))

df.write.mode("overwrite").saveAsTable("asset_bundle_catalog.asset_bundle_schema.transformation_table")

print("transformation is completed...!")