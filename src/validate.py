df = spark.table("asset_bundle_catalog.asset_bundle_schema.transformation_table")

count = df.count()

print(f"Row count: {count}")

if count > 0:
    dbutils.jobs.taskValues.set(key="status", value="success")
else:
    dbutils.jobs.taskValues.set(key="status", value="failed")
    