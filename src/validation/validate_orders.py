from pyspark.sql import SparkSession
from config import get_table

spark = SparkSession.builder.getOrCreate()

gold_df = spark.read.table(get_table("gold_orders"))

count = gold_df.count()

print(f"Gold layer count: {count}")

if count > 0:
    dbutils.jobs.taskValues.set(key="status", value="success")
else:
    dbutils.jobs.taskValues.set(key="status", value="failed")
    