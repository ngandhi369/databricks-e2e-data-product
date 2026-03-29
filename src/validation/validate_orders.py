import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from src.config import get_config

spark = SparkSession.builder.getOrCreate()

config = get_config()
catalog = config["catalog"]
schema = config["schema"]

gold_df = spark.read.table(f"{catalog}.{schema}.gold_orders")

count = gold_df.count()

print(f"Gold layer count: {count}")

if count > 0:
    dbutils.jobs.taskValues.set(key="status", value="success")
else:
    dbutils.jobs.taskValues.set(key="status", value="failed")
    