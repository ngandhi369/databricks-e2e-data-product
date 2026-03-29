import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from src.config import get_config

spark = SparkSession.builder.getOrCreate()

config = get_config()

catalog = config["catalog"]
schema = config["schema"]
volume_path = config["volume_path"]

file_path = f"{volume_path}orders.csv"


bronze_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(file_path)

# Type casting to ensure data consistency and quality before writing to the bronze layer
bronze_df = bronze_df.selectExpr(
    "cast(id as int)",
    "cast(customer_id as int)",
    "cast(order_date as date)",
    "cast(amount as double)",
    "city"
)

# Writing to the bronze layer.  
bronze_df.write.mode("overwrite").saveAsTable(get_table("bronze_orders"))

print("✅ Bronze created...!")
