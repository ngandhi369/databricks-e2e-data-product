import sys, os, shutil
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from src.config import get_config

spark = SparkSession.builder.getOrCreate()

config = get_config()

catalog = config["catalog"]
schema = config["schema"]
volume_path = config["volume_path"]



# Get the current working directory
cwd = os.getcwd()

# Traverse up the directory tree until we find the "files" directory in deployed bundle on workspace
base_path = cwd
while base_path and not base_path.endswith("files"):
    base_path = os.path.dirname(base_path)

source_file = os.path.join(base_path, "data", "orders.csv")
volume_file_path = f"{volume_path}orders.csv"

print(f"📂 Copying file from {source_file} → {volume_file_path}")

dbutils.fs.cp(source_file, volume_file_path)
print("✅ file copied to volume...!")




bronze_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(volume_file_path)

# Type casting to ensure data consistency and quality before writing to the bronze layer
bronze_df = bronze_df.selectExpr(
    "cast(id as int)",
    "cast(customer_id as int)",
    "cast(order_date as date)",
    "cast(amount as double)",
    "city"
)

# Writing to the bronze layer.  
bronze_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_orders")

print("✅ Bronze created...!")
