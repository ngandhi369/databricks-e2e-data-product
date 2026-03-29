import sys
import os

# Add project root dynamically
project_root = os.path.dirname(os.path.dirname(os.getcwd()))
print(f"project_root:{project_root}")
if project_root not in sys.path:
    sys.path.append(project_root)
from src.utils.config import get_table, get_file_path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Volume file path. Please upload the file before this
file_path = get_file_path("orders") # path taken from databricks.yml variables section. Set this variable in the dev/prod targets based on environments.

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
