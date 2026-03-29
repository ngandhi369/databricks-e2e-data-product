from pyspark.sql import SparkSession
from utils.config import get_table # udf function

spark = SparkSession.builder.getOrCreate()

# Volume file path. Please upload the file before this
file_path = "dbfs:/Volumes/nirdosh_catalog/nirdosh_schema/nirdosh_volume/orders.csv"

bronze_df = spark.read.format("csv")\
    .option(header=True)\
    .option(inferSchema=True)\
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
