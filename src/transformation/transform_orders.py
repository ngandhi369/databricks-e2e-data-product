import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, upper, col, rank, sum as _sum, count, max
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from src.config import get_config

spark = SparkSession.builder.getOrCreate()

config = get_config()

catalog = config["catalog"]
schema = config["schema"]
volume_path = config["volume_path"]

bronze_df = spark.read.table(f"{catalog}.{schema}.bronze_orders")

# --------------- Transformation Logic: SILVER LAYER ---------------

window_spec = Window.partitionBy("id").orderBy(col("order_date").desc())

silver_df = bronze_df.withColumn("rn", row_number().over(window_spec))\
    .filter(col("rn") == 1)\
    .drop("rn")

silver_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_orders")

print("✅ Silver created...!")


# --------------- Transformation Logic: GOLD LAYER ---------------

gold_df = silver_df.groupBy("customer_id","city")\
    .agg(
        count("*").alias("total_orders"),
        _sum("amount").alias("total_spent"),
        max("order_date").alias("last_order_date")
    )

rank_window = Window.partitionBy("city").orderBy(col("total_spent").desc())
overall_window = Window.orderBy(col("total_spent").desc())

gold_df = gold_df.withColumn("city_rank", rank().over(rank_window))\
    .withColumn("overall_rank", rank().over(overall_window))

gold_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_orders")

print("✅ Gold created...!")