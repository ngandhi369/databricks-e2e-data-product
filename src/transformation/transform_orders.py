import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, upper, col, rank, sum as _sum, count, max, avg, month, year, to_date, datediff, current_date, when
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

bronze_df = bronze_df.withColumn("order_date", to_date(col("order_date")))

# Deduplicate properly (same customer, same date)
window_spec = Window.partitionBy("customer_id", "order_date", "amount") \
                    .orderBy(col("order_date").desc())

silver_df = bronze_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# Add derived features
silver_df = silver_df \
    .withColumn("order_month", month("order_date")) \
    .withColumn("order_year", year("order_date"))

silver_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_orders")

print("✅ Silver created (clean + enriched)")


# --------------- Transformation Logic: GOLD LAYER ---------------

customer_df = silver_df.groupBy("customer_id", "city") \
    .agg(
        count("*").alias("total_orders"),
        _sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date")
    )

customer_df = customer_df.withColumn(
    "days_since_last_order",
    datediff(current_date(), col("last_order_date"))
)

customer_df = customer_df.withColumn(
    "customer_segment",
    when(col("total_spent") > 1500, "High Value")
    .when(col("total_spent") > 800, "Medium Value")
    .otherwise("Low Value")
)

city_window = Window.partitionBy("city").orderBy(col("total_spent").desc())
overall_window = Window.orderBy(col("total_spent").desc())

gold_df = customer_df \
    .withColumn("city_rank", rank().over(city_window)) \
    .withColumn("overall_rank", rank().over(overall_window))

gold_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_orders")

print("✅ Gold created...!")