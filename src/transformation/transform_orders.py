from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, upper, col, rank, sum as _sum, count, max
from pyspark.sql.window import Window

from config import get_table # udf function

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.read.table(get_table("bronze_orders"))

# --------------- Transformation Logic: SILVER LAYER ---------------

window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

silver_df = bronze_df.withColumn("rn", row_number().over(window_spec))\
    .filter(col("rn") == 1)\
    .drop("rn")

silver_df.write.mode("overwrite").saveAsTable(get_table("silver_orders"))

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

gold_df.write.mode("overwrite").saveAsTable(get_table("gold_orders"))

print("✅ Gold created...!")