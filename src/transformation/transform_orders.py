import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql.functions import (
    row_number, col,
    rank, sum as _sum,
    count, max, avg,
    month, year,
    to_date, datediff,
    current_date, when
)
from pyspark.sql.window import Window

from src.config import get_config
from src.spark_session import get_spark

spark  = get_spark()
config = get_config()
catalog = config["catalog"]
schema  = config["schema"]

# Read Bronze:
bronze_df = spark.read.table(f"{catalog}.{schema}.bronze_orders")

# Silver Layer:
bronze_df = bronze_df.withColumn("order_date", to_date(col("order_date")))

# Deduplicate: keep the latest record per (customer_id, order_date, amount)
window_spec = Window.partitionBy("customer_id", "order_date", "amount") \
                    .orderBy(col("order_date").desc())

silver_df = (
    bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Derived time features
silver_df = silver_df \
    .withColumn("order_month", month("order_date")) \
    .withColumn("order_year", year("order_date"))

silver_table = f"{catalog}.{schema}.silver_orders"
silver_df.createOrReplaceTempView("incoming_silver")

spark.sql(f"""
    MERGE INTO {silver_table} AS target
    USING incoming_silver AS source
    ON  target.customer_id = source.customer_id
    AND target.order_date = source.order_date
    AND target.amount = source.amount
    WHEN NOT MATCHED THEN INSERT *
""")
print("✅ Silver MERGE complete")

# Re-read Silver from the table to ensure Gold is built from the committed state
silver_df = spark.read.table(silver_table)

# Gold Layer:

customer_df = (
    silver_df
    .groupBy("customer_id", "city")
    .agg(
        count("*").alias("total_orders"),
        _sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
    )
)

customer_df = customer_df.withColumn(
    "days_since_last_order",
    datediff(current_date(), col("last_order_date"))
)

HIGH_VALUE_THRESHOLD = 1200
MEDIUM_VALUE_THRESHOLD = 600

customer_df = customer_df.withColumn(
    "customer_segment",
    when(col("total_spent") > HIGH_VALUE_THRESHOLD, "High Value")
    .when(col("total_spent") > MEDIUM_VALUE_THRESHOLD, "Medium Value")
    .otherwise("Low Value")
)

# Window rankings
city_window = Window.partitionBy("city").orderBy(col("total_spent").desc())
overall_window = Window.orderBy(col("total_spent").desc())

gold_df = customer_df \
    .withColumn("city_rank", rank().over(city_window)) \
    .withColumn("overall_rank", rank().over(overall_window))

gold_table = f"{catalog}.{schema}.gold_orders"
gold_df.createOrReplaceTempView("incoming_gold")

spark.sql(f"""
    MERGE INTO {gold_table} AS target
    USING incoming_gold AS source
    ON  target.customer_id = source.customer_id
    AND target.city = source.city
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print("✅ Gold MERGE complete")

# Emit Gold row count for the downstream validate task
gold_count = spark.read.table(gold_table).count()
print(f"📊 Gold table rows: {gold_count}")

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    pass

dbutils.jobs.taskValues.set(key="gold_row_count", value=gold_count)
