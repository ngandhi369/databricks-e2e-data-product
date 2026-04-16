import dlt
from pyspark.sql.functions import col, count, sum, avg, max
import dlt.pipeline

# spark is injected by the Databricks DLT runtime — not a normal import.
# ruff does not know this, so we suppress F821 (undefined name) for this file.
# ruff: noqa: F821

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
volume_path = spark.conf.get("volume_path")
volume_file_path = f"{volume_path}/orders.csv"


# BRONZE:
@dlt.table(
    name="bronze_orders_dlt",
    comment="Raw orders data ingested from source system"
)
def bronze_orders():
    return (
        spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(volume_file_path)
    )    


# SILVER:
@dlt.table(
    name="silver_orders_dlt",
    comment="Type casted and null-dropped orders"
)
def silver_orders():
    df = dlt.read("bronze_orders_dlt")
    return (df.withColumn("amount", col("amount").cast("double")).dropna())


# VALIDATION:
@dlt.table(
    name="validate_orders_dlt",
    comment="Silver orders with DLT data quality expectations applied"
)
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
def validate_orders():
    return dlt.read("silver_orders_dlt")


# GOLD:
@dlt.table(
    name="customer_metrics_dlt",
    comment="Pre-customer aggregate metrics - Gold Layer"
)
def customer_metrics():
    df = dlt.read("validate_orders_dlt")
    return (
        df.groupBy("customer_id", "city")\
        .agg(
            count("*").alias("total_orders"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date")
        )
    )
