import sys, os, shutil
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from itertools import count
from os import name
from tokenize import Comment

import dlt
from pyspark.sql.functions import *

from src.config import get_config


config = get_config()

catalog = config["catalog"]
schema = config["schema"]
volume_path = config["volume_path"]


volume_file_path = f"{volume_path}orders.csv"

# ------------- BRONZE --------------
@dlt.table(
    name="bronze_orders",
    comment="Raw orders data ingested from source system"
)
def bronze_orders():
    return (
        spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(volume_file_path)
    )


# ------------- SILVER --------------

@dlt.table(
    name="silver_orders",
    comment="Cleaned data"
)
def silver_orders():
    df = dlt.read("bronze_orders")

    return df \
        .withColumn("amount", col("amount").cast("double")) \
        .dropna()


# ------------- GOLD --------------

@dlt.table(
    name="customer_metrics",
    comment="Customer insights"
)
def customer_metrics():
    df = dlt.read("silver_orders")

    return df.groupBy("customer_id", "city") \
        .agg(
            count("*").alias("total_orders"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date")
        )


# ------------- VALIDATION --------------
@dlt.table(
    name="silver_orders",
    comment="Cleaned data"
)
@dlt.expect("valid_amount", "amount > 0")
def silver_orders():
    return dlt.read("bronze_orders") \
        .withColumn("amount", col("amount").cast("double"))