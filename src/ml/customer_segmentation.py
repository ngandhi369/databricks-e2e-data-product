import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

from src.config import get_config

config = get_config()
spark = SparkSession.builder.getOrCreate()

catalog = config["catalog"]
schema = config["schema"]

gold_orders_df = spark.read.table(f"{catalog}.{schema}.gold_orders")

feature_df = gold_orders_df.select("total_orders", "total_spent", "avg_order_value", "days_since_last_order").dropna()


# Converting to features vector for ML model input
assembler = VectorAssembler(
    inputCols=["total_orders", "total_spent", "avg_order_value", "days_since_last_order"],
    outputCol="features"
)
final_df = assembler.transform(feature_df)


# KMeans clustering for customer segmentation
kmeans = KMeans(k=3, seed=42)
model = kmeans.fit(final_df)
clustered_df = model.transform(final_df)


# Joining clustered_df with gold_orders_df to get customer_id and city back in the final output
result_df = gold_orders_df.join(clustered_df.select("features", "prediction"), on="features", how="inner")


# Mapping cluster predictions to meaningful segment labels
result_df = result_df.withColumn(
    "customer_cluster",
    when(col("prediction") == 0, "Low Value")
    .when(col("prediction") == 1, "Medium Value")
    .otherwise("High Value")
)


result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.customer_segments")

print("✅ Customer segmentation completed...!")


