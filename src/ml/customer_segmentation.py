import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql.functions import col, when

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

from src.config import get_config
from src.spark_session import get_spark

spark = get_spark()

print("Spark:", spark)

config = get_config()

catalog = config["catalog"]
schema = config["schema"]

gold_orders_df = spark.read.table(f"{catalog}.{schema}.gold_orders")

feature_df = gold_orders_df.select("customer_id", "total_orders", "total_spent", "avg_order_value", "days_since_last_order").dropna()

# feature_df.printSchema()
feature_df = feature_df.fillna(0)

# Converting to features vector for ML model input
assembler = VectorAssembler(
    inputCols=["total_orders", "total_spent", "avg_order_value", "days_since_last_order"],
    outputCol="features"
)
assembled_df = assembler.transform(feature_df)

scaler = StandardScaler(
    inputCol="features", 
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)
scaled_df = scaler.fit(assembled_df).transform(assembled_df)


# KMeans clustering for customer segmentation
kmeans = KMeans(featuresCol="scaled_features", k=3, seed=42)
model = kmeans.fit(scaled_df)
clustered_df = model.transform(scaled_df)


cost = []
for k in range(2, 6):
    kmeans = KMeans(featuresCol="scaled_features", k=k, seed=42)
    model = kmeans.fit(scaled_df)
    cost.append((k, model.summary.trainingCost))

print("Elbow method data:", cost)


# Joining clustered_df with gold_orders_df to get customer_id and city back in the final output
result_df = gold_orders_df.join(
    clustered_df.select("customer_id", "prediction"),
    on="customer_id",
    how="inner"
)


# Mapping cluster predictions to meaningful segment labels
result_df = result_df.withColumn(
    "customer_cluster",
    when(col("total_spent") > 1200, "High Value")
    .when(col("total_spent") > 600, "Medium Value")
    .otherwise("Low Value")
)


result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.customer_segments")

print("✅ Customer segmentation completed...!")


