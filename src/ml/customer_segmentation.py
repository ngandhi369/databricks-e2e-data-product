import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

import mlflow
import mlflow.spark

from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

from src.config import get_config
from src.spark_session import get_spark

# -------------------- SETUP --------------------
spark = get_spark()
print("Spark:", spark)

config = get_config()
catalog = config["catalog"]
schema = config["schema"]

# -------------------- LOAD DATA --------------------
gold_orders_df = spark.read.table(f"{catalog}.{schema}.gold_orders")

feature_df = (
    gold_orders_df
    .select(
        "customer_id",
        "total_orders",
        "total_spent",
        "avg_order_value",
        "days_since_last_order"
    )
    .dropna()
    .fillna(0)
)

# -------------------- FEATURE ENGINEERING --------------------
assembler = VectorAssembler(
    inputCols=[
        "total_orders",
        "total_spent",
        "avg_order_value",
        "days_since_last_order"
    ],
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

# -------------------- ML TRAINING + MLFLOW --------------------
with mlflow.start_run(run_name="customer_segmentation"):

    # Train model
    kmeans = KMeans(featuresCol="scaled_features", k=3, seed=42)
    model = kmeans.fit(scaled_df)

    # Log params
    mlflow.log_param("k", 3)
    mlflow.log_param(
        "features",
        ["total_orders", "total_spent", "avg_order_value", "days_since_last_order"]
    )

    # Log model
    mlflow.spark.log_model(model, "kmeans_model")

    print("✅ Model logged to MLflow")

    # -------------------- ELBOW METHOD --------------------
    cost = []
    for k in range(2, 6):
        temp_model = KMeans(featuresCol="scaled_features", k=k, seed=42).fit(scaled_df)
        cost.append((k, temp_model.summary.trainingCost))

    print("📊 Elbow method data:", cost)

    # -------------------- PREDICTION --------------------
    clustered_df = model.transform(scaled_df)

# -------------------- POST PROCESSING --------------------
result_df = gold_orders_df.join(
    clustered_df.select("customer_id", "prediction"),
    on="customer_id",
    how="inner"
)

# Business-friendly labels
result_df = result_df.withColumn(
    "customer_cluster",
    when(col("total_spent") > 1200, "High Value")
    .when(col("total_spent") > 600, "Medium Value")
    .otherwise("Low Value")
)

# -------------------- SAVE OUTPUT --------------------
result_df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.customer_segments")

print("✅ Customer segmentation completed...!")