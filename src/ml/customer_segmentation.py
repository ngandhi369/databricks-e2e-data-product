import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

import mlflow
import mlflow.spark
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline as MLPipeline

from src.config import get_config
from src.spark_session import get_spark

# SETUP:

spark = get_spark()
config = get_config()
catalog = config["catalog"]
schema = config["schema"]

FEATURE_COLS = ["total_orders", "total_spent", "avg_order_value", "days_since_last_order"]
N_CLUSTERS = 3
MODEL_NAME = f"{catalog}.{schema}.customer_segmentation_kmeans"


mlflow.set_experiment(f"/Shared/{catalog}/customer_segmentation")


# Load Data:

gold_orders_df = spark.read.table(f"{catalog}.{schema}.gold_orders")

feature_df = (
    gold_orders_df
    .select("customer_id", *FEATURE_COLS)
    .dropna()
    .fillna(0)
)


# Feature Engineering:

assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)
kmeans = KMeans(featuresCol="scaled_features", predictionCol="prediction", k=N_CLUSTERS, seed=42)


pipeline = MLPipeline(stages=[assembler, scaler, kmeans])


# Train + Log:

with mlflow.start_run(run_name="customer_segmentation_kmeans"):

    # Log params
    mlflow.log_param("k", N_CLUSTERS)
    mlflow.log_param("features", FEATURE_COLS)
    mlflow.log_param("seed", 42)
    mlflow.log_param("scaler", "StandardScaler(withStd=True, withMean=True)")

    # Fit the full pipeline (assembler → scaler → kmeans)
    pipeline_model = pipeline.fit(feature_df)

    # Training cost of the final model
    kmeans_model = pipeline_model.stages[-1]          # last stage is the fitted KMeans
    training_cost  = kmeans_model.summary.trainingCost
    mlflow.log_metric("training_cost", training_cost)
    print(f"✅ Training cost (k={N_CLUSTERS}): {training_cost:.4f}")

    # Elbow Method:
    print("📊 Running elbow method...")
    for k in range(2, 7):
        temp_pipeline = MLPipeline(stages=[assembler, scaler,
                            KMeans(featuresCol="scaled_features", k=k, seed=42)])
        temp_model = temp_pipeline.fit(feature_df)
        cost = temp_model.stages[-1].summary.trainingCost
        mlflow.log_metric("elbow_cost", cost, step=k)   
        print(f"  k={k}  cost={cost:.4f}")

    # Log & Register Model:
    mlflow.spark.log_model(
        pipeline_model,
        artifact_path="kmeans_pipeline",
        registered_model_name=MODEL_NAME,
    )
    print(f"✅ Model logged and registered as '{MODEL_NAME}'")


# Predict:

# Run inference with the fitted pipeline
clustered_df = pipeline_model.transform(feature_df)   # adds `prediction` column (0, 1, 2)

# Post-processing:

result_df = gold_orders_df.join(
    clustered_df.select("customer_id", "prediction"),
    on="customer_id",
    how="inner"
)


cluster_means = (
    result_df.groupBy("prediction")
             .agg({"total_spent": "mean"})
             .orderBy("avg(total_spent)", ascending=False)   # highest spend = rank 0
             .collect()
)

# Build a dynamic mapping: cluster with highest avg spend → "High Value", etc.
label_map = {
    cluster_means[0]["prediction"]: "High Value",
    cluster_means[1]["prediction"]: "Medium Value",
    cluster_means[2]["prediction"]: "Low Value",
}
print(f"📊 Cluster label mapping: {label_map}")

# Apply mapping using when/otherwise
result_df = result_df.withColumn(
    "customer_cluster",
    when(col("prediction") == label_map["High Value"],   "High Value")
    .when(col("prediction") == label_map["Medium Value"], "Medium Value")
    .otherwise("Low Value")
)

# Save:

result_df.drop("prediction") \
         .write.mode("overwrite") \
         .option("overwriteSchema", "true") \
         .saveAsTable(f"{catalog}.{schema}.customer_segments")

print("✅ Customer segmentation saved to customer_segments table")
