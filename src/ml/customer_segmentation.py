import sys
import os
import inspect

try:
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    _file = inspect.currentframe().f_code.co_filename
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_file))))

sys.path.insert(0, _root)

import mlflow
import mlflow.spark
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline as MLPipeline
from pyspark.ml.evaluation import ClusteringEvaluator

from src.config import get_config
from src.spark_session import get_spark

# SETUP:
spark = get_spark()
config = get_config()
catalog = config["catalog"]
schema = config["schema"]
volume_path = config["volume_path"]
mlflow_tmp_dir = volume_path.replace("dbfs:", "").rstrip("/") + "/mlflow_tmp"

FEATURE_COLS = ["total_orders", "total_spent", "avg_order_value", "days_since_last_order"]
N_CLUSTERS = 3
MODEL_NAME = f"{catalog}.{schema}.customer_segmentation_kmeans"


current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/customer_segmentation")


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
assembled_df = assembler.transform(feature_df)

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)
kmeans = KMeans(
    featuresCol="scaled_features",
    predictionCol="prediction",
    k=N_CLUSTERS,
    seed=42
)
pipeline = MLPipeline(stages=[scaler, kmeans])

# Fit main model
pipeline_model = pipeline.fit(assembled_df)

with mlflow.start_run(run_name="customer_segmentation_kmeans"):
    mlflow.log_param("k", N_CLUSTERS)
    mlflow.log_param("features", str(FEATURE_COLS))
    mlflow.log_param("seed", 42)
    mlflow.log_param("metric", "silhouette")

    mlflow.spark.log_model(
        pipeline_model,
        artifact_path="kmeans_pipeline",
        registered_model_name=MODEL_NAME,
        dfs_tmpdir=mlflow_tmp_dir,
    )
    print(f"✅ Model logged and registered as '{MODEL_NAME}'")

    clustered_eval_df = pipeline_model.transform(assembled_df)

    evaluator = ClusteringEvaluator(
        predictionCol="prediction",
        featuresCol="scaled_features",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )
    silhouette = evaluator.evaluate(clustered_eval_df)
    mlflow.log_metric("silhouette_score", silhouette)
    print(f"✅ Silhouette score (k={N_CLUSTERS}): {silhouette:.4f}")
    

    # ── Elbow method ──────────────────────────────────────────────────────
    print("📊 Running elbow method...")
    for k in range(2, 7):
        temp_pipeline = MLPipeline(stages=[
            StandardScaler(inputCol="features", outputCol="scaled_features",
                           withStd=True, withMean=True),
            KMeans(featuresCol="scaled_features", k=k, seed=42)
        ])
        temp_model = temp_pipeline.fit(assembled_df)
        score = evaluator.evaluate(temp_model.transform(assembled_df))
        mlflow.log_metric("elbow_silhouette", score, step=k)
        print(f"  k={k}  silhouette={score:.4f}")


# Predict:

# Run inference with the fitted pipeline
clustered_df = pipeline_model.transform(assembled_df) # adds `prediction` column (0, 1, 2)

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
    "High Value": cluster_means[0]["prediction"],
    "Medium Value": cluster_means[1]["prediction"],
    "Low Value": cluster_means[2]["prediction"],
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
