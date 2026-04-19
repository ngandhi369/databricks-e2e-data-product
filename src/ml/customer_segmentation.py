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
import mlflow.sklearn
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.metrics import silhouette_score
from src.config import get_config
from src.spark_session import get_spark
from mlflow.models.signature import infer_signature

# SETUP
spark = get_spark()
config = get_config()
catalog = config["catalog"]
schema  = config["schema"]

FEATURE_COLS = ["total_orders", "total_spent", "avg_order_value", "days_since_last_order"]
N_CLUSTERS   = 3
MODEL_NAME   = f"{catalog}.{schema}.customer_segmentation_kmeans"

current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/customer_segmentation")

# Load Data
gold_orders_df = spark.read.table(f"{catalog}.{schema}.gold_orders")
feature_df = (
    gold_orders_df
    .select("customer_id", *FEATURE_COLS)
    .dropna()
    .fillna(0)
)

# Collect to pandas — sklearn serializes cleanly, no Spark Connect ML cache issues
feature_pdf = feature_df.toPandas()
X = feature_pdf[FEATURE_COLS].values

# Build sklearn pipeline
sk_pipeline = SklearnPipeline([
    ("scaler", StandardScaler(with_std=True, with_mean=True)),
    ("kmeans", KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)),
])

# Train + Log
with mlflow.start_run(run_name="customer_segmentation_kmeans"):
    mlflow.log_param("k", N_CLUSTERS)
    mlflow.log_param("features", str(FEATURE_COLS))
    mlflow.log_param("seed", 42)
    mlflow.log_param("metric", "silhouette")

    sk_pipeline.fit(X)
    labels = sk_pipeline.predict(X)
    signature = infer_signature(X, labels)

    # Registers cleanly in UC Model Registry — no dfs_tmpdir needed
    mlflow.sklearn.log_model(
        sk_pipeline,
        artifact_path="kmeans_pipeline",
        registered_model_name=MODEL_NAME,
        signature = signature,
        input_example = X[:5],
    )
    print(f"✅ Model logged and registered as '{MODEL_NAME}'")

    # Evaluate
    X_scaled = sk_pipeline.named_steps["scaler"].transform(X)
    sil = silhouette_score(X_scaled, labels)
    mlflow.log_metric("silhouette_score", sil)
    print(f"✅ Silhouette score (k={N_CLUSTERS}): {sil:.4f}")

    # Elbow method (pure sklearn — no Spark Connect involved)
    print("📊 Running elbow method...")
    for k in range(2, 7):
        temp_labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(X_scaled)
        score = silhouette_score(X_scaled, temp_labels)
        mlflow.log_metric("elbow_silhouette", score, step=k)
        print(f"  k={k}  silhouette={score:.4f}")

# Post-processing: map clusters to value labels by avg spend
feature_pdf["prediction"] = labels
cluster_means = (
    feature_pdf.groupby("prediction")["total_spent"]
    .mean()
    .sort_values(ascending=False)
)
label_map = {
    "High Value":   int(cluster_means.index[0]),
    "Medium Value": int(cluster_means.index[1]),
    "Low Value":    int(cluster_means.index[2]),
}
print(f"📊 Cluster label mapping: {label_map}")

feature_pdf["customer_cluster"] = feature_pdf["prediction"].map(
    {v: k for k, v in label_map.items()}
)

# Bring predictions back to Spark and join
pred_spark_df = spark.createDataFrame(feature_pdf[["customer_id", "customer_cluster"]])
result_df     = gold_orders_df.join(pred_spark_df, on="customer_id", how="inner")

# Save
result_df.write.mode("overwrite") \
         .option("overwriteSchema", "true") \
         .saveAsTable(f"{catalog}.{schema}.customer_segments")
print("✅ Customer segmentation saved to customer_segments table")
