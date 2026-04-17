# 🏗️ Databricks End-to-End Data Product

![CI/CD](https://img.shields.io/github/actions/workflow/status/ngandhi369/databricks-e2e-data-product/deploy.yml?label=CI%2FCD&logo=githubactions)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundle-red?logo=databricks)
![FastAPI](https://img.shields.io/badge/API-FastAPI-green?logo=fastapi)
![Render](https://img.shields.io/badge/Hosted%20on-Render-46E3B7?logo=render)
![MLflow](https://img.shields.io/badge/ML-MLflow-blue?logo=mlflow)

> A production-grade, end-to-end data product built entirely on Databricks — from raw CSV ingestion through to a live ML-powered REST API. Every component is automated, version-controlled, and deployed via CI/CD.

---

## ⚡ What This Project Does — In 30 Seconds

1. **Ingests** raw city order data from a CSV file into a Unity Catalog Volume
2. **Transforms** it through a Bronze → Silver → Gold medallion lakehouse
3. **Validates** data quality with schema, null, range and business rule checks
4. **Segments** customers into High / Medium / Low Value using KMeans ML (logged in MLflow)
5. **Serves** the results via a live FastAPI deployed on Render.com
6. **Deploys** everything automatically via GitHub Actions on every push to master

No manual steps. No notebooks. Pure production Python.

---

## 🏛️ Architecture

```
                        ┌─────────────────────────────────────┐
                        │         GitHub Actions CI/CD         │
                        │  lint → test → deploy → run pipeline │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
 orders.csv ──► DBFS Volume ──► Bronze ──► Silver ──► Gold ──► Validation
                                                         │
                                                         ▼
                                                  ML Segmentation
                                                  (KMeans + MLflow)
                                                         │
                                                         ▼
                                                 customer_segments
                                                  (Unity Catalog)
                                                         │
                                                         ▼
                                              FastAPI on Render.com
                                              /top-customers
                                              /customer-segments
                                              /revenue-by-city
                                              /customer/{id}

 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 Parallel DLT Pipeline (declarative):
 bronze_orders_dlt → silver_orders_dlt → validated_orders_dlt → customer_metrics_dlt
```

---

## 🗂️ Project Structure

```
databricks-e2e-data-product/
│
├── .github/
│   └── workflows/
│       └── deploy.yml              # CI/CD pipeline — 4 jobs, 2 environments
│
├── api/
│   └── app.py                      # FastAPI app with auth, caching, Spark Connect
│
├── data/
│   └── orders.csv                  # Sample source data (city order transactions)
│
├── resources/
│   ├── jobs/
│   │   └── pipeline.yml            # 4-task Databricks Job definition
│   └── pipelines/
│       └── pipeline.yml            # Delta Live Tables pipeline definition
│
├── src/
│   ├── ingestion/
│   │   └── ingest_orders.py        # CSV → DBFS Volume → bronze_orders (Delta MERGE)
│   ├── transformation/
│   │   └── transform_orders.py     # Bronze → Silver (dedup) → Gold (aggregates)
│   ├── validation/
│   │   └── validate_orders.py      # Schema + null + range + business rule checks
│   ├── ml/
│   │   └── customer_segmentation.py  # KMeans pipeline + MLflow + UC Model Registry
│   ├── dlt/
│   │   └── pipeline.py             # DLT medallion pipeline with @dlt.expect rules
│   ├── config.py                   # CLI argument parser for job parameters
│   └── spark_session.py            # Databricks Connect / local Spark fallback
│
├── tests/
│   └── test_connection.py          # Spark session smoke tests (runs in CI)
│
├── databricks.yml                  # Asset Bundle — dev + prod targets, variables
├── render.yml                      # Render.com web service config
├── requirements.txt                # Production dependencies (pinned)
├── pyproject.toml                  # Project metadata, ruff config, pytest config
└── start.sh                        # Uvicorn startup command for Render
```

---

## 🔁 Data Pipeline — Step by Step

### Task 1 — Ingest

- Copies `orders.csv` from the deployed workspace bundle to a Unity Catalog Volume
- Reads the CSV, applies explicit type casting (`id → int`, `amount → double`, `order_date → date`)
- Writes to `bronze_orders` using a **Delta MERGE** (idempotent — safe to re-run)
- Emits `bronze_row_count` as a task value for downstream observability

### Task 2 — Transform

- Reads `bronze_orders`, deduplicates on `(customer_id, order_date, amount)` using a window function
- Adds `order_month` and `order_year` derived columns → writes to `silver_orders` (MERGE)
- Computes per-customer aggregates: `total_orders`, `total_spent`, `avg_order_value`, `days_since_last_order`
- Adds `customer_segment`, `city_rank`, `overall_rank` → writes to `gold_orders` (MERGE)
- All writes use **Delta MERGE** to preserve time travel history

### Task 3 — Validate

- **Schema check** — verifies all expected columns exist with correct types
- **Null check** — zero tolerance on `customer_id`, `city`, `total_spent`, `total_orders`
- **Range checks** — `total_spent > 0`, ranks ≥ 1, `customer_segment` is a valid label
- Raises `ValueError` on failure (stops the job, prevents ML training on bad data)
- Emits `gold_row_count` and `segment_dist` as task values

### Task 4 — ML Segmentation

- Reads `gold_orders`, builds a Spark ML Pipeline: `VectorAssembler → StandardScaler → KMeans`
- Trains KMeans (k=3) on `total_orders`, `total_spent`, `avg_order_value`, `days_since_last_order`
- Evaluates with **silhouette score** (serverless-safe — avoids Spark Connect ML cache issues)
- Runs elbow method (k=2–6), logs all metrics to **MLflow**
- Registers the full pipeline model in **Unity Catalog Model Registry**
- Maps cluster IDs to business labels (High / Medium / Low Value) dynamically by average spend
- Writes final labels to `customer_segments`

---

## 🌊 Delta Live Tables Pipeline

A declarative, parallel pipeline in `src/dlt/pipeline.py` that implements the same medallion logic using DLT's managed runtime:

| Table | Layer | Description |
|---|---|---|
| `bronze_orders_dlt` | Bronze | Raw CSV ingested from Volume |
| `silver_orders_dlt` | Silver | Type-cast, null-dropped orders |
| `validated_orders_dlt` | Silver+ | DLT expectations: `amount > 0`, `customer_id IS NOT NULL` |
| `customer_metrics_dlt` | Gold | Per-customer aggregates |

DLT handles incremental processing, data quality metrics, and pipeline lineage automatically in the Databricks UI.

---

## 🌐 REST API

Deployed on **Render.com** at `https://nirdosh-databricks-api.onrender.com`. Connects to Databricks Serverless via `databricks-connect` for live query execution.

All endpoints (except `/`) require the API key header:

```
nirdosh-dab-key: <your-api-key>
```

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Health check — pings Spark connectivity |
| `GET` | `/top-customers` | Top customers by spend. Params: `city`, `limit`, `offset` |
| `GET` | `/customer-segments` | Segment distribution. Params: `cluster` |
| `GET` | `/customer/{customer_id}` | Full profile for a single customer |
| `GET` | `/revenue-by-city` | Total revenue aggregated by city |

**Example:**

```bash
curl -H "nirdosh-dab-key: your-api-key" \
     "https://nirdosh-databricks-api.onrender.com/top-customers?city=Mumbai&limit=5"
```

---

## 🤖 ML Model

**Algorithm:** KMeans clustering (PySpark MLlib)

**Features:**

| Feature | Description |
|---|---|
| `total_orders` | Number of orders placed |
| `total_spent` | Cumulative spend amount |
| `avg_order_value` | Average spend per order |
| `days_since_last_order` | Customer recency |

**Pipeline stages:** `VectorAssembler → StandardScaler (μ=0, σ=1) → KMeans (k=3)`

**Evaluation metric:** Silhouette score (range -1 to 1; higher = better-separated clusters)

**Output segments:**

| Segment | Criteria |
|---|---|
| High Value | Cluster with highest average `total_spent` |
| Medium Value | Cluster with mid average `total_spent` |
| Low Value | Cluster with lowest average `total_spent` |

The model is registered in Unity Catalog under `{catalog}.{schema}.customer_segmentation_kmeans` and every training run is tracked in MLflow with params, silhouette score, and elbow curve.

---

## ⚙️ CI/CD

Defined in `.github/workflows/deploy.yml` with four jobs:

```
lint_and_validate
    ├──► deploy_dev ──► run_pipeline_dev    (on push to master or manual dispatch → DEV)
    └──► deploy_prod                        (on release or manual dispatch → PROD)
```

| Trigger | Jobs that run |
|---|---|
| Push to `master` | Lint + Test → Deploy Dev → Run Pipeline |
| `workflow_dispatch` → DEV | Lint + Test → Deploy Dev → Run Pipeline |
| `workflow_dispatch` → PROD | Lint + Test → Deploy Prod (with approval gate) |
| GitHub Release published | Lint + Test → Deploy Prod (with approval gate) |

The `lint_and_validate` job runs `ruff` for linting and `pytest` for unit tests using a local SparkSession — no Databricks credentials needed. Only the deploy jobs require secrets.

---

## 🚀 Setup & Local Development

### Prerequisites

- Python 3.10–3.12
- Databricks workspace with Unity Catalog enabled
- Databricks CLI (`brew install databricks` or see [docs](https://docs.databricks.com/dev-tools/cli/index.html))

### 1. Clone and install

```bash
git clone https://github.com/ngandhi369/databricks-e2e-data-product
cd databricks-e2e-data-product
pip install -r requirements.txt
pip install ruff pytest pytest-cov
```

### 2. Configure Databricks CLI

```bash
databricks configure --token
# Enter your workspace URL and personal access token
```

### 3. Validate and deploy

```bash
# Validate the bundle config
databricks bundle validate --target dev

# Deploy resources to dev workspace
databricks bundle deploy --target dev

# Run the full pipeline
databricks bundle run city_order_pipeline --target dev
```

### 4. Run tests locally

```bash
pytest tests/ -v --tb=short
```

---

## 🔐 GitHub Actions Setup

Create two environments in **Settings → Environments**:

**`databricks-dev`**

| Type | Name | Value |
|---|---|---|
| Secret | `DATABRICKS_TOKEN` | Personal access token |
| Variable | `DATABRICKS_HOST` | `https://dbc-14ad825f-2de6.cloud.databricks.com` |

**`databricks-prod`**

| Type | Name | Value |
|---|---|---|
| Secret | `DATABRICKS_TOKEN_PROD` | Service principal token |
| Variable | `DATABRICKS_HOST` | Prod workspace URL |
| Reviewers | Add required reviewers | Enforces manual approval before prod deploy |

---

## 🌿 Render.com Setup

Set these environment variables in your Render service dashboard (never commit them):

```
API_KEY              = <your chosen API key>
DATABRICKS_HOST      = https://dbc-14ad825f-2de6.cloud.databricks.com
DATABRICKS_TOKEN     = <your Databricks PAT>
DATABRICKS_CLUSTER_ID = <your serverless cluster ID>
```

---

## 📦 Unity Catalog Resources

The following resources must exist in your workspace before deployment:

| Environment | Catalog | Schema | Volume |
|---|---|---|---|
| Dev | `nirdosh_catalog_dev` | `nirdosh_schema_dev` | `nirdosh_volume_dev` |
| Prod | `nirdosh_catalog_prod` | `nirdosh_schema_prod` | `nirdosh_volume_prod` |

Tables created by the pipeline:

| Table | Layer |
|---|---|
| `bronze_orders` | Bronze |
| `silver_orders` | Silver |
| `gold_orders` | Gold |
| `customer_segments` | ML Output |
| `bronze_orders_dlt` | DLT Bronze |
| `silver_orders_dlt` | DLT Silver |
| `validated_orders_dlt` | DLT Silver+ |
| `customer_metrics_dlt` | DLT Gold |

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Bundle & Deployment | Databricks Asset Bundles (DAB) |
| Compute | Databricks Serverless (Spark Connect) |
| Storage | Unity Catalog · Delta Lake · DBFS Volumes |
| Streaming | Delta Live Tables (DLT) |
| Transformation | PySpark |
| Machine Learning | PySpark MLlib · KMeans · MLflow |
| Model Registry | Unity Catalog Model Registry |
| AI Assistant | Databricks Genie |
| API Framework | FastAPI |
| API Hosting | Render.com |
| CI/CD | GitHub Actions |
| Linting | Ruff |
| Testing | Pytest |

---

## 👤 Author

**Nirdosh Gandhi** — [nirdoshgandhi02@gmail.com](mailto:nirdoshgandhi02@gmail.com)

GitHub: [@ngandhi369](https://github.com/ngandhi369)
