import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from fastapi import FastAPI
from databricks.connect import DatabricksSession
from src.config import get_config

app = FastAPI()

# Create spark session
spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()

# CATALOG = "main"
# SCHEMA = "city_order_dev"

config = get_config()

catalog = config["catalog"]
schema = config["schema"]


@app.get("/top-customers")
def top_customers():
    df = spark.sql(f"""
        SELECT customer_id, city, total_spent
        FROM {CATALOG}.{SCHEMA}.gold_customer_metrics
        ORDER BY total_spent DESC
        LIMIT 10
    """)

    return [row.asDict() for row in df.collect()]



@app.get("/customer-segments")
def customer_segments():
    df = spark.sql(f"""
        SELECT customer_cluster, COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.gold_customer_segments
        GROUP BY customer_cluster
    """)

    return [row.asDict() for row in df.collect()]



@app.get("/revenue-by-city")
def revenue_by_city():
    df = spark.sql(f"""
        SELECT city, SUM(total_spent) as revenue
        FROM {CATALOG}.{SCHEMA}.gold_customer_metrics
        GROUP BY city
    """)

    return [row.asDict() for row in df.collect()]