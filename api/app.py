from fastapi import FastAPI
from databricks.connect import DatabricksSession

app = FastAPI()

# Create spark session
spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()

CATALOG = "nirdosh_catalog_dev"
SCHEMA = "nirdosh_schema_dev"

@app.get("/top-customers")
def top_customers():
    try:
        print("🚀 API called")

        df = spark.sql(f"""
            SELECT customer_id, city, total_spent
            FROM {CATALOG}.{SCHEMA}.customer_segments
            ORDER BY total_spent DESC
            LIMIT 10
        """)

        result = [row.asDict() for row in df.collect()]

        print("✅ Query success")
        return result

    except Exception as e:
        print("❌ ERROR:", str(e))
        return {"error": str(e)}
    


@app.get("/customer-segments")
def customer_segments():
    df = spark.sql(f"""
        SELECT customer_cluster, COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.customer_segments
        GROUP BY customer_cluster
    """)

    return [row.asDict() for row in df.collect()]



@app.get("/revenue-by-city")
def revenue_by_city():
    df = spark.sql(f"""
        SELECT city, SUM(total_spent) as revenue
        FROM {CATALOG}.{SCHEMA}.customer_segments
        GROUP BY city
    """)

    return [row.asDict() for row in df.collect()]