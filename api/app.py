from fastapi import FastAPI, Query, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
import os
from pydantic import BaseModel, Field
from typing import Optional
from functools import lru_cache
from databricks.connect import DatabricksSession

API_KEY_NAME = "nirdosh-dab-key"
API_KEY = os.getenv("API_KEY", "nirdosh0369")  # fallback for now

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


class TopCustomerRequest(BaseModel):
    city: Optional[str] = Field(None, description="Filter by city")
    limit: int = Field(10, ge=1, le=100, description="Number of top customers to return")


@lru_cache(maxsize=50)
def get_top_customers_cached(city, limit, offset):
    query = f"""
        SELECT customer_id, city, total_spent
        FROM {CATALOG}.{SCHEMA}.customer_segments
    """
    if city:
        query += f" WHERE city = '{city}'"
    query += f" ORDER BY total_spent DESC LIMIT {limit} OFFSET {offset}"

    df = spark.sql(query)
    return [row.asDict() for row in df.collect()]


def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    raise HTTPException(status_code=403, detail="Invalid API Key")

app = FastAPI()

# Create spark session
spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()

CATALOG = "nirdosh_catalog_dev"
SCHEMA = "nirdosh_schema_dev"



@app.get("/top-customers")
def top_customers(
    api_key: str = Security(get_api_key),
    city: Optional[str] = None,
    limit: int = 10,
    offset: int = 0
):
    try:
        result = get_top_customers_cached(city, limit, offset)

        return {
            "status": "success",
            "cached": True,
            "count" : len(result),
            "data": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}



@app.get("/customer-segments")
def customer_segments(cluster: Optional[str] = None):
    query = f"""
        SELECT customer_cluster, COUNT(*) as count
        FROM {CATALOG}.{SCHEMA}.customer_segments
    """

    if cluster:
        query += f" WHERE customer_cluster = '{cluster}'"
    query += " GROUP BY customer_cluster"

    df = spark.sql(query)
    return [row.asDict() for row in df.collect()]



@app.get("/customer/{customer_id}")
def get_customer(customer_id: int):
    df = spark.sql(f"""
        SELECT *
        FROM {CATALOG}.{SCHEMA}.customer_segments
        WHERE customer_id = {customer_id}
    """)
    result = df.collect()

    if not result:
        return {"message": "Customer not found"}
    return result[0].asDict()


@app.get("/")
def health():
    return {"status": "API is running 🚀"}


@app.get("/revenue-by-city")
def revenue_by_city():
    try:
        df = spark.sql(f"""
            SELECT city, SUM(total_spent) as revenue
            FROM {CATALOG}.{SCHEMA}.customer_segments
            GROUP BY city
        """)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        return {"error": str(e)}
