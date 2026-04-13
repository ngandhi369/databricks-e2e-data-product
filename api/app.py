from unittest import result

from click import Option
from fastapi import FastAPI, Query, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
import os
from pydantic import BaseModel, Field
from typing import Optional
from functools import lru_cache
from databricks.connect import DatabricksSession

# Auth:

API_KEY_NAME = "nirdosh-dab-key"
# API_KEY = os.getenv("API_KEY", "nirdosh0369")  # fallback for now
API_KEY = os.environ["API_KEY"]

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, 
        detail="Invalid or missing API key"
    )


# App & Spark:

app = FastAPI()

spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()

CATALOG = "nirdosh_catalog_dev"
SCHEMA = "nirdosh_schema_dev"


# Cache helper:

@lru_cache(maxsize=50)
def _query_top_customers(city: Optional[str], limit: int, offset: int) -> list[dict]:

    from pyspark.sql.functions import col, lit
    df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer_segments")\
        .select("customer_id", "city", "total_spent")

    if city:
        df = df.filter(col("city") == lit(city))

    df = df.orderBy(col("total_spent").desc()).limit(limit + offset)

    rows = df.collect()
    return [row.asDict() for row in rows[offset:]]


# ENDPOINTS:

@app.get("/top-customers")
def top_customers(
    api_key: str = Security(get_api_key),
    city: Optional[str] = None,
    limit: int = Query(default=10, ge=1, le=100),  # limit between 1 and 100
    offset: int = Query(default=0, ge=0),
):
    try:
        before = _query_top_customers.cache_info().hits
        result = _query_top_customers(city, limit, offset)
        after = _query_top_customers.cache_info().hits

        # Check if the result was served from cache
        was_cached = after > before

        return {
            "status": "success",
            "cached": was_cached,
            "count" : len(result),
            "data": result
        }
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))



@app.get("/customer-segments")
def customer_segments(
    api_key: str = Security(get_api_key), 
    cluster: Optional[str] = None,
):

    from pyspark.sql.functions import col, lit, count as _count

    df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer_segments")\
        .groupBy("customer_cluster")

    if cluster:
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer_segments")\
            .filter(col("customer_cluster") == lit(cluster))\
            .groupBy("customer_cluster")
     
    result = df.agg(_count("*").alias("count"))
    return [row.asDict() for row in result.collect()]



@app.get("/customer/{customer_id}")
def get_customer(
    customer_id: int,
    api_key: str = Security(get_api_key),
):
    from pyspark.sql.functions import col, lit
    df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer_segments")\
        .filter(col("customer_id") == lit(customer_id))
    
    result = df.collect()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_100_CONTINUE,
            detail=f"Customer: {customer_id} not found"
            )
    return result[0].asDict()


@app.get("/revenue-by-city")
def revenue_by_city(
    api_key: str = Security(get_api_key),
):
    from pyspark.sql.functions import col, lit, sum as _sum
    
    df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer_segments")\
        .groupBy("city")\
        .agg(_sum("total_spent").alias("revenue"))
    return [row.asDict() for row in df.collect()]


@app.get("/")
def health():
    try:
        # Simple query to check if Spark session is healthy
        spark.sql("SELECT 1").collect()
        return {"status": "healthy", "spark": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Spark Unavailable: {e}")

