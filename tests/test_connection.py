"""
Smoke test — verifies that the Spark session can be obtained and
that a basic query executes successfully.

Run locally:
    pytest tests/test_connection.py -v

Run in CI (add to deploy.yml lint_and_validate job):
    pytest tests/ -v --tb=short
"""

from src.spark_session import get_spark


def test_spark_session_returns():
    """get_spark() should always return a non-None SparkSession."""
    spark = get_spark()
    assert spark is not None


def test_spark_basic_query():
    """A trivial SQL query should execute and return expected rows."""
    spark = get_spark()
    result = spark.range(5).collect()
    assert len(result) == 5
    assert result[0][0] == 0
    assert result[4][0] == 4


def test_spark_sql_select_one():
    """SELECT 1 should return a single row with value 1."""
    spark = get_spark()
    row = spark.sql("SELECT 1 AS value").collect()
    assert len(row) == 1
    assert row[0]["value"] == 1
