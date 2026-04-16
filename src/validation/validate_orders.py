import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from pyspark.sql.functions import col, count, when, isnull
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from src.config import get_config
from src.spark_session import get_spark

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    pass

spark  = get_spark()
config = get_config()
catalog = config["catalog"]
schema  = config["schema"]

gold_table = f"{catalog}.{schema}.gold_orders"
gold_df    = spark.read.table(gold_table)

print(f"🔍 Running validation on {gold_table}...")

# 1. Row Count Check:

total_rows = gold_df.count()
print(f"📊 Total rows: {total_rows}")

if total_rows == 0:
    dbutils.jobs.taskValues.set(key="status",     value="failed")
    dbutils.jobs.taskValues.set(key="fail_reason", value="gold_orders is empty")
    raise ValueError("❌ Validation failed: gold_orders table is empty")

# 2. Schema Validation:

EXPECTED_SCHEMA = {
    "customer_id": IntegerType(),
    "city": StringType(),
    "total_orders": LongType(),
    "total_spent": DoubleType(),
    "avg_order_value": DoubleType(),
    "days_since_last_order": IntegerType(),
    "customer_segment": StringType(),
    "city_rank": IntegerType(),
    "overall_rank": IntegerType(),
}

actual_fields = {f.name: type(f.dataType) for f in gold_df.schema.fields}
schema_errors = []

for col_name, expected_type in EXPECTED_SCHEMA.items():
    if col_name not in actual_fields:
        schema_errors.append(f"Missing column: {col_name}")
   elif not isinstance(gold_df.schema[col_name].dataType, type(expected_type)):
        schema_errors.append(
            f"Type mismatch on '{col_name}': "
            f"expected {type(expected_type).__name__}, "
            f"got {type(gold_df.schema[col_name].dataType).__name__}"
        )

if schema_errors:
    for err in schema_errors:
        print(f"❌ Schema error: {err}")
    dbutils.jobs.taskValues.set(key="status",     value="failed")
    dbutils.jobs.taskValues.set(key="fail_reason", value="; ".join(schema_errors))
    raise ValueError(f"Schema validation failed: {schema_errors}")

print("✅ Schema validation passed")

# 3. Null Rate Check:

NULL_THRESHOLD = 0.0   # zero tolerance for these columns
CRITICAL_COLS  = ["customer_id", "city", "total_spent", "total_orders"]

null_counts = gold_df.select([
    count(when(isnull(c), c)).alias(c) for c in CRITICAL_COLS
]).collect()[0].asDict()

null_errors = []
for col_name, null_n in null_counts.items():
    null_rate = null_n / total_rows
    print(f" Null rate [{col_name}]: {null_rate:.2%} ({null_n} rows)")
    if null_rate > NULL_THRESHOLD:
        null_errors.append(f"{col_name} has {null_n} nulls ({null_rate:.2%})")

if null_errors:
    dbutils.jobs.taskValues.set(key="status",     value="failed")
    dbutils.jobs.taskValues.set(key="fail_reason", value="; ".join(null_errors))
    raise ValueError(f"❌ Null check failed: {null_errors}")

print("✅ Null checks passed")

# 4. Range / Business Rule Checks:

range_checks = {
    "total_spent must be > 0": gold_df.filter(col("total_spent") <= 0).count(),
    "total_orders must be > 0": gold_df.filter(col("total_orders") <= 0).count(),
    "avg_order_value must be > 0": gold_df.filter(col("avg_order_value") <= 0).count(),
    "city_rank must be >= 1": gold_df.filter(col("city_rank") < 1).count(),
    "overall_rank must be >= 1": gold_df.filter(col("overall_rank") < 1).count(),
    "customer_segment must be valid": gold_df.filter(
        ~col("customer_segment").isin("High Value", "Medium Value", "Low Value")
    ).count(),
}

range_errors = []
for rule, bad_count in range_checks.items():
    status_icon = "✅" if bad_count == 0 else "❌"
    print(f"  {status_icon} {rule}: {bad_count} violations")
    if bad_count > 0:
        range_errors.append(f"{rule} — {bad_count} violations")

if range_errors:
    dbutils.jobs.taskValues.set(key="status",     value="failed")
    dbutils.jobs.taskValues.set(key="fail_reason", value="; ".join(range_errors))
    raise ValueError(f"❌ Range checks failed: {range_errors}")

print("✅ Range checks passed")

# 5. Emit Quality Metrics as Task Values:

segment_dist = {
    row["customer_segment"]: row["count"]
    for row in gold_df.groupBy("customer_segment")
                      .count()
                      .collect()
}
print(f"📊 Segment distribution: {segment_dist}")

dbutils.jobs.taskValues.set(key="status", value="success")
dbutils.jobs.taskValues.set(key="gold_row_count", value=total_rows)
dbutils.jobs.taskValues.set(key="segment_dist", value=str(segment_dist))

print(f"✅ All validation checks passed — {total_rows} rows ready for ML")
