import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))

from src.config import get_config
from src.spark_session import get_spark

try:
    from databricks.sdk.runtime import dbutils   # works in jobs & interactive clusters
except ImportError:
    pass   # falls back to the globally injected dbutils when running on cluster

spark  = get_spark()
config = get_config()
catalog     = config["catalog"]
schema      = config["schema"]
volume_path = config["volume_path"]

# Locate source file:

# The correct pattern: pass the workspace file path as a job parameter and
# read it from the config. In pipeline.yml we pass ${workspace.file_path}
# as --workspace_file_path so this script never has to guess its location.

workspace_file_path = config.get("workspace_file_path", "")

# Build deterministic source path from the declared workspace root
source_file = f"{workspace_file_path}/data/orders.csv"
volume_file_path = f"{volume_path}orders.csv"

print(f"📂 Copying {source_file} → {volume_file_path}")
dbutils.fs.cp(source_file, volume_file_path, recurse=False)
print("✅ File copied to volume")

# Read into Bronze:

bronze_df = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(volume_file_path)
)

# Type-cast for consistency before writing to Delta
bronze_df = bronze_df.selectExpr(
    "cast(id as int) as id",
    "cast(customer_id as int) as customer_id",
    "cast(order_date as date) as order_date",
    "cast(amount as double) as amount",
    "city"
)

# Write to Bronze:

bronze_table = f"{catalog}.{schema}.bronze_orders"

# Strategy: MERGE on the natural key (id) so re-runs are idempotent.
# New rows are inserted, existing rows with changed data are updated,
# and nothing is deleted — giving you a clean, growing Bronze table.

bronze_df.createOrReplaceTempView("incoming_orders")

spark.sql(f"""
    MERGE INTO {bronze_table} AS target
    USING incoming_orders AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.amount <> source.amount  OR
        target.order_date <> source.order_date
    ) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ Bronze MERGE complete → {bronze_table}")

# Emit row count as a task value so downstream tasks can inspect it
row_count = spark.read.table(bronze_table).count()
print(f"📊 Bronze table total rows: {row_count}")
dbutils.jobs.taskValues.set(key="bronze_row_count", value=row_count)
