# test_connect.py
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()

print(spark.range(5).collect())