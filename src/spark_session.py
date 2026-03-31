# try first for local SparkSession using databricks-connect, if it fails (e.g. not running in Databricks), fallback to regular SparkSession
def get_spark():
    try:
        from databricks.connect import DatabricksSession
        print("🔗 Using Databricks Connect")
        return DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()
    except:
        from pyspark.sql import SparkSession
        print("⚡ Using SparkSession")
        return SparkSession.builder.getOrCreate()