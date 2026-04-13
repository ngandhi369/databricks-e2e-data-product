import logging

logger = logging.getLogger(__name__)

def get_spark():
    """
    Returns a SparkSession appropriate for the current environment.

    - In a Databricks job or interactive cluster: returns a DatabricksSession
      (databricks-connect), which routes compute to the remote serverless cluster.
    - In local development without databricks-connect configured: falls back to
      a local SparkSession so unit tests and scripts can still run.
    """
  
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile("DEFAULT").serverless(True).getOrCreate()
        logger.info("🔗 Using Databricks Connect (serverless)")
        return spark

    except ImportError:
        # databricks-connect is not installed — expected in local/CI environments
        logger.warning("databricks-connect not installed, falling back to local SparkSession")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("city-order-local") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")   # suppress noisy Spark INFO logs locally
        logger.info("⚡ Using local SparkSession")
        return spark

    except Exception as e:
        # databricks-connect is installed but misconfigured (bad profile, expired token, etc.)
        # — surface the real error rather than silently falling back
        logger.error(f"Databricks Connect failed to initialise: {e}")
        raise
