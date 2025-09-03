from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import logging


def get_spark(app_name: str, logger: logging.Logger) -> SparkSession:
    """
    Create or get a SparkSession. If running in Synapse, use minimal config.
    Otherwise, create with full local configs for Delta Lake.
    """
    logger.info(f"Starting SparkSession creation for app: {app_name}")

    # Log environment variables for debugging
    logger.info("Checking environment variables:")
    logger.info(f"  MMLSPARK_PLATFORM_INFO: {os.getenv('MMLSPARK_PLATFORM_INFO', 'Not set')}")
    logger.info(f"  AZURE_SERVICE: {os.getenv('AZURE_SERVICE', 'Not set')}")

    # Check if we're running in Azure Synapse
    is_synapse = os.getenv("MMLSPARK_PLATFORM_INFO") is not None or os.getenv("AZURE_SERVICE") is not None

    logger.info(f"Synapse environment detected: {is_synapse}")

    # Start with basic builder
    builder = SparkSession.builder.appName(app_name)

    if is_synapse:
        logger.info("Running in Synapse environment - using existing spark configuration")
    else:
        logger.info("Running in local/non-Synapse environment - using full local configuration")
        master = os.getenv("SPARK_MASTER", "local[*]")
        logger.info(f"  Using Spark master: {master}")

        # Full local configuration with Delta Lake support
        builder = (
            builder.master(master)
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.sql.session.timeZone", "UTC")
        )

    # Create or get the SparkSession
    try:
        logger.info("Creating/getting SparkSession...")
        spark = builder.getOrCreate()

        logger.info("Successfully created/retrieved SparkSession")
        logger.info(f"  Spark version: {spark.version}")
        logger.info(f"  Application ID: {spark.sparkContext.applicationId}")
        logger.info(f"  Application name: {spark.sparkContext.appName}")
        logger.info(f"  Master: {spark.sparkContext.master}")

        # Verify Delta Lake configurations are properly set
        logger.info("Verifying applied configurations:")
        logger.info(f"  spark.sql.extensions: {spark.conf.get('spark.sql.extensions', 'Not set')}")
        logger.info(f"  spark.sql.catalog.spark_catalog: {spark.conf.get('spark.sql.catalog.spark_catalog', 'Not set')}")

        # Log any additional configurations for debugging
        logger.info("Additional Spark configurations:")
        conf = spark.sparkContext.getConf()
        for key, value in conf.getAll():
            logger.info(f"  {key}: {value}")

        if not is_synapse:
            logger.info(f"  spark.driver.bindAddress: {spark.conf.get('spark.driver.bindAddress', 'Not set')}")
            logger.info(f"  spark.sql.files.ignoreCorruptFiles: {spark.conf.get('spark.sql.files.ignoreCorruptFiles', 'Not set')}")
            logger.info(f"  spark.sql.session.timeZone: {spark.conf.get('spark.sql.session.timeZone', 'Not set')}")

        return spark

    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise e
