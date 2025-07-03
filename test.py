"""Test script to verify PySpark and Delta Lake are working correctly."""
 
from __future__ import annotations
 
import os
import shutil
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
 
# Set environment variables to fix possible initialization issues
# os.environ["PYSPARK_PYTHON"] = "/workspaces/dataops/.venv/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/workspaces/dataops/.venv/bin/python"
 
# Initialize a Spark session with Delta Lake support
spark = (
    SparkSession.builder.appName("SparkDeltaTest")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)
 
# Print Spark version
print(f"Spark version: {spark.version}")
 
# Create a simple dataframe
data = [
    {"id": 1, "name": "Feature1", "type": "HOLE", "diameter": 10.5},
    {"id": 2, "name": "Feature2", "type": "POCKET", "diameter": 20.0},
    {"id": 3, "name": "Feature3", "type": "SLOT", "diameter": 15.2},
]
 
df = spark.createDataFrame(data)
 
# Show the dataframe
print("Original DataFrame:")
df.show()
 
# Perform simple transformations
transformed_df = df.withColumn("description", lit("CAD Feature")).withColumn(
    "diameter_inch", col("diameter") / 25.4
)
 
print("Transformed DataFrame:")
transformed_df.show()
 
# Write to Parquet (example of how to save data)
output_path = "./output/parquet/features.parquet"
print(f"Writing data to {output_path}")
transformed_df.write.mode("overwrite").parquet(output_path)
 
# Read back from Parquet
read_df = spark.read.parquet(output_path)
print("Data read from Parquet:")
read_df.show()
 
# Test Delta Lake functionality
print("\nTesting Delta Lake functionality...")
 
# Define the Delta table path
delta_path = "./output/delta_test"
 
# Clean up any existing data
if os.path.exists(delta_path):
    shutil.rmtree(delta_path)
 
# Write DataFrame as a Delta table
print("Writing data to Delta table...")
transformed_df.write.format("delta").mode("overwrite").save(delta_path)
 
# Read the Delta table
print("Reading data from Delta table...")
delta_df = spark.read.format("delta").load(delta_path)
print("Delta table content:")
delta_df.show()
 
# Add new data to the Delta table
print("Appending data to Delta table...")
new_data = [
    {
        "id": 4,
        "name": "Feature4",
        "type": "COUNTERBORE",
        "diameter": 25.4,
        "description": "CAD Feature",
        "diameter_inch": 1.0,
    },
    {
        "id": 5,
        "name": "Feature5",
        "type": "CHAMFER",
        "diameter": 12.7,
        "description": "CAD Feature",
        "diameter_inch": 0.5,
    },
]
new_df = spark.createDataFrame(new_data)
new_df.write.format("delta").mode("append").save(delta_path)
 
# Read the updated Delta table
print("Reading updated Delta table...")
updated_df = spark.read.format("delta").load(delta_path)
print("Updated Delta table content:")
updated_df.show()
 
# Test Delta table versioning
print("Testing Delta table versioning...")
print("Reading Delta table version 0:")
v0_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
v0_df.show()
 
# Stop the Spark session
spark.stop()
print("Spark and Delta Lake test completed successfully!")