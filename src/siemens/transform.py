from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def json_dir_to_delta(
    spark: SparkSession,
    input_dir: str,
    output_dir: str,
    *,
    recursive: bool = True,
    multiline: bool = False,
    repartition: int | None = None,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    merge_schema: bool = False,  # useful when columns evolve
    overwrite_schema: bool = False,
) -> DataFrame:
    """
    Read a directory of JSON files and write Parquet.
    Returns the final DataFrame (handy for testing).
    """
    reader = spark.read.option("multiLine", "true" if multiline else "false")
    if recursive:
        reader = reader.option("recursiveFileLookup", "true")

    df = reader.json(input_dir)

    # Example: normalize/cleanup (customize as needed)
    # df = df.withColumn("ingestion_ts", F.current_timestamp())

    if repartition:
        df = df.repartition(repartition)

    writer = df.write.mode(mode).format("delta").option("mergeSchema", str(merge_schema).lower())  # ← Delta Lake sink

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(output_dir)
    return df
