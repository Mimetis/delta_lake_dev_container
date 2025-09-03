import argparse
import logging
import sys
from siemens.spark_util import get_spark
from siemens.transform import json_dir_to_delta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("siemens")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Transform a JSON directory to Parquet using Spark")
    p.add_argument("--input", default="./input/json", help="Input JSON directory (local or abfss://)")
    p.add_argument("--output", default="./output/parquet", help="Output Parquet directory (local or abfss://)")
    p.add_argument("--repartition", type=int, default=None)
    p.add_argument("--partition-by", nargs="*", default=["year", "month"], help="Columns to partition by")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "errorifexists", "ignore"])
    p.add_argument("--no-recursive", action="store_true", help="Disable recursive file lookup")
    p.add_argument("--multiline", action="store_true", help="Enable multiline JSON")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark("siemens", logger)
    logger.info("Starting JSON→Parquet | input=%s output=%s", args.input, args.output)

    df = json_dir_to_delta(
        spark=spark,
        input_dir=args.input,
        output_dir=args.output,
        recursive=not args.no_recursive,
        multiline=args.multiline,
        repartition=args.repartition,
        mode=args.mode,
        partition_by=args.partition_by,
    )
    logger.info("Rows written: %s", df.count())


if __name__ == "__main__":
    main()
