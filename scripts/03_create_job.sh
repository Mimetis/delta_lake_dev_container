#!/usr/bin/env bash
set -euo pipefail

# --- CONFIG (edit or pass as env vars) ---
WS_NAME="${WS_NAME:-your-synapse-workspace}"
POOL_NAME="${POOL_NAME:-your-spark-pool}"

# ADLS Gen2 data lake
STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-youradlsaccount}"        # without .dfs.core.windows.net
FILE_SYSTEM="${FILE_SYSTEM:-yourfilesystem}"                  # container / filesystem name
CODE_PATH="${CODE_PATH:-synapse/code/siemens}"           # path inside filesystem for code
INPUT_PATH="${INPUT_PATH:-synapse/input/json}"                # input JSON in ADLS
OUTPUT_PATH="${OUTPUT_PATH:-synapse/output/parquet}"          # output Parquet in ADLS

# Spark sizing (tune to your pool)
EXECUTOR_COUNT="${EXECUTOR_COUNT:-2}"
EXECUTOR_SIZE="${EXECUTOR_SIZE:-Small}"   # Small/Medium/Large (Synapse pool sizes)

# -----------------------------------------

MAIN_FILE_ABFSS="abfss://${FILE_SYSTEM}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${CODE_PATH}/entrypoint.py"
IN_ABFSS="abfss://${FILE_SYSTEM}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${INPUT_PATH}"
OUT_ABFSS="abfss://${FILE_SYSTEM}@${STORAGE_ACCOUNT}.dfs.core.windows.net/${OUTPUT_PATH}"

JOB_NAME="siemens_$(date +%Y%m%d_%H%M%S)"

echo "Submitting Spark job '${JOB_NAME}'"
# Note: 'az synapse spark job submit' accepts main file + args; language=Python.
az synapse spark job submit \
  --workspace-name "${WS_NAME}" \
  --spark-pool-name "${POOL_NAME}" \
  --name "${JOB_NAME}" \
  --language Python \
  --main-definition-file "${MAIN_FILE_ABFSS}" \
  --arguments "--input $IN_ABFSS --output $OUT_ABFSS --repartition 8" \
  --executors "${EXECUTOR_COUNT}" \
  --executor-size "${EXECUTOR_SIZE}"

echo "Submitted. You can monitor the job in Synapse Studio > Monitor > Apache Spark applications."