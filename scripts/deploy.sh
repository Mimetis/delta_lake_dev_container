#!/usr/bin/env bash
set -euo pipefail

# --- CONFIG (edit or pass as env vars) ---
WS_NAME="${WS_NAME:-your-synapse-workspace}"
RG_NAME="${RG_NAME:-your-resource-group}"
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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" # repo root (/workspaces)
WHEEL_PATH="$(ls -1 dist/siemens-*.whl | tail -n1)"
WHEEL_NAME="$(basename "${WHEEL_PATH}")"

if [[ ! -f "${WHEEL_PATH}" ]]; then
  echo "Wheel not found. Building..."
  python -m pip install --upgrade build
  python -m build
  WHEEL_PATH="$(ls -1 "${ROOT_DIR}"/dist/siemens-*.whl | tail -n1)"
  WHEEL_NAME="$(basename "${WHEEL_PATH}")"
fi

echo "Logging into Azure (make sure your identity has Synapse + Storage perms)..."
az account show >/dev/null || az login -t fdpo.onmicrosoft.com

echo "Uploading workspace package: ${WHEEL_NAME}"
az synapse workspace-package upload \
  --workspace-name "${WS_NAME}" \
  --package "${WHEEL_PATH}"

# Attach to Spark pool (idempotent add; remove with --package-action Remove).
echo "Attaching package to Spark pool: ${POOL_NAME}"
az synapse spark pool update \
  --workspace-name "${WS_NAME}" \
  --resource-group "${RG_NAME}" \
  --name "${POOL_NAME}" \
  --package-action Add \
  --package "${WHEEL_NAME}"

# Stage entrypoint.py to ADLS (so it's accessible via abfss://)
echo "Uploading entrypoint.py to ADLS path '${CODE_PATH}'"
az storage fs file upload \
  --account-name "${STORAGE_ACCOUNT}" \
  --auth-mode login \
  --file-system "${FILE_SYSTEM}" \
  --source "jobs/entrypoint.py" \
  --path "${CODE_PATH}/entrypoint.py" \
  --overwrite true

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