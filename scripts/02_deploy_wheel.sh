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
# -----------------------------------------

# Find the latest wheel file (more flexible approach)
WHEEL_FILES=(dist/*.whl)
if [ ! -e "${WHEEL_FILES[0]}" ]; then
    echo "Error: No wheel files found in dist/ directory"
    echo "Please run the build script first to create the wheel package"
    exit 1
fi

# Get the most recent wheel file
WHEEL_PATH="$(ls -1t dist/*.whl | head -n1)"
WHEEL_NAME="$(basename "${WHEEL_PATH}")"

echo "Using wheel file: ${WHEEL_NAME}"

# Validate required environment variables
if [ -z "$WS_NAME" ] || [ -z "$RG_NAME" ] || [ -z "$POOL_NAME" ] || [ -z "$STORAGE_ACCOUNT" ] || [ -z "$FILE_SYSTEM" ]; then
    echo "Error: Missing required environment variables"
    echo "Please ensure WS_NAME, RG_NAME, POOL_NAME, STORAGE_ACCOUNT, and FILE_SYSTEM are set"
    exit 1
fi

# Check for and remove any existing siemens packages from Spark pool to avoid conflicts
echo "Checking for existing siemens packages in Spark pool: ${POOL_NAME}"
EXISTING_SIEMENS_PACKAGES=$(az synapse spark pool show \
    --workspace-name "${WS_NAME}" \
    --resource-group "${RG_NAME}" \
    --name "${POOL_NAME}" \
    --query "customLibraries[?starts_with(name, 'siemens-')].name" \
    -o tsv 2>/dev/null || echo "")

if [ -n "$EXISTING_SIEMENS_PACKAGES" ]; then
    echo "Found existing siemens packages that need to be removed:"
    echo "$EXISTING_SIEMENS_PACKAGES"
    
    # Stop the Spark pool to be able to remove packages
    echo "Stopping Spark pool to enable package removal..."
    POOL_STATE=$(az synapse spark pool show \
        --workspace-name "${WS_NAME}" \
        --resource-group "${RG_NAME}" \
        --name "${POOL_NAME}" \
        --query "provisioningState" -o tsv 2>/dev/null || echo "")
    
    echo "Current pool state: ${POOL_STATE}"
    
    if [ "$POOL_STATE" != "Paused" ]; then
        echo "Pausing Spark pool: ${POOL_NAME}"
        if ! az synapse spark pool update \
            --workspace-name "${WS_NAME}" \
            --resource-group "${RG_NAME}" \
            --name "${POOL_NAME}" \
            --enable-auto-pause true \
            --auto-pause-delay 5; then
            echo "Warning: Failed to configure auto-pause for pool ${POOL_NAME}"
        fi
        
        # Wait a moment for the pool to start pausing
        echo "Waiting for pool to pause (this may take a few minutes)..."
        sleep 30
        
        # Check pool state
        POOL_STATE=$(az synapse spark pool show \
            --workspace-name "${WS_NAME}" \
            --resource-group "${RG_NAME}" \
            --name "${POOL_NAME}" \
            --query "provisioningState" -o tsv 2>/dev/null || echo "")
        echo "Pool state after pause request: ${POOL_STATE}"
    else
        echo "Pool is already paused"
    fi
    
    # Remove each existing siemens package
    echo "Removing existing siemens packages..."
    while IFS= read -r package_name; do
        if [ -n "$package_name" ]; then
            echo "Removing existing package: ${package_name}"
            if ! az synapse spark pool update \
                --workspace-name "${WS_NAME}" \
                --resource-group "${RG_NAME}" \
                --name "${POOL_NAME}" \
                --package-action Remove \
                --package "${package_name}"; then
                echo "Warning: Failed to remove package ${package_name} (it may not exist or be in use)"
            else
                echo "Successfully removed package: ${package_name}"
            fi
        fi
    done <<< "$EXISTING_SIEMENS_PACKAGES"
    
    echo "Finished removing existing siemens packages"
else
    echo "No existing siemens packages found in Spark pool"
fi

# Check if the package exists in workspace
echo "Checking if package ${WHEEL_NAME} already exists in workspace..."
PACKAGE_EXISTS=$(az synapse workspace-package show \
  --workspace-name "$WS_NAME" \
  --name "$WHEEL_NAME" \
  --query "id" -o tsv 2>/dev/null || echo "")

if [ -n "$PACKAGE_EXISTS" ]; then
    echo "Package ${WHEEL_NAME} already exists in workspace"
    echo "Workspace package ID: ${PACKAGE_EXISTS}"
    echo "Removing existing package to upload the latest version..."
    
    if ! az synapse workspace-package delete \
        --workspace-name "${WS_NAME}" \
        --name "${WHEEL_NAME}" \
        --yes; then
        echo "Warning: Failed to delete existing package ${WHEEL_NAME}"
        echo "Proceeding with upload anyway..."
    else
        echo "Successfully removed existing package: ${WHEEL_NAME}"
    fi
else
    echo "Package not found in workspace"
fi

# Upload the package (always upload since we removed existing one)
echo "Uploading workspace package: ${WHEEL_NAME}"

if ! az synapse workspace-package upload \
    --workspace-name "${WS_NAME}" \
    --package "${WHEEL_PATH}"; then
    echo "Error: Failed to upload workspace package"
    exit 1
fi

echo "Successfully uploaded package: ${WHEEL_NAME}"

# Now attach the new package to Spark pool
echo "Attaching new package to Spark pool: ${POOL_NAME}"

if ! az synapse spark pool update \
    --workspace-name "${WS_NAME}" \
    --resource-group "${RG_NAME}" \
    --name "${POOL_NAME}" \
    --package-action Add \
    --package "${WHEEL_NAME}"; then
    echo "Error: Failed to attach package to Spark pool"
    exit 1
fi

echo "Successfully attached package to Spark pool: ${POOL_NAME}"

# Stage entrypoint.py to ADLS (so it's accessible via abfss://)
echo "Uploading entrypoint.py to ADLS path '${CODE_PATH}'"

# Check if entrypoint.py exists
if [ ! -f "jobs/entrypoint.py" ]; then
    echo "Error: entrypoint.py not found in jobs/ directory"
    exit 1
fi

if ! az storage fs file upload \
    --account-name "${STORAGE_ACCOUNT}" \
    --auth-mode login \
    --file-system "${FILE_SYSTEM}" \
    --source "jobs/entrypoint.py" \
    --path "${CODE_PATH}/entrypoint.py" \
    --overwrite true; then
    echo "Error: Failed to upload entrypoint.py to ADLS"
    exit 1
fi

echo "Successfully uploaded entrypoint.py to ADLS"
echo "Deployment completed successfully!"