# =============================================================================
# Delta Lake Development Container - Makefile
# =============================================================================
# This Makefile provides convenient commands for building and deploying
# Delta Lake projects in the development container environment.
#
# Usage:
#   make help          - Show this help message
#   make build         - Build the Python wheel package
#   make deploy-wheel  - Deploy the built wheel package to Azure Synapse
#   make create-job    - Create and submit a Spark job in Azure Synapse
#   make clean         - Clean build artifacts
#   make all           - Run build, deploy-wheel, and create-job tasks sequentially
#
# Requirements:
#   - Python 3.x with pip installed
#   - Azure CLI (for deployment)
#   - Proper Azure authentication configured
#   - .env file with required environment variables in project root
# =============================================================================

# Default shell for make commands
SHELL := /bin/bash

# Project directories
SCRIPTS_DIR := scripts
DIST_DIR := dist
BUILD_DIR := build

# Colors for output formatting (use with printf)
COLOR_RESET := \033[0m
COLOR_GREEN := \033[32m
COLOR_YELLOW := \033[33m
COLOR_BLUE := \033[34m
COLOR_RED := \033[31m

# Default target
.DEFAULT_GOAL := help

# =============================================================================
# ENVIRONMENT SETUP
# =============================================================================

# Function to check if .env file exists and load it
define check_env
	@if [ ! -f ".env" ]; then \
		printf "$(COLOR_RED)Error: .env file not found in project root$(COLOR_RESET)\n"; \
		printf "$(COLOR_YELLOW)Please create a .env file with required environment variables$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@printf "$(COLOR_BLUE)Loading environment variables from .env file...$(COLOR_RESET)\n"
	@set -a; source .env; set +a
endef

# Function to validate required environment variables
define validate_env
	$(call check_env)
	@printf "$(COLOR_BLUE)Validating required environment variables...$(COLOR_RESET)\n"
	@missing_vars=""; \
	for var in WS_NAME RG_NAME POOL_NAME STORAGE_ACCOUNT FILE_SYSTEM CODE_PATH; do \
		if [ -z "$$(set -a; source .env; eval echo \$$$$var)" ]; then \
			missing_vars="$$missing_vars $$var"; \
		fi; \
	done; \
	if [ -n "$$missing_vars" ]; then \
		printf "$(COLOR_RED)Error: Missing required environment variables:$$missing_vars$(COLOR_RESET)\n"; \
		printf "$(COLOR_YELLOW)Please check your .env file and ensure all variables are set$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@printf "$(COLOR_GREEN)✓ All required environment variables are set$(COLOR_RESET)\n"
endef

# =============================================================================
# HELP TARGET
# =============================================================================

.PHONY: help
help: ## Show this help message
	@printf "$(COLOR_BLUE)Delta Lake Development Container - Available Commands$(COLOR_RESET)\n"
	@printf "==================================================================\n"
	@printf "\n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "$(COLOR_GREEN)%-15s$(COLOR_RESET) %s\n", $$1, $$2}'
	@printf "\n"
	@printf "$(COLOR_YELLOW)Examples:$(COLOR_RESET)\n"
	@printf "  make build         # Build the Python wheel\n"
	@printf "  make deploy-wheel  # Deploy the wheel package\n"
	@printf "  make create-job    # Create and submit a Spark job\n"
	@printf "  make all           # Build, deploy wheel, and create job\n"
	@printf "\n"

# =============================================================================
# BUILD TARGETS
# =============================================================================

.PHONY: build
build: ## Build the Python wheel package using the build script
	$(call check_env)
	@printf "$(COLOR_BLUE)Building Python wheel package...$(COLOR_RESET)\n"
	@if [ ! -f "$(SCRIPTS_DIR)/01_build_wheel.sh" ]; then \
		printf "$(COLOR_RED)Error: Build script not found at $(SCRIPTS_DIR)/01_build_wheel.sh$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@chmod +x $(SCRIPTS_DIR)/01_build_wheel.sh
	@set -a; source .env; bash $(SCRIPTS_DIR)/01_build_wheel.sh
	@printf "$(COLOR_GREEN)✓ Build completed successfully!$(COLOR_RESET)\n"
	@if [ -d "$(DIST_DIR)" ]; then \
		printf "$(COLOR_BLUE)Built artifacts:$(COLOR_RESET)\n"; \
		ls -la $(DIST_DIR)/; \
	fi

# =============================================================================
# DEPLOYMENT TARGETS
# =============================================================================

.PHONY: deploy-wheel
deploy-wheel: ## Deploy the built wheel package to Azure Synapse
	$(call validate_env)
	@printf "$(COLOR_BLUE)Deploying wheel package to Azure Synapse...$(COLOR_RESET)\n"
	@if [ ! -f "$(SCRIPTS_DIR)/02_deploy_wheel.sh" ]; then \
		printf "$(COLOR_RED)Error: Deploy wheel script not found at $(SCRIPTS_DIR)/02_deploy_wheel.sh$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@if [ ! -d "$(DIST_DIR)" ] || [ -z "$$(ls -A $(DIST_DIR)/*.whl 2>/dev/null)" ]; then \
		printf "$(COLOR_RED)Error: No wheel files found in $(DIST_DIR)/$(COLOR_RESET)\n"; \
		printf "$(COLOR_YELLOW)Please run 'make build' first to create the wheel package$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@chmod +x $(SCRIPTS_DIR)/02_deploy_wheel.sh
	@set -a; source .env; bash $(SCRIPTS_DIR)/02_deploy_wheel.sh
	@printf "$(COLOR_GREEN)✓ Wheel deployment completed successfully!$(COLOR_RESET)\n"

.PHONY: create-job
create-job: ## Create and submit a Spark job in Azure Synapse
	$(call validate_env)
	@printf "$(COLOR_BLUE)Creating and submitting Spark job to Azure Synapse...$(COLOR_RESET)\n"
	@if [ ! -f "$(SCRIPTS_DIR)/03_create_job.sh" ]; then \
		printf "$(COLOR_RED)Error: Create job script not found at $(SCRIPTS_DIR)/03_create_job.sh$(COLOR_RESET)\n"; \
		exit 1; \
	fi
	@chmod +x $(SCRIPTS_DIR)/03_create_job.sh
	@set -a; source .env; bash $(SCRIPTS_DIR)/03_create_job.sh
	@printf "$(COLOR_GREEN)✓ Spark job submitted successfully!$(COLOR_RESET)\n"
	@printf "$(COLOR_YELLOW)Monitor the job in Synapse Studio > Monitor > Apache Spark applications$(COLOR_RESET)\n"

# =============================================================================
# COMBINED TARGETS
# =============================================================================

.PHONY: all
all: build deploy-wheel create-job ## Build the package, deploy the wheel, and create a Spark job (full pipeline)
	@printf "$(COLOR_GREEN)✓ All tasks completed successfully!$(COLOR_RESET)\n"

.PHONY: pipeline
pipeline: all ## Alias for 'all' target

# =============================================================================
# UTILITY TARGETS
# =============================================================================

.PHONY: clean
clean: ## Clean build artifacts and temporary files
	@printf "$(COLOR_BLUE)Cleaning build artifacts...$(COLOR_RESET)\n"
	@rm -rf $(DIST_DIR)/ $(BUILD_DIR)/ *.egg-info/
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@printf "$(COLOR_GREEN)✓ Cleanup completed!$(COLOR_RESET)\n"

.PHONY: check-scripts
check-scripts: ## Verify that required scripts exist and are executable
	@printf "$(COLOR_BLUE)Checking required scripts...$(COLOR_RESET)\n"
	@if [ -f "$(SCRIPTS_DIR)/01_build_wheel.sh" ]; then \
		printf "$(COLOR_GREEN)✓ Build script found$(COLOR_RESET)\n"; \
	else \
		printf "$(COLOR_RED)✗ Build script missing$(COLOR_RESET)\n"; \
	fi
	@if [ -f "$(SCRIPTS_DIR)/02_deploy_wheel.sh" ]; then \
		printf "$(COLOR_GREEN)✓ Deploy wheel script found$(COLOR_RESET)\n"; \
	else \
		printf "$(COLOR_RED)✗ Deploy wheel script missing$(COLOR_RESET)\n"; \
	fi
	@if [ -f "$(SCRIPTS_DIR)/03_create_job.sh" ]; then \
		printf "$(COLOR_GREEN)✓ Create job script found$(COLOR_RESET)\n"; \
	else \
		printf "$(COLOR_RED)✗ Create job script missing$(COLOR_RESET)\n"; \
	fi

.PHONY: env-check
env-check: ## Check if .env file exists and display environment variables
	$(call check_env)
	@printf "$(COLOR_BLUE)Environment variables from .env file:$(COLOR_RESET)\n"
	@set -a; source .env; \
	printf "  WS_NAME: $${WS_NAME:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  RG_NAME: $${RG_NAME:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  POOL_NAME: $${POOL_NAME:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  STORAGE_ACCOUNT: $${STORAGE_ACCOUNT:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  FILE_SYSTEM: $${FILE_SYSTEM:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  CODE_PATH: $${CODE_PATH:-$(COLOR_RED)NOT SET$(COLOR_RESET)}\n"; \
	printf "  INPUT_PATH: $${INPUT_PATH:-$(COLOR_YELLOW)OPTIONAL$(COLOR_RESET)}\n"; \
	printf "  OUTPUT_PATH: $${OUTPUT_PATH:-$(COLOR_YELLOW)OPTIONAL$(COLOR_RESET)}\n"; \
	printf "  EXECUTOR_COUNT: $${EXECUTOR_COUNT:-$(COLOR_YELLOW)OPTIONAL$(COLOR_RESET)}\n"; \
	printf "  EXECUTOR_SIZE: $${EXECUTOR_SIZE:-$(COLOR_YELLOW)OPTIONAL$(COLOR_RESET)}\n"

# =============================================================================
# DEVELOPMENT TARGETS
# =============================================================================

.PHONY: test
test: ## Run tests (if available)
	@printf "$(COLOR_BLUE)Running tests...$(COLOR_RESET)\n"
	@if [ -d "tests/" ]; then \
		python -m pytest tests/ -v; \
	else \
		printf "$(COLOR_YELLOW)No tests directory found$(COLOR_RESET)\n"; \
	fi

.PHONY: lint
lint: ## Run code linting (if tools are available)
	@printf "$(COLOR_BLUE)Running code linting...$(COLOR_RESET)\n"
	@if command -v flake8 >/dev/null 2>&1; then \
		flake8 src/ --max-line-length=88; \
	elif command -v python -c "import flake8" >/dev/null 2>&1; then \
		python -m flake8 src/ --max-line-length=88; \
	else \
		printf "$(COLOR_YELLOW)flake8 not available - skipping linting$(COLOR_RESET)\n"; \
	fi

.PHONY: format
format: ## Format code using black (if available)
	@printf "$(COLOR_BLUE)Formatting code...$(COLOR_RESET)\n"
	@if command -v black >/dev/null 2>&1; then \
		black src/ tests/; \
	elif command -v python -c "import black" >/dev/null 2>&1; then \
		python -m black src/ tests/; \
	else \
		printf "$(COLOR_YELLOW)black not available - skipping formatting$(COLOR_RESET)\n"; \
	fi

# =============================================================================
# INFO TARGETS
# =============================================================================

.PHONY: info
info: ## Show project information
	@printf "$(COLOR_BLUE)Delta Lake Development Container - Project Information$(COLOR_RESET)\n"
	@printf "========================================================\n"
	@printf "Project Root: $(PWD)\n"
	@printf "Scripts Directory: $(SCRIPTS_DIR)/\n"
	@printf "Distribution Directory: $(DIST_DIR)/\n"
	@printf "Python Version: $$(python --version 2>/dev/null || echo 'Not available')\n"
	@printf "PySpark Version: $$(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'Not available')\n"
	@printf "Delta Lake Version: $$(python -c 'import delta; print(delta.__version__)' 2>/dev/null || echo 'Not available')\n"
	@printf "\n"

# Ensure scripts are executable when needed
$(SCRIPTS_DIR)/01_build_wheel.sh:
	@chmod +x $@
