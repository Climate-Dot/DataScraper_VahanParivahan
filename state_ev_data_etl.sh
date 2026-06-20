#!/bin/bash

set -e

log_step() {
    printf '%s - INFO - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

# Check if arguments (e.g., "OCT 2024") are passed
ARGS="$@"

# Set up virtual environment
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# Set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
log_step "Starting state extraction for ${ARGS:-default previous month}"
python3 State-level/state_level_data_scraper.py $ARGS

# Run missing files extraction
log_step "Running state missing-file recovery for ${ARGS:-default previous month}"
python3 State-level/state_level_get_missing_files.py $ARGS

# Run Pre Processing
log_step "Running state preprocessing for ${ARGS:-default previous month}"
python3 State-level/state_level_data_pre_processing.py $ARGS

# Ingestion
log_step "Running state ingestion for ${ARGS:-default previous month}"
python3 State-level/state_level_data_ingestion.py $ARGS

# File Upload and Cleanup
log_step "Running state upload and cleanup for ${ARGS:-default previous month}"
python3 State-level/upload_files_to_blob_storage.py $ARGS
