#!/bin/bash

set -e

log_step() {
    printf '%s - INFO - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

# Check if arguments (e.g., "OCT 2024") are passed
ARGS="$@"

# set up venv
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
log_step "Starting OEM extraction for ${ARGS:-default previous month}"
python3 OEM-level/oem_level_data_scraper.py $ARGS

# Run missing files extraction
log_step "Running OEM missing-file recovery for ${ARGS:-default previous month}"
python3 OEM-level/get_missing_files.py $ARGS

# Run Pre Processing
log_step "Running OEM preprocessing for ${ARGS:-default previous month}"
python3 OEM-level/data_preprocessing_v2.py $ARGS

# Ingestion
log_step "Running OEM ingestion for ${ARGS:-default previous month}"
python3 OEM-level/data_ingestion.py $ARGS

# File Upload and Cleanup
log_step "Running OEM upload and cleanup for ${ARGS:-default previous month}"
python3 OEM-level/upload_files_to_blob_storage.py $ARGS
