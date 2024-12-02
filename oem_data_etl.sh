#!/bin/bash

# Check if arguments (e.g., "OCT 2024") are passed
ARGS="$@"

# set up venv
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
python3 OEM-level/oem_level_data_scraper.py $ARGS

# Run missing files extraction
python3 OEM-level/get_missing_files.py $ARGS

# Run Pre Processing
python3 OEM-level/data_preprocessing_v2.py $ARGS

# Ingestion
python3 OEM-level/data_ingestion.py $ARGS

# File Upload and Cleanup
python3 OEM-level/upload_files_to_blob_storage.py $ARGS
