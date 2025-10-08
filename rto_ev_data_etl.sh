#!/bin/bash

# Check if arguments (e.g., "OCT 2024") are passed
ARGS="$@"

# Set up virtual environment
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# Set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
python3 rto_level/rto_level_data_scraper.py $ARGS

# Run missing files extraction
python3 rto_level/rto_level_get_missing_files.py $ARGS

# Run Pre Processing
python3 rto_level/rto_level_data_pre_processing.py $ARGS

# Ingestion
python3 rto_level/rto_level_data_ingestion.py $ARGS

# File Upload and Cleanup
python3 rto_level/upload_files_to_blob_storage.py $ARGS

# Run dbt model
cd /home/climate_dot_data/DataScraper_VahanParivahan/climate_dot_dbt
dbt run --select rto_wise_ev_data >> /home/climate_dot_data/DataScraper_VahanParivahan/dbt_rto_wise_logs.txt 2>&1
