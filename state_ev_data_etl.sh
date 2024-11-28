#!/bin/bash

# set up venv
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
python3 State-level/state_level_data_scraper.py

# Run missing files extraction
python3 State-level/state_level_get_missing_files.py

# Run Pre Processing
python3 State-level/state_level_data_preprocessing.py

# Ingestion
python3 State-level/state_level_data_ingestion.py

# File Upload and Cleanup
python3 State-level/upload_files_to_blob_storage.py