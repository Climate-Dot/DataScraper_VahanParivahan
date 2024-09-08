#!/bin/bash

# set up venv
. /home/climate_dot_data/DataScraper_VahanParivahan/venv/bin/activate

# set up current directory
cd /home/climate_dot_data/DataScraper_VahanParivahan

# Run the Extraction
python3 OEM-level/oem_level_data_scraper.py

# Run missing files extraction
python3 OEM-level/get_missing_files.py

# Run Pre Processing
python3 OEM-level/data_preprocessing_v2.py

# Ingestion
python3 OEM-level/data_ingestion.py

# File Upload and Cleanup
python3 OEM-level/upload_files_to_blob_storage.py
