import os
import sys

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_blob_upload import upload_pipeline_artifacts
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args

configure_pipeline_logging()


def main():
    month, year = resolve_month_year_args(sys.argv[1:])
    upload_pipeline_artifacts(
        month=month,
        year=year,
        raw_file_pattern="oem_level/oem_data_by_state_and_category/*/*/{year}/{month}/*.xlsx",
        relative_root="oem_level/oem_data_by_state_and_category",
        raw_container_config_key="container_name",
        csv_container_config_key="csv_container_name",
        csv_prefix="oem_data_by_state_and_category_{month}_{year}",
    )


if __name__ == "__main__":
    main()
