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
        raw_file_pattern="rto_level/rto_level_ev_data/*/*/{year}/{month}/*.xlsx",
        relative_root="rto_level/rto_level_ev_data",
        raw_container_config_key="rto_wise_container_name",
        csv_container_config_key="rto_wise_csv_container_name",
        csv_prefix="rto_level_ev_data_{month}_{year}",
    )


if __name__ == "__main__":
    main()
