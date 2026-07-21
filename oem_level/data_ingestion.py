import os
import sys

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_ingestion import BaseSqlServerIngestor
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args

configure_pipeline_logging()


class OEMDataIngest(BaseSqlServerIngestor):
    def __init__(self):
        super().__init__(
            file_prefix="oem_data_by_state_and_category",
            staging_table_name="staging_fact_oem_data_by_state_and_category",
            final_table_name="fact_oem_data_by_state_and_category",
            merge_key_columns=["date", "state", "vehicle_class"],
            missing_file_hint="data_preprocessing_v2",
        )


def main():
    oem_data_ingest = OEMDataIngest()
    month, year = resolve_month_year_args(sys.argv[1:])
    oem_data_ingest.data_ingest(month, year)


if __name__ == "__main__":
    main()
