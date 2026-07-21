import os
import sys

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_path not in sys.path:
    sys.path.append(repo_path)

from etl_ingestion import BaseSqlServerIngestor
from pipeline_logging import configure_pipeline_logging
from runtime_config import resolve_month_year_args

configure_pipeline_logging()


class StateDataIngest(BaseSqlServerIngestor):
    def __init__(self):
        super().__init__(
            file_prefix="state_level_ev_data",
            staging_table_name="staging_fact_ev_data_by_state",
            final_table_name="fact_ev_data_by_state",
            merge_key_columns=["date", "state", "vehicle_class"],
            missing_file_hint="state_level_data_pre_processing",
        )


def main():
    state_ev_data_ingest = StateDataIngest()
    month, year = resolve_month_year_args(sys.argv[1:])
    state_ev_data_ingest.data_ingest(month, year)


if __name__ == "__main__":
    main()
