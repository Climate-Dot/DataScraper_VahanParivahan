import unittest
from pathlib import Path

import yaml

from pipeline_constants import COMMON_FUEL_COLUMN_RENAME_MAP


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES_PATH = REPO_ROOT / "climate_dot_dbt" / "models" / "curated" / "sources.yml"

COMMON_FUEL_OUTPUT_COLUMNS = list(COMMON_FUEL_COLUMN_RENAME_MAP.values())

EXPECTED_SOURCE_COLUMNS = {
    "fact_ev_data_by_rto": [
        "year",
        "month",
        "day",
        "date",
        "state",
        "rto_name",
        "rto_code",
        "vehicle_type",
        "vehicle_category",
        "vehicle_use_type",
        "vehicle_class",
        *COMMON_FUEL_OUTPUT_COLUMNS,
        "inserted_at",
    ],
    "fact_ev_data_by_state": [
        "year",
        "month",
        "day",
        "date",
        "state",
        "vehicle_type",
        "vehicle_category",
        "vehicle_use_type",
        "vehicle_class",
        *COMMON_FUEL_OUTPUT_COLUMNS,
        "inserted_at",
    ],
    "fact_oem_data_by_state_and_category": [
        "year",
        "month",
        "day",
        "date",
        "state",
        "vehicle_class",
        "vehicle_type",
        "vehicle_category",
        "vehicle_use_type",
        "maker",
        *COMMON_FUEL_OUTPUT_COLUMNS,
        "inserted_at",
    ],
}


class DbtContractTests(unittest.TestCase):
    def test_raw_sources_match_expected_columns(self):
        payload = yaml.safe_load(SOURCES_PATH.read_text())
        tables = payload["sources"][0]["tables"]
        actual = {
            table["name"]: [column["name"] for column in table.get("columns", [])]
            for table in tables
        }

        for table_name, expected_columns in EXPECTED_SOURCE_COLUMNS.items():
            self.assertIn(table_name, actual)
            self.assertEqual(actual[table_name], expected_columns)

    def test_curated_models_reference_new_raw_schema(self):
        curated_dir = REPO_ROOT / "climate_dot_dbt" / "models" / "curated"
        checks = {
            "rto_wise_ev_data.sql": "fact_ev_data_by_rto",
            "state_wise_ev_data.sql": "fact_ev_data_by_state",
            "oem_wise_ev_data.sql": "fact_oem_data_by_state_and_category",
        }

        for filename, source_name in checks.items():
            sql = (curated_dir / filename).read_text()
            self.assertIn(f"source('raw_data', '{source_name}')", sql)
            self.assertNotIn("petrol_ethanol", sql)
            self.assertNotIn("electric_vehicles", sql)
            self.assertIn("ethanol_e100", sql)
            self.assertIn("petrol_e20", sql)
            self.assertIn("inserted_at", sql)


if __name__ == "__main__":
    unittest.main()
