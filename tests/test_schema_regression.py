import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from pipeline_constants import COMMON_FUEL_COLUMN_RENAME_MAP, STATE_LIST


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_WORKBOOK = Path("/Users/monish/Downloads/reportTable (7).xlsx")

COMMON_FUEL_OUTPUT_COLUMNS = list(COMMON_FUEL_COLUMN_RENAME_MAP.values())

STATE_OUTPUT_COLUMNS = [
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
]

RTO_OUTPUT_COLUMNS = [
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
]

OEM_OUTPUT_COLUMNS = [
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
]


def load_module(relative_path: str, module_name: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def build_raw_dataframe():
    row = {
        "Unnamed: 1": "MOTOR CAR",
        "BIO-CNG/BIO-GAS": 1,
        "BIO-DIESEL(B100)": 2,
        "BIO-METHANE": 3,
        "CNG ONLY": 4,
        "DIESEL": 5,
        "DIESEL/HYBRID": 6,
        "DI-METHYL ETHER": 7,
        "DUAL DIESEL/BIO CNG": 8,
        "DUAL DIESEL/CNG": 9,
        "DUAL DIESEL/LNG": 10,
        "ELECTRIC(BOV)": 11,
        "ETHANOL(E100)": 12,
        "FLEX-FUEL(ETHANOL)": 13,
        "FUEL CELL HYDROGEN": 14,
        "LNG": 15,
        "LPG ONLY": 16,
        "METHANOL": 17,
        "NOT APPLICABLE": 18,
        "PETROL": 19,
        "PETROL/CNG": 20,
        "PETROL(E20)": 21,
        "PETROL(E20)/CNG": 22,
        "PETROL(E20)/HYBRID": 23,
        "PETROL(E20)/LPG": 24,
        "PETROL/HYBRID": 25,
        "PETROL/LPG": 26,
        "PETROL/METHANOL": 27,
        "PLUG-IN HYBRID EV": 28,
        "PURE EV": 29,
        "SOLAR": 30,
        "STRONG HYBRID EV": 31,
        "Unnamed: 38": 32,
    }
    return pd.DataFrame([row], index=["Total"])


def write_report_table(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, startrow=3)


def write_mapping_workbook(root: Path):
    mapping_df = pd.DataFrame(
        [
            {
                "Vehicle Class": "MOTOR CAR",
                "Vehicle Type": "Passenger",
                "Vehicle Category": "Car",
                "Vehicle Use Type": "Private",
            }
        ]
    )
    with pd.ExcelWriter(root / "Table and Mapping V2.xlsx", engine="openpyxl") as writer:
        mapping_df.to_excel(writer, sheet_name="Mapping", index=False)


class PipelineSchemaRegressionTests(unittest.TestCase):
    def test_state_list_includes_telangana(self):
        self.assertIn("Telangana", STATE_LIST)

    def test_real_sample_workbook_headers_match_shared_mapping(self):
        if not SAMPLE_WORKBOOK.exists():
            self.skipTest(f"Sample workbook not found at {SAMPLE_WORKBOOK}")

        df = pd.read_excel(SAMPLE_WORKBOOK, skiprows=3, index_col=0)
        expected_headers = set(COMMON_FUEL_COLUMN_RENAME_MAP.keys()) | {"Unnamed: 1"}
        self.assertEqual(set(df.columns), expected_headers)

    def test_rto_preprocessing_outputs_expected_columns(self):
        module = load_module("rto_level/rto_level_data_pre_processing.py", "rto_pre")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_mapping_workbook(root)
            report_path = (
                root
                / "rto_level"
                / "rto_level_ev_data"
                / "Test State"
                / "TestRto_TS01"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_report_table(report_path, build_raw_dataframe())

            with mock.patch.object(module.os, "getcwd", return_value=str(root)):
                processor = module.RTOLevelDataPreProcessor()
                result = processor.data_preprocessing("JUN", "2026")

            self.assertEqual(list(result.columns), RTO_OUTPUT_COLUMNS)
            self.assertEqual(result.loc[0, "rto_name"], "TestRto")
            self.assertEqual(result.loc[0, "rto_code"], "TS01")
            self.assertEqual(result.loc[0, "vehicle_type"], "Passenger")
            self.assertEqual(result.loc[0, "vehicle_category"], "Car")
            self.assertEqual(result.loc[0, "vehicle_use_type"], "Private")
            self.assertEqual(result.loc[0, "month"], 6)
            self.assertEqual(result.loc[0, "date"], "01/06/2026")
            self.assertTrue(pd.isna(result.loc[0, "hcng"]))
            self.assertTrue(pd.isna(result.loc[0, "hydrogen_ice"]))
            self.assertTrue(pd.isna(result.loc[0, "flex_fuel_bio_diesel"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_e20_hybrid_cng"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_hybrid_cng"]))

    def test_state_preprocessing_outputs_expected_columns(self):
        module = load_module(
            "State-level/state_level_data_pre_processing.py", "state_pre"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_mapping_workbook(root)
            report_path = (
                root
                / "State-level"
                / "state_level_ev_data"
                / "Test State"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_report_table(report_path, build_raw_dataframe())

            with mock.patch.object(module.os, "getcwd", return_value=str(root)):
                processor = module.StateLevelDataPreProcessor()
                result = processor.data_preprocessing("JUN", "2026")

            self.assertEqual(list(result.columns), STATE_OUTPUT_COLUMNS)
            self.assertEqual(result.loc[0, "vehicle_type"], "Passenger")
            self.assertEqual(result.loc[0, "vehicle_category"], "Car")
            self.assertEqual(result.loc[0, "vehicle_use_type"], "Private")
            self.assertEqual(result.loc[0, "month"], 6)
            self.assertEqual(result.loc[0, "date"], "01/06/2026")
            self.assertTrue(pd.isna(result.loc[0, "hcng"]))
            self.assertTrue(pd.isna(result.loc[0, "hydrogen_ice"]))
            self.assertTrue(pd.isna(result.loc[0, "flex_fuel_bio_diesel"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_e20_hybrid_cng"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_hybrid_cng"]))

    def test_oem_preprocessing_outputs_expected_columns(self):
        module = load_module("OEM-level/data_preprocessing_v2.py", "oem_pre")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_mapping_workbook(root)
            raw_df = build_raw_dataframe().rename(columns={"Unnamed: 1": "Unnamed: 1"})
            raw_df["Unnamed: 1"] = "Test Maker"
            report_path = (
                root
                / "OEM-level"
                / "oem_data_by_state_and_category"
                / "Test State"
                / "MOTOR CAR"
                / "2026"
                / "JUN"
                / "reportTable.xlsx"
            )
            write_report_table(report_path, raw_df)

            with mock.patch.object(module.os, "getcwd", return_value=str(root)):
                processor = module.OEMDataPreProcessor()
                result = processor.data_preprocessing("JUN", "2026")

            self.assertEqual(list(result.columns), OEM_OUTPUT_COLUMNS)
            self.assertEqual(result.loc[0, "maker"], "Test Maker")
            self.assertEqual(result.loc[0, "vehicle_class"], "MOTOR CAR")
            self.assertEqual(result.loc[0, "vehicle_type"], "Passenger")
            self.assertEqual(result.loc[0, "vehicle_category"], "Car")
            self.assertEqual(result.loc[0, "vehicle_use_type"], "Private")
            self.assertEqual(result.loc[0, "month"], 6)
            self.assertEqual(result.loc[0, "date"], "01/06/2026")
            self.assertTrue(pd.isna(result.loc[0, "hcng"]))
            self.assertTrue(pd.isna(result.loc[0, "hydrogen_ice"]))
            self.assertTrue(pd.isna(result.loc[0, "flex_fuel_bio_diesel"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_e20_hybrid_cng"]))
            self.assertTrue(pd.isna(result.loc[0, "petrol_hybrid_cng"]))


if __name__ == "__main__":
    unittest.main()
