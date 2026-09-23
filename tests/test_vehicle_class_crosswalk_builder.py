import csv
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from new_portal import build_vehicle_class_crosswalk as builder


def write_mapping_workbook(path: Path, rows: list[dict]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Mapping", index=False)


class LoadAuthoritativeMappingTests(unittest.TestCase):
    def test_loads_mapping_keyed_by_normalized_class(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mapping.xlsx"
            write_mapping_workbook(
                path,
                [
                    {
                        "Vehicle Class": "MOTOR CAR",
                        "Vehicle Type": "4W_Personal",
                        "Vehicle Category": "4-Wheelers",
                        "Vehicle Use Type": "Personal",
                    }
                ],
            )

            mapping = builder.load_authoritative_mapping(path)

        key = builder.normalize_class_name("MOTOR CAR")
        self.assertEqual(mapping[key][1:], ("4W_Personal", "4-Wheelers", "Personal"))

    def test_raises_on_conflicting_rows_for_the_same_normalized_class(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mapping.xlsx"
            write_mapping_workbook(
                path,
                [
                    {
                        "Vehicle Class": "MOTOR CAR",
                        "Vehicle Type": "4W_Personal",
                        "Vehicle Category": "4-Wheelers",
                        "Vehicle Use Type": "Personal",
                    },
                    {
                        "Vehicle Class": "motor car",
                        "Vehicle Type": "4W_Shared",
                        "Vehicle Category": "4-Wheelers",
                        "Vehicle Use Type": "Shared",
                    },
                ],
            )

            with self.assertRaises(ValueError):
                builder.load_authoritative_mapping(path)


class NormalizeClassNameTests(unittest.TestCase):
    def test_collapses_whitespace_and_uppercases(self):
        self.assertEqual(builder.normalize_class_name("  road   roller  "), "ROAD ROLLER")

    def test_strips_punctuation(self):
        self.assertEqual(
            builder.normalize_class_name("Motor Cycle/Scooter-With Trailer"),
            "MOTOR CYCLE SCOOTER WITH TRAILER",
        )

    def test_treats_pluralization_variant_as_same_key_only_if_identical_after_normalization(self):
        # ROAD ROLLER vs ROAD ROLLERS is a real-world example from the actual
        # mapping file vs. observed data — NOT collapsed by normalization
        # (that would be guessing), so both must be handled as distinct keys
        # by callers, same as any other genuinely unmatched class.
        self.assertNotEqual(
            builder.normalize_class_name("ROAD ROLLER"),
            builder.normalize_class_name("ROAD ROLLERS"),
        )


class BuildCrosswalkRowsTests(unittest.TestCase):
    def test_uses_authoritative_mapping_when_class_is_found(self):
        authoritative = {
            "MOPED": ("MOPED", "2W_Personal", "2-Wheelers", "Personal"),
        }
        rows = builder.build_crosswalk_rows(authoritative, ["MOPED"])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["vehicle_type"], "2W_Personal")
        self.assertEqual(rows[0]["source"], builder.SOURCE_AUTHORITATIVE)

    def test_defaults_to_others_when_class_is_observed_but_not_in_mapping_file(self):
        rows = builder.build_crosswalk_rows({}, ["SCHOOL BUS"])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["vehicle_type"], builder.DEFAULT_DIMENSION_VALUE)
        self.assertEqual(rows[0]["vehicle_category"], builder.DEFAULT_DIMENSION_VALUE)
        self.assertEqual(rows[0]["vehicle_use_type"], builder.DEFAULT_DIMENSION_VALUE)
        self.assertEqual(rows[0]["source"], builder.SOURCE_DEFAULTED)

    def test_matches_case_and_whitespace_insensitively(self):
        authoritative = {
            builder.normalize_class_name("Motor Car"): (
                "Motor Car",
                "4W_Personal",
                "4-Wheelers",
                "Personal",
            ),
        }
        rows = builder.build_crosswalk_rows(authoritative, ["  motor   car  "])

        self.assertEqual(rows[0]["source"], builder.SOURCE_AUTHORITATIVE)
        self.assertEqual(rows[0]["vehicle_category"], "4-Wheelers")


class WriteCrosswalkTests(unittest.TestCase):
    def test_writes_expected_csv_content(self):
        rows = [
            {
                "vehicle_class": "MOPED",
                "vehicle_type": "2W_Personal",
                "vehicle_category": "2-Wheelers",
                "vehicle_use_type": "Personal",
                "source": builder.SOURCE_AUTHORITATIVE,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "nested" / "vehicle_class_crosswalk.csv"
            builder.write_crosswalk(rows, output_path=output_path)

            with output_path.open(newline="", encoding="utf-8") as f:
                written = list(csv.DictReader(f))

        self.assertEqual(written[0]["vehicle_class"], "MOPED")
        self.assertEqual(written[0]["source"], "authoritative_mapping_file")


if __name__ == "__main__":
    unittest.main()
