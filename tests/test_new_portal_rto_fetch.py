import csv
import json
import threading
import re
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from new_portal import client, rto_fetch, schema


class SchemaTests(unittest.TestCase):
    def test_all_portal_fuel_labels_map_to_distinct_columns(self):
        columns = list(schema.FUEL_LABEL_TO_COLUMN.values())
        self.assertEqual(len(columns), len(set(columns)))

    def test_fuel_column_names_are_sql_safe(self):
        for column in schema.FUEL_COLUMNS:
            self.assertRegex(column, r"^[a-z][a-z0-9_]*$")

    def test_specific_fuel_label_slugs(self):
        self.assertEqual(schema.fuel_label_to_column("PETROL(E20)/HYBRID/CNG"), "petrol_e20_hybrid_cng")
        self.assertEqual(schema.fuel_label_to_column("BIO-CNG/BIO-GAS"), "bio_cng_bio_gas")
        self.assertEqual(schema.fuel_label_to_column("ELECTRIC(BOV)"), "electric_bov")
        self.assertEqual(schema.fuel_label_to_column("NOT APPLICABLE"), "not_applicable")

    def test_portal_absent_fuels_are_still_columns(self):
        # NULL, not dropped and not zero-filled — CLAUDE.md safety rule 4.
        for column in schema.FUEL_COLUMNS_NOT_ON_NEW_PORTAL:
            self.assertIn(column, schema.FUEL_COLUMNS)
            self.assertNotIn(column, schema.FUEL_LABEL_TO_COLUMN.values())

    def test_csv_columns_are_unique_and_cover_the_grain(self):
        self.assertEqual(len(schema.CSV_COLUMNS), len(set(schema.CSV_COLUMNS)))
        for column in schema.KEY_COLUMNS:
            self.assertIn(column, schema.CSV_COLUMNS)

    def test_default_status_scope_is_all_statuses_only(self):
        self.assertEqual(schema.DEFAULT_STATUS_SCOPES, [schema.STATUS_SCOPE_ALL])
        self.assertIn(schema.STATUS_SCOPE_ACTIVE, schema.STATUS_SCOPES)

    def test_parse_year_as_string(self):
        self.assertEqual(schema.parse_year_as_string("2026-August"), (2026, 8))
        self.assertEqual(schema.parse_year_as_string("2013-January"), (2013, 1))

    def test_parse_year_as_string_rejects_garbage(self):
        with self.assertRaises(ValueError):
            schema.parse_year_as_string("2026-Smarch")


class MigrationParityTests(unittest.TestCase):
    """The migration SQL and schema.py must not drift apart.

    Ingestion does `INSERT INTO final SELECT * FROM staging`, so a column-order
    mismatch would silently write values into the wrong columns.
    """

    MIGRATION = (
        Path(__file__).resolve().parent.parent
        / "sql"
        / "migrations"
        / "2026-08-18_new_portal_rto_v2_tables.sql"
    )

    def _columns_of(self, table: str) -> list[str]:
        sql = self.MIGRATION.read_text()
        block = re.search(rf"CREATE TABLE dbo\.{table} \((.*?)\n\);", sql, re.S)
        self.assertIsNotNone(block, f"{table} not found in the migration")
        return re.findall(r"^\s*\[(\w+)\]", block.group(1), re.M)

    def test_final_table_matches_csv_columns_in_order(self):
        self.assertEqual(
            self._columns_of("fact_ev_data_by_rto_v2"),
            schema.CSV_COLUMNS + ["inserted_at"],
        )

    def test_staging_table_matches_final_table(self):
        self.assertEqual(
            self._columns_of("staging_fact_ev_data_by_rto_v2"),
            self._columns_of("fact_ev_data_by_rto_v2"),
        )


class LoadRtoTargetsTests(unittest.TestCase):
    def _write(self, rows):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "rto_code_crosswalk.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "state", "state_code", "new_rto_code", "new_rto_name",
                    "legacy_rto_code", "legacy_rto_name", "link_status",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_portal_only_rtos_are_included(self):
        # Decided 2026-08-18: V2 ingests all 1,676 live RTOs, not just linked.
        path = self._write([
            {"state": "Haryana", "state_code": "HR", "new_rto_code": "99",
             "new_rto_name": "NEW OFFICE", "legacy_rto_code": "HR99",
             "legacy_rto_name": "", "link_status": "portal_only"},
        ])
        self.assertEqual(len(rto_fetch.load_rto_targets(path)), 1)

    def test_legacy_only_rtos_are_skipped(self):
        path = self._write([
            {"state": "Mizoram", "state_code": "MZ", "new_rto_code": "",
             "new_rto_name": "", "legacy_rto_code": "MZ10",
             "legacy_rto_name": "KHAWZAWL", "link_status": "legacy_only"},
        ])
        self.assertEqual(rto_fetch.load_rto_targets(path), [])

    def test_state_filter(self):
        path = self._write([
            {"state": "Haryana", "state_code": "HR", "new_rto_code": "1",
             "new_rto_name": "A", "legacy_rto_code": "HR1",
             "legacy_rto_name": "A", "link_status": "linked"},
            {"state": "Maharashtra", "state_code": "MH", "new_rto_code": "12",
             "new_rto_name": "PUNE", "legacy_rto_code": "MH12",
             "legacy_rto_name": "PUNE", "link_status": "linked"},
        ])
        targets = rto_fetch.load_rto_targets(path, states=["MH"])
        self.assertEqual([t["legacy_rto_code"] for t in targets], ["MH12"])

    def test_real_seed_loads_and_covers_both_ingestible_statuses(self):
        targets = rto_fetch.load_rto_targets()
        self.assertGreater(len(targets), 1600)
        statuses = {t["link_status"] for t in targets}
        self.assertEqual(statuses, {"linked", "portal_only"})


class NormalizeClassLabelTests(unittest.TestCase):
    def test_case_differences_collapse(self):
        self.assertEqual(
            rto_fetch.normalize_class_label("Motor Car"),
            rto_fetch.normalize_class_label("MOTOR CAR"),
        )

    def test_the_motorised_cycle_spelling_gap_collapses(self):
        # D2: the portal drops the ">" and doubles the space, which sent this
        # class to "Others" instead of 2W_Personal.
        self.assertEqual(
            rto_fetch.normalize_class_label("Motorised Cycle (CC  25cc)"),
            rto_fetch.normalize_class_label("MOTORISED CYCLE (CC > 25CC)"),
        )

    def test_distinct_classes_stay_distinct(self):
        # The normalization must not merge two classes that are genuinely
        # different, or counts from one would land on the other.
        self.assertNotEqual(
            rto_fetch.normalize_class_label("TRAILER (COMMERCIAL)"),
            rto_fetch.normalize_class_label("TRAILER (AGRICULTURAL)"),
        )

    def test_every_seeded_class_gets_a_unique_key(self):
        dimensions = rto_fetch.load_vehicle_class_dimensions()
        with rto_fetch.VEHICLE_CLASS_CROSSWALK_PATH.open(newline="", encoding="utf-8") as f:
            seeded = [row["vehicle_class"].strip() for row in csv.DictReader(f)]
        self.assertEqual(len(dimensions), len(seeded))


class LookupDimensionsTests(unittest.TestCase):
    def test_matches_case_insensitively(self):
        # Portal says "Motor Car"; the mapping file says "MOTOR CAR".
        dimensions = rto_fetch.load_vehicle_class_dimensions()
        _, vtype, category, use_type = rto_fetch.lookup_dimensions("Motor Car", dimensions)
        self.assertEqual((vtype, category, use_type), ("4W_Personal", "4-Wheelers", "Personal"))

    def test_returns_v1_spelling_not_the_portals(self):
        # D1: writing the portal's title case would break every join against
        # fact_ev_data_by_rto, which stores upper case.
        dimensions = rto_fetch.load_vehicle_class_dimensions()
        canonical, *_ = rto_fetch.lookup_dimensions("Motor Car", dimensions)
        self.assertEqual(canonical, "MOTOR CAR")

    def test_motorised_cycle_resolves_to_two_wheeler_not_others(self):
        # D2 end to end: this used to come back as Others/Others/Others.
        dimensions = rto_fetch.load_vehicle_class_dimensions()
        canonical, vtype, category, use_type = rto_fetch.lookup_dimensions(
            "Motorised Cycle (CC  25cc)", dimensions
        )
        self.assertEqual(canonical, "MOTORISED CYCLE (CC > 25CC)")
        self.assertEqual((vtype, category, use_type), ("2W_Personal", "2-Wheelers", "Personal"))

    def test_unknown_class_defaults_to_others_and_warns(self):
        with self.assertLogs(rto_fetch.logger, level="WARNING"):
            resolved = rto_fetch.lookup_dimensions("FLYING CAR", {})
        # Keeps the source's own label rather than inventing a canonical name.
        self.assertEqual(resolved, ("FLYING CAR", "Others", "Others", "Others"))

    def test_real_seed_resolves_a_known_class(self):
        dimensions = rto_fetch.load_vehicle_class_dimensions()
        self.assertIn(rto_fetch.normalize_class_label("MOTOR CAR"), dimensions)


def _portal_stub(class_breakdown, fuel_breakdowns, monthly):
    portal = mock.Mock()
    portal.get_class_distribution.return_value = class_breakdown
    portal.get_fuel_type_breakdown.side_effect = lambda **kw: fuel_breakdowns[kw["vehicle_classes"]]
    portal.get_duration_wise_registration.side_effect = (
        lambda **kw: monthly[(kw["vehicle_classes"], kw["vehicle_fuels"][0])]
    )
    return portal


class FetchRtoYearTests(unittest.TestCase):
    def setUp(self):
        self.rto = {
            "state": "Maharashtra", "state_code": "MH", "new_rto_code": "12",
            "new_rto_name": "PUNE", "legacy_rto_code": "MH12",
            "legacy_rto_name": "PUNE", "link_status": "linked",
        }
        # Keyed the way load_vehicle_class_dimensions keys it, and carrying the
        # canonical V1 spelling as the first element.
        self.dimensions = {
            rto_fetch.normalize_class_label("MOTOR CAR"): ("MOTOR CAR", "4W", "LMV", "Personal")
        }

    def _fetch_with_gaps(self, portal, status_scopes=None):
        return rto_fetch.fetch_rto_year(
            portal, self.rto, 2026, self.dimensions,
            status_scopes=status_scopes, sleep_seconds=0, sleep_func=lambda _: None,
        )

    def _fetch(self, portal, status_scopes=None):
        rows, _ = self._fetch_with_gaps(portal, status_scopes)
        return rows

    def test_builds_one_row_per_month_and_class(self):
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [100]},
            {"Motor Car": {"labels": ["PETROL", "PURE EV"], "data": [70, 30]}},
            {
                ("Motor Car", "PETROL"): [
                    {"yearAsString": "2026-January", "registeredVehicleCount": 40},
                    {"yearAsString": "2026-February", "registeredVehicleCount": 30},
                ],
                ("Motor Car", "PURE EV"): [
                    {"yearAsString": "2026-January", "registeredVehicleCount": 30},
                ],
            },
        )

        rows = self._fetch(portal)

        # 2 months x 1 class x 1 status scope (ALL_STATUSES only by default)
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["status_scope"] for r in rows}, {"ALL_STATUSES"})
        january = next(r for r in rows if r["month"] == 1)
        self.assertEqual(january["petrol"], 40)
        self.assertEqual(january["pure_ev"], 30)
        self.assertEqual(january["total"], 70)
        self.assertEqual(january["date"], "2026-01-01")
        self.assertEqual(january["rto_code"], 12)
        self.assertEqual(january["legacy_rto_code"], "MH12")
        self.assertEqual(january["vehicle_type"], "4W")

    def test_total_equals_the_sum_of_reported_fuels(self):
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [10]},
            {"Motor Car": {"labels": ["PETROL", "DIESEL"], "data": [6, 4]}},
            {
                ("Motor Car", "PETROL"): [{"yearAsString": "2026-March", "registeredVehicleCount": 6}],
                ("Motor Car", "DIESEL"): [{"yearAsString": "2026-March", "registeredVehicleCount": 4}],
            },
        )

        for row in self._fetch(portal):
            fuels = sum(row[c] for c in schema.FUEL_COLUMNS if row[c] != "")
            self.assertEqual(row["total"], fuels)

    def test_unreported_fuels_stay_blank_rather_than_zero(self):
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [5]},
            {"Motor Car": {"labels": ["PETROL"], "data": [5]}},
            {("Motor Car", "PETROL"): [{"yearAsString": "2026-April", "registeredVehicleCount": 5}]},
        )

        row = self._fetch(portal)[0]

        self.assertEqual(row["diesel"], "")
        self.assertEqual(row["bio_methane"], "")

    def test_zero_count_classes_and_fuels_are_never_queried(self):
        portal = _portal_stub(
            {"labels": ["Motor Car", "Ambulance"], "data": [5, 0]},
            {"Motor Car": {"labels": ["PETROL", "DIESEL"], "data": [5, 0]}},
            {("Motor Car", "PETROL"): [{"yearAsString": "2026-May", "registeredVehicleCount": 5}]},
        )

        self._fetch(portal)

        queried_classes = {c.kwargs["vehicle_classes"] for c in portal.get_fuel_type_breakdown.call_args_list}
        self.assertEqual(queried_classes, {"Motor Car"})
        queried_fuels = {c.kwargs["vehicle_fuels"][0] for c in portal.get_duration_wise_registration.call_args_list}
        self.assertEqual(queried_fuels, {"PETROL"})

    def test_unknown_fuel_label_is_dropped_with_a_warning(self):
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [5]},
            {"Motor Car": {"labels": ["ANTIMATTER"], "data": [5]}},
            {},
        )

        with self.assertLogs(rto_fetch.logger, level="WARNING") as captured:
            rows = self._fetch(portal)

        self.assertEqual(rows, [])
        self.assertIn("ANTIMATTER", captured.output[0])

    def test_defaults_to_all_statuses_only(self):
        # Decided 2026-08-19: ACTIVE is an as-of-today measure that decays with
        # age, so it is not ingested. See new_portal/schema.py.
        portal = _portal_stub({"labels": [], "data": []}, {}, {})

        self._fetch(portal)

        used = [c.kwargs["archive_types"] for c in portal.get_class_distribution.call_args_list]
        self.assertEqual(used, [rto_fetch.ARCHIVE_TYPES_ALL_STATUSES])

    def test_active_scope_can_still_be_requested_explicitly(self):
        portal = _portal_stub({"labels": [], "data": []}, {}, {})

        self._fetch(portal, status_scopes=["ALL_STATUSES", "ACTIVE"])

        used = [c.kwargs["archive_types"] for c in portal.get_class_distribution.call_args_list]
        self.assertEqual(
            used, [rto_fetch.ARCHIVE_TYPES_ALL_STATUSES, rto_fetch.ARCHIVE_TYPES_ACTIVE_ONLY]
        )

    def test_each_extra_scope_costs_another_full_pass(self):
        # Guards the doubling claim in the docs: no endpoint returns more than
        # one archive-status scope per request.
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [5]},
            {"Motor Car": {"labels": ["PETROL"], "data": [5]}},
            {("Motor Car", "PETROL"): [{"yearAsString": "2026-June", "registeredVehicleCount": 5}]},
        )

        self._fetch(portal, status_scopes=["ALL_STATUSES"])
        one_scope = portal.get_duration_wise_registration.call_count
        portal.get_duration_wise_registration.reset_mock()
        self._fetch(portal, status_scopes=["ALL_STATUSES", "ACTIVE"])

        self.assertEqual(portal.get_duration_wise_registration.call_count, one_scope * 2)


class WriteRowsTests(unittest.TestCase):
    def test_header_matches_schema_exactly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "out.csv"
            rto_fetch.write_rows([], path)
            with path.open(newline="", encoding="utf-8") as f:
                header = next(csv.reader(f))
        self.assertEqual(header, schema.CSV_COLUMNS)


class IngestorWiringTests(unittest.TestCase):
    """The ingestor must look for exactly the file the fetcher writes.

    NewPortalRtoIngest is not instantiated here: its base class reads real DB
    credentials from config.yaml at construction time, which does not exist in
    CI. The wiring these assert on is all module-level.
    """

    def test_ingestor_reads_the_path_the_fetcher_writes(self):
        from new_portal import rto_ingest

        self.assertIs(rto_ingest.build_output_path, rto_fetch.build_output_path)
        self.assertEqual(rto_ingest.FILE_PREFIX, rto_fetch.FILE_PREFIX)

    def test_replacement_scope_columns_exist_in_the_schema(self):
        from new_portal import rto_ingest

        for column in rto_ingest.REPLACEMENT_SCOPE_COLUMNS:
            self.assertIn(column, schema.CSV_COLUMNS)

    def test_replacement_scope_is_year_wide_not_month_wide(self):
        # A fetch pulls a whole year, so the delete must clear the whole year;
        # scoping to the month would strand rows for months the refetch dropped.
        from new_portal import rto_ingest

        self.assertIn("year", rto_ingest.REPLACEMENT_SCOPE_COLUMNS)
        self.assertNotIn("date", rto_ingest.REPLACEMENT_SCOPE_COLUMNS)

    def test_status_scope_is_in_the_replacement_scope(self):
        # Otherwise ingesting one scope would wipe the other's rows.
        from new_portal import rto_ingest

        self.assertIn("status_scope", rto_ingest.REPLACEMENT_SCOPE_COLUMNS)

    def test_state_code_is_in_the_replacement_scope(self):
        # Otherwise a --states MH run would delete every other state's rows
        # for that year, since the delete matches staging via EXISTS.
        from new_portal import rto_ingest

        self.assertIn("state_code", rto_ingest.REPLACEMENT_SCOPE_COLUMNS)

    def test_replacement_scope_excludes_rto_code(self):
        # Deliberate: scoping to the RTO would strand rows for an office that
        # legitimately reported nothing this run (V1's orphaned-row bug).
        from new_portal import rto_ingest

        self.assertNotIn("rto_code", rto_ingest.REPLACEMENT_SCOPE_COLUMNS)


class GapIsolationTests(unittest.TestCase):
    """A portal 500 on one call must not discard the whole office.

    This is the 2026-09-23 failure: a drifting subset of (RTO, class) pairs
    returned 500, and because the only try/except sat at the RTO level, 7 of 10
    Mizoram offices produced nothing despite most classes being fetchable.
    """

    def setUp(self):
        self.rto = {
            "state": "Mizoram", "state_code": "MZ", "new_rto_code": "1",
            "new_rto_name": "AIZAWL DTO", "legacy_rto_code": "MZ1",
            "legacy_rto_name": "AIZAWL", "link_status": "linked",
        }
        self.dimensions = rto_fetch.load_vehicle_class_dimensions()

    def _fetch(self, portal):
        return rto_fetch.fetch_rto_year(
            portal, self.rto, 2026, self.dimensions,
            sleep_seconds=0, sleep_func=lambda _: None,
        )

    def test_a_poisoned_class_costs_only_that_class(self):
        monthly = {
            ("Motor Car", "PETROL"): [
                {"yearAsString": "2026-January", "registeredVehicleCount": 40}
            ],
        }

        def fuels(**kw):
            if kw["vehicle_classes"] == "Maxi Cab":
                raise client.NewPortalError("fueltypedonutchart returned HTTP 500")
            return {"labels": ["PETROL"], "data": [40]}

        portal = mock.Mock()
        portal.get_class_distribution.return_value = {
            "labels": ["Motor Car", "Maxi Cab"], "data": [40, 7]
        }
        portal.get_fuel_type_breakdown.side_effect = fuels
        portal.get_duration_wise_registration.side_effect = (
            lambda **kw: monthly[(kw["vehicle_classes"], kw["vehicle_fuels"][0])]
        )

        rows, gaps = self._fetch(portal)

        # Motor Car survived.
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["vehicle_class"], "MOTOR CAR")
        # Maxi Cab is named, not silently absent.
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].stage, rto_fetch.STAGE_FUEL_BREAKDOWN)
        self.assertEqual(gaps[0].vehicle_class, "Maxi Cab")
        self.assertEqual(gaps[0].rto_label, "MZ/1 AIZAWL DTO")

    def test_a_poisoned_fuel_costs_only_that_fuel(self):
        def monthly(**kw):
            if kw["vehicle_fuels"][0] == "DIESEL":
                raise client.NewPortalError("durationWise… returned HTTP 500")
            return [{"yearAsString": "2026-January", "registeredVehicleCount": 40}]

        portal = mock.Mock()
        portal.get_class_distribution.return_value = {"labels": ["Motor Car"], "data": [50]}
        portal.get_fuel_type_breakdown.return_value = {
            "labels": ["PETROL", "DIESEL"], "data": [40, 10]
        }
        portal.get_duration_wise_registration.side_effect = monthly

        rows, gaps = self._fetch(portal)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["petrol"], 40)
        # The lost diesel count is blank, never 0 — a 0 would be a fabricated count.
        self.assertEqual(rows[0]["diesel"], "")
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].stage, rto_fetch.STAGE_MONTHLY)
        self.assertEqual(gaps[0].fuel_label, "DIESEL")

    def test_class_distribution_failure_gaps_the_whole_scope(self):
        portal = mock.Mock()
        portal.get_class_distribution.side_effect = client.NewPortalError("500")

        rows, gaps = self._fetch(portal)

        self.assertEqual(rows, [])
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].stage, rto_fetch.STAGE_CLASS_DISTRIBUTION)
        self.assertIsNone(gaps[0].vehicle_class)
        portal.get_fuel_type_breakdown.assert_not_called()

    def test_a_clean_office_reports_no_gaps(self):
        portal = _portal_stub(
            {"labels": ["Motor Car"], "data": [5]},
            {"Motor Car": {"labels": ["PETROL"], "data": [5]}},
            {("Motor Car", "PETROL"): [
                {"yearAsString": "2026-June", "registeredVehicleCount": 5}
            ]},
        )

        rows, gaps = self._fetch(portal)

        self.assertEqual(len(rows), 1)
        self.assertEqual(gaps, [])

    def test_non_portal_errors_still_propagate(self):
        # A bug in our own parsing must not be filed as "the portal refused it".
        portal = mock.Mock()
        portal.get_class_distribution.side_effect = KeyError("yearAsString")

        with self.assertRaises(KeyError):
            self._fetch(portal)


class GapManifestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.gap = rto_fetch.Gap(
            state_code="MZ", rto_code=4, rto_name="CHAMPHAI",
            status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_FUEL_BREAKDOWN,
            vehicle_class="Maxi Cab", error="HTTP 500",
        )

    def test_manifest_sits_beside_the_csv(self):
        csv_path = rto_fetch.build_output_path(2026, Path(self.tmp.name))
        manifest = rto_fetch.build_gap_manifest_path(2026, Path(self.tmp.name))
        self.assertEqual(manifest.parent, csv_path.parent)
        self.assertTrue(manifest.name.endswith("__gaps.json"))

    def test_manifest_records_every_gap_and_the_affected_offices(self):
        path = Path(self.tmp.name) / "gaps.json"
        rto_fetch.write_gap_manifest([self.gap], 2026, path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["year"], 2026)
        self.assertEqual(payload["gap_count"], 1)
        self.assertEqual(payload["affected_rtos"], ["MZ/4 CHAMPHAI"])
        self.assertEqual(payload["gaps"][0]["vehicle_class"], "Maxi Cab")
        self.assertEqual(payload["gaps"][0]["stage"], rto_fetch.STAGE_FUEL_BREAKDOWN)

    def test_describe_names_the_office_scope_stage_and_labels(self):
        described = self.gap.describe()
        for expected in ("MZ/4 CHAMPHAI", "ALL_STATUSES", "fuel_breakdown", "Maxi Cab"):
            self.assertIn(expected, described)


class RecoverGapsTests(unittest.TestCase):
    def setUp(self):
        self.rto = {
            "state": "Mizoram", "state_code": "MZ", "new_rto_code": "4",
            "new_rto_name": "CHAMPHAI", "legacy_rto_code": "MZ4",
            "legacy_rto_name": "CHAMPHAI", "link_status": "linked",
        }
        self.label = "MZ/4 CHAMPHAI"
        self.targets = {self.label: self.rto}
        self.gap = rto_fetch.Gap(
            state_code="MZ", rto_code=4, rto_name="CHAMPHAI",
            status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_MONTHLY,
            vehicle_class="Motor Car", fuel_label="DIESEL", error="HTTP 500",
        )

    def _recover(self, fetch_result):
        with mock.patch.object(rto_fetch, "fetch_rto_year", side_effect=fetch_result):
            return rto_fetch.recover_gaps(
                [self.gap], self.targets, 2026, {},
                client_factory=mock.Mock(),
                sleep_func=lambda _: None,
                settle_seconds=0,
            )

    def test_no_gaps_means_no_second_pass(self):
        factory = mock.Mock()
        recovered, remaining = rto_fetch.recover_gaps(
            [], self.targets, 2026, {}, client_factory=factory
        )
        self.assertEqual(recovered, {})
        self.assertEqual(remaining, [])
        factory.assert_not_called()

    def test_a_successful_retry_replaces_the_offices_rows(self):
        recovered, remaining = self._recover(lambda *a, **k: ([{"total": 9}], []))
        self.assertEqual(recovered, {self.label: [{"total": 9}]})
        self.assertEqual(remaining, [])

    def test_a_retry_that_does_not_improve_keeps_the_first_pass(self):
        # Otherwise a worse retry could shrink data we already had.
        worse = rto_fetch.Gap(
            state_code="MZ", rto_code=4, rto_name="CHAMPHAI",
            status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_CLASS_DISTRIBUTION,
        )
        with self.assertLogs(rto_fetch.logger, level="WARNING"):
            recovered, remaining = self._recover(lambda *a, **k: ([], [worse]))
        self.assertEqual(recovered, {})
        self.assertEqual(remaining, [self.gap])

    def test_a_retry_that_raises_keeps_the_original_gap(self):
        with self.assertLogs(rto_fetch.logger, level="ERROR"):
            recovered, remaining = self._recover(RuntimeError("connection reset"))
        self.assertEqual(recovered, {})
        self.assertEqual(remaining, [self.gap])

    def test_it_waits_before_retrying(self):
        # The 500s clear over minutes; an immediate retry mostly re-hits them.
        slept = []
        with mock.patch.object(rto_fetch, "fetch_rto_year", return_value=([], [])):
            rto_fetch.recover_gaps(
                [self.gap], self.targets, 2026, {},
                client_factory=mock.Mock(),
                sleep_func=slept.append,
                settle_seconds=120,
            )
        self.assertIn(120, slept)

    def test_it_builds_a_fresh_session(self):
        factory = mock.Mock()
        with mock.patch.object(rto_fetch, "fetch_rto_year", return_value=([], [])):
            rto_fetch.recover_gaps(
                [self.gap], self.targets, 2026, {},
                client_factory=factory,
                sleep_func=lambda _: None,
                settle_seconds=0,
                workers=1,
            )
        factory.assert_called_once()

    def test_it_recovers_offices_in_parallel(self):
        """Sequential recovery was slower than the pass it patched up.

        At 83% of offices carrying a gap, one-at-a-time recovery over ~1,391
        offices is roughly 8 hours — longer than the main pass. It must fan out.
        """
        targets = {
            f"MZ/{i} OFFICE {i}": {
                "state": "Mizoram", "state_code": "MZ", "new_rto_code": str(i),
                "new_rto_name": f"OFFICE {i}", "legacy_rto_code": f"MZ{i}",
                "legacy_rto_name": f"OFFICE {i}", "link_status": "linked",
            }
            for i in range(1, 9)
        }
        gaps = [
            rto_fetch.Gap(
                state_code="MZ", rto_code=i, rto_name=f"OFFICE {i}",
                status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_MONTHLY,
                vehicle_class="Motor Car", fuel_label="DIESEL",
            )
            for i in range(1, 9)
        ]
        # A barrier makes this deterministic: it only releases once `workers`
        # calls are in flight at the same moment. Counting distinct thread names
        # would pass spuriously, since a fast mock lets one thread drain the
        # whole queue before the others are scheduled.
        barrier = threading.Barrier(4, timeout=5)

        def wait_for_peers(*a, **k):
            barrier.wait()
            return [{"total": 1}], []

        with mock.patch.object(rto_fetch, "fetch_rto_year", side_effect=wait_for_peers):
            recovered, remaining = rto_fetch.recover_gaps(
                gaps, targets, 2026, {},
                client_factory=mock.Mock(),
                sleep_func=lambda _: None,
                settle_seconds=0,
                workers=4,
            )

        self.assertEqual(len(recovered), 8)
        self.assertEqual(remaining, [])

    def test_each_worker_gets_its_own_session(self):
        # The portal serializes concurrent requests sharing a JSESSIONID, so
        # sharing one client across workers would erase the parallelism.
        targets = {
            f"MZ/{i} OFFICE {i}": {
                "state": "Mizoram", "state_code": "MZ", "new_rto_code": str(i),
                "new_rto_name": f"OFFICE {i}", "legacy_rto_code": f"MZ{i}",
                "legacy_rto_name": f"OFFICE {i}", "link_status": "linked",
            }
            for i in range(1, 5)
        }
        gaps = [
            rto_fetch.Gap(
                state_code="MZ", rto_code=i, rto_name=f"OFFICE {i}",
                status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_MONTHLY,
            )
            for i in range(1, 5)
        ]
        factory = mock.Mock(side_effect=lambda: mock.Mock())
        seen = set()

        def record_client(portal, *a, **k):
            seen.add(id(portal))
            return [{"total": 1}], []

        with mock.patch.object(rto_fetch, "fetch_rto_year", side_effect=record_client):
            rto_fetch.recover_gaps(
                gaps, targets, 2026, {},
                client_factory=factory,
                sleep_func=lambda _: None,
                settle_seconds=0,
                workers=4,
            )

        # One client per thread that actually ran, never one shared by all.
        self.assertEqual(factory.call_count, len(seen))


class ProgressCheckpointTests(unittest.TestCase):
    """Resume across portal windows.

    The portal swings between serving and dead within minutes — three runs on
    2026-09-23 reached 26, 133 and 320 offices before conditions collapsed, and
    each abort discarded everything. Checkpointing turns an abort into a resume
    point so the dataset accumulates across short good windows.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "progress.jsonl"
        self.gap = rto_fetch.Gap(
            state_code="MZ", rto_code=1, rto_name="AIZAWL DTO",
            status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_MONTHLY,
            vehicle_class="Motor Car", fuel_label="DIESEL", error="HTTP 500",
        )

    def test_path_sits_beside_the_csv(self):
        csv_path = rto_fetch.build_output_path(2026, Path(self.tmp.name))
        progress = rto_fetch.build_progress_path(2026, Path(self.tmp.name))
        self.assertEqual(progress.parent, csv_path.parent)
        self.assertTrue(progress.name.endswith("__progress.jsonl"))

    def test_a_missing_checkpoint_is_simply_empty(self):
        self.assertEqual(rto_fetch.load_progress(self.path), {})

    def test_round_trips_rows_and_gaps(self):
        rto_fetch.append_progress(self.path, "MZ/1 AIZAWL DTO", [{"total": 5}], [self.gap])
        loaded = rto_fetch.load_progress(self.path)
        rows, gaps = loaded["MZ/1 AIZAWL DTO"]
        self.assertEqual(rows, [{"total": 5}])
        self.assertEqual(gaps, [self.gap])

    def test_a_later_entry_supersedes_an_earlier_one(self):
        # Re-attempting an office appends; the newest result must win.
        rto_fetch.append_progress(self.path, "MZ/1 AIZAWL DTO", [], [self.gap])
        rto_fetch.append_progress(self.path, "MZ/1 AIZAWL DTO", [{"total": 9}], [])
        rows, gaps = rto_fetch.load_progress(self.path)["MZ/1 AIZAWL DTO"]
        self.assertEqual(rows, [{"total": 9}])
        self.assertEqual(gaps, [])

    def test_a_truncated_final_line_does_not_lose_the_file(self):
        # A killed run can leave a half-written line; that costs one re-fetch,
        # not the whole checkpoint.
        rto_fetch.append_progress(self.path, "MZ/1 AIZAWL DTO", [{"total": 5}], [])
        with self.path.open("a", encoding="utf-8") as f:
            f.write('{"rto_label": "MZ/2 LUNGLEI", "rows": [{"tot')
        with self.assertLogs(rto_fetch.logger, level="WARNING"):
            loaded = rto_fetch.load_progress(self.path)
        self.assertEqual(list(loaded), ["MZ/1 AIZAWL DTO"])

    def test_completeness_means_no_gaps_not_no_rows(self):
        # A genuinely empty office is finished; a gapped one is not.
        self.assertTrue(rto_fetch.office_is_complete(([], [])))
        self.assertTrue(rto_fetch.office_is_complete(([{"total": 1}], [])))
        self.assertFalse(rto_fetch.office_is_complete(([], [self.gap])))
        self.assertFalse(rto_fetch.office_is_complete(([{"total": 1}], [self.gap])))

    def test_an_unattempted_office_is_not_recorded_as_complete(self):
        """The abort path must not mark 1,586 untouched offices as done.

        `fetch_one` returns None for an office it skipped (run already aborted)
        or one that crashed. Returning ([], []) instead made those
        indistinguishable from a genuinely empty office, and the run reported
        "1617 of 1676 complete" when the real figure was 31.
        """
        results = [
            (0, [{"total": 5}], []),   # fetched
            (1, None, None),           # skipped after the abort
            (2, None, None),           # crashed
        ]
        progress = {}
        for index, rows, gaps in results:
            if rows is None:
                continue
            progress[f"office-{index}"] = (rows, gaps)

        self.assertEqual(list(progress), ["office-0"])
        complete = sum(1 for v in progress.values() if rto_fetch.office_is_complete(v))
        self.assertEqual(complete, 1)

    def test_unvisited_offices_are_attempted_before_gapped_ones(self):
        """Otherwise every attempt re-walks the same early offices.

        With "everything not complete, in list order", the second real attempt
        tried 385 offices of which only 21 were new — offices past ~450 would
        never be reached however many attempts ran.
        """
        targets = [{"state_code": "XX", "new_rto_code": str(i), "new_rto_name": "O"}
                   for i in range(1, 6)]
        label_of = lambda r: f"{r['state_code']}/{r['new_rto_code']} {r['new_rto_name']}"
        gap = [self.gap]
        progress = {
            label_of(targets[0]): ([], gap),        # gapped, early in the list
            label_of(targets[1]): ([{"t": 1}], []),  # complete
            label_of(targets[2]): ([], gap),        # gapped
        }
        done = {label_of(targets[1])}

        ordered = rto_fetch.order_pending(targets, progress, done, label_of)

        # Offices 4 and 5 have never been seen; they must come first.
        self.assertEqual(
            [label_of(r) for r in ordered],
            ["XX/4 O", "XX/5 O", "XX/1 O", "XX/3 O"],
        )

    def test_completed_offices_are_never_reattempted(self):
        targets = [{"state_code": "XX", "new_rto_code": str(i), "new_rto_name": "O"}
                   for i in range(1, 4)]
        label_of = lambda r: f"{r['state_code']}/{r['new_rto_code']} {r['new_rto_name']}"
        progress = {label_of(t): ([{"t": 1}], []) for t in targets}
        done = set(progress)

        self.assertEqual(rto_fetch.order_pending(targets, progress, done, label_of), [])

    def test_appends_are_safe_from_concurrent_writers(self):
        # Eight workers checkpoint as they finish; no line may be interleaved.
        lock = threading.Lock()

        def write(i):
            with lock:
                rto_fetch.append_progress(self.path, f"MZ/{i} OFFICE", [{"total": i}], [])

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(write, range(40)))

        loaded = rto_fetch.load_progress(self.path)
        self.assertEqual(len(loaded), 40)
        self.assertEqual(loaded["MZ/7 OFFICE"][0], [{"total": 7}])


class RetryPolicyTests(unittest.TestCase):
    def test_in_line_retries_default_to_one_attempt(self):
        """Fail fast in the main pass; the recovery pass does the real retrying.

        3 attempts with 5s linear backoff cost ~15s per failed call and moved a
        Mizoram sweep from 13 failures to only 11 — ~42% of nationwide
        wall-clock for almost nothing.
        """
        self.assertEqual(rto_fetch.DEFAULT_RETRY_ATTEMPTS, 1)

    def test_the_client_still_defaults_to_patient_retries(self):
        # Only the bulk fetch opts out; other callers keep the safer default.
        self.assertGreater(client.DEFAULT_RETRY_ATTEMPTS, 1)


class AbortThresholdTests(unittest.TestCase):
    """Guards the 2026-08-20 outage behaviour.

    The portal's /analytics backend went down mid-run; because a failed session
    creation was not cached, every RTO retried it and 96 failed RTOs produced
    401 requests against an already-struggling service.
    """

    def test_abort_window_is_set_and_modest(self):
        self.assertGreater(rto_fetch.ABORT_MIN_SAMPLE, 0)
        self.assertLessEqual(rto_fetch.ABORT_MIN_SAMPLE, rto_fetch.ABORT_WINDOW)
        self.assertLess(
            rto_fetch.ABORT_WINDOW,
            len(rto_fetch.load_rto_targets()),
            "aborting must trip well before the full RTO list is exhausted",
        )

    def test_a_client_is_built_once_per_thread_not_once_per_rto(self):
        # The amplification bug: a client that fails to start was never cached,
        # so every subsequent RTO on that thread retried session creation.
        source = (Path(rto_fetch.__file__)).read_text()
        self.assertIn("thread_state.client = client", source)
        self.assertIn("preflight_client", source)


class ShouldAbortTests(unittest.TestCase):
    """A dead portal must stop the run; a merely bad one must not.

    "N consecutive failures" could not tell those apart. Failures here are
    per-office and clustered, so a small state whose offices all fail together
    produced a long consecutive run while the portal was still serving most of
    the country — it killed a run at office 133 that already held 5,072 rows
    from 71 healthy offices.
    """

    def test_a_dead_portal_aborts(self):
        self.assertTrue(rto_fetch.should_abort([True] * 50))

    def test_the_real_degraded_run_would_not_have_aborted(self):
        # 37% failure, the rate that killed the previous run. 71 offices were
        # returning real rows at the time.
        window = [True] * 19 + [False] * 31
        self.assertFalse(rto_fetch.should_abort(window))

    def test_a_clustered_run_of_failures_does_not_abort_on_its_own(self):
        # 25 consecutive failures inside a window that is otherwise healthy —
        # exactly the Arunachal Pradesh cluster.
        window = [False] * 25 + [True] * 25
        self.assertFalse(rto_fetch.should_abort(window))

    def test_threshold_is_at_the_boundary(self):
        self.assertTrue(rto_fetch.should_abort([True] * 45 + [False] * 5))
        self.assertFalse(rto_fetch.should_abort([True] * 44 + [False] * 6))

    def test_an_empty_window_never_aborts(self):
        self.assertFalse(rto_fetch.should_abort([]))

    def test_the_breaker_needs_a_minimum_sample(self):
        # Otherwise the first few offices, which are Andaman islands, could
        # abort a healthy run before it reaches the mainland.
        self.assertGreaterEqual(rto_fetch.ABORT_MIN_SAMPLE, 25)

class OfficeFailureClassificationTests(unittest.TestCase):
    """What counts toward the circuit breaker.

    Two regressions live here. Gap isolation first made every office that
    *returned* a success, which disabled the breaker on a degraded portal — the
    case it exists for. Over-correcting to "no rows = failure" then counted
    genuinely empty offices, which would abort a healthy run: the first 13
    offices in the national list are Andaman islands with no 2026 data.
    """

    GAP = rto_fetch.Gap(
        state_code="MZ", rto_code=1, rto_name="AIZAWL DTO",
        status_scope="ALL_STATUSES", stage=rto_fetch.STAGE_CLASS_DISTRIBUTION,
    )

    def test_rows_are_a_success(self):
        self.assertFalse(rto_fetch.office_counts_as_failure([{"total": 1}], []))

    def test_rows_with_gaps_are_still_a_success(self):
        # Partial data means the office answered; the recovery pass owns gaps.
        self.assertFalse(rto_fetch.office_counts_as_failure([{"total": 1}], [self.GAP]))

    def test_a_genuinely_empty_office_is_not_a_failure(self):
        # No rows AND no gaps = the office reported nothing, and nothing failed.
        self.assertFalse(rto_fetch.office_counts_as_failure([], []))

    def test_no_rows_with_gaps_is_a_failure(self):
        # Every call failed — indistinguishable from a hard failure.
        self.assertTrue(rto_fetch.office_counts_as_failure([], [self.GAP]))

    def test_twenty_five_empty_offices_would_not_trip_the_breaker(self):
        # The Andaman case, end to end.
        empties = [rto_fetch.office_counts_as_failure([], []) for _ in range(25)]
        self.assertEqual(sum(empties), 0)


if __name__ == "__main__":
    unittest.main()
