import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    REPO_ROOT / "sql" / "migrations" / "2026-06-19_vahan_fuel_schema_refresh.sql"
)


class MigrationContractTests(unittest.TestCase):
    def test_migration_has_backup_and_transaction_safety(self):
        sql = MIGRATION_PATH.read_text()
        self.assertIn("BEGIN TRANSACTION", sql)
        self.assertIn("COMMIT TRANSACTION", sql)
        self.assertIn("ROLLBACK TRANSACTION", sql)
        self.assertIn("fact_ev_data_by_rto_backup_20260619", sql)
        self.assertIn("fact_ev_data_by_state_backup_20260619", sql)
        self.assertIn("fact_oem_data_by_state_and_category_backup_20260619", sql)

    def test_migration_covers_new_shared_columns(self):
        sql = MIGRATION_PATH.read_text()
        for column_name in [
            "bio_cng_bio_gas",
            "bio_diesel_b100",
            "bio_methane",
            "ethanol_e100",
            "flex_fuel_bio_diesel",
            "flex_fuel_ethanol",
            "hcng",
            "hydrogen_ice",
            "petrol_e20",
            "petrol_e20_cng",
            "petrol_e20_hybrid",
            "petrol_e20_hybrid_cng",
            "petrol_e20_lpg",
            "petrol_hybrid_cng",
            "inserted_at",
        ]:
            self.assertIn(column_name, sql)

    def test_migration_handles_legacy_vm_drift(self):
        sql = MIGRATION_PATH.read_text()
        self.assertIn("electric_vehicles", sql)
        self.assertIn("insert_date", sql)
        self.assertIn("COL_LENGTH", sql)


if __name__ == "__main__":
    unittest.main()
