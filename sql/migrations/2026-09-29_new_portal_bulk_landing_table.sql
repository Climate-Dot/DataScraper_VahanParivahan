-- Landing table for BULK INSERT from Azure Blob Storage.
--
-- WHY: pushing a year of rows up as parameterised INSERTs is network-bound and
-- slow — 170,718 rows took ~36 minutes, with the database idle at 0% CPU the
-- whole time because the cost is one round trip per batch. Azure SQL can read
-- the same CSV straight out of blob storage, which the pipeline already writes
-- there for archival. Measured 2026-09-29: the same file loaded in 16.5
-- seconds, ~130x faster.
--
-- WHY A SEPARATE TABLE: BULK INSERT maps CSV columns positionally, and the
-- staging table carries an extra `inserted_at` column that the CSV does not.
-- Rather than maintain a format file that must track column order, the CSV
-- lands in a table shaped exactly like the file, and a single server-side
-- INSERT ... SELECT adds the timestamp on the way into staging.
--
-- The column definitions are copied verbatim from staging_fact_ev_data_by_rto_v2
-- minus inserted_at, so the two cannot drift in type. tests/ asserts this.
--
-- ROLLBACK:
--   DROP TABLE dbo.landing_fact_ev_data_by_rto_v2;

IF OBJECT_ID('dbo.landing_fact_ev_data_by_rto_v2') IS NOT NULL
BEGIN
    PRINT 'landing_fact_ev_data_by_rto_v2 already exists; nothing to do.';
END
ELSE
BEGIN
    CREATE TABLE dbo.landing_fact_ev_data_by_rto_v2 (
    [year] INT NOT NULL,
    [month] INT NOT NULL,
    [date] DATE NOT NULL,
    [state] NVARCHAR(100) NOT NULL,
    [state_code] NVARCHAR(4) NOT NULL,
    [rto_code] INT NOT NULL,
    [rto_name] NVARCHAR(200) NOT NULL,
    [legacy_rto_code] NVARCHAR(20) NULL,
    [vehicle_class] NVARCHAR(120) NOT NULL,
    [vehicle_type] NVARCHAR(100) NULL,
    [vehicle_category] NVARCHAR(100) NULL,
    [vehicle_use_type] NVARCHAR(100) NULL,
    [status_scope] NVARCHAR(20) NOT NULL,
    [bio_cng_bio_gas] INT NULL,
    [bio_diesel_b100] INT NULL,
    [bio_methane] INT NULL,
    [cng_only] INT NULL,
    [di_methyl_ether] INT NULL,
    [diesel] INT NULL,
    [diesel_hybrid] INT NULL,
    [dual_diesel_bio_cng] INT NULL,
    [dual_diesel_cng] INT NULL,
    [dual_diesel_lng] INT NULL,
    [electric_bov] INT NULL,
    [ethanol_e100] INT NULL,
    [flex_fuel_bio_diesel] INT NULL,
    [flex_fuel_ethanol] INT NULL,
    [fuel_cell_hydrogen] INT NULL,
    [hcng] INT NULL,
    [hydrogen_ice] INT NULL,
    [lng] INT NULL,
    [lpg_only] INT NULL,
    [methanol] INT NULL,
    [not_applicable] INT NULL,
    [petrol] INT NULL,
    [petrol_cng] INT NULL,
    [petrol_e20] INT NULL,
    [petrol_e20_cng] INT NULL,
    [petrol_e20_hybrid] INT NULL,
    [petrol_e20_hybrid_cng] INT NULL,
    [petrol_e20_lpg] INT NULL,
    [petrol_hybrid] INT NULL,
    [petrol_hybrid_cng] INT NULL,
    [petrol_lpg] INT NULL,
    [petrol_methanol] INT NULL,
    [plug_in_hybrid_ev] INT NULL,
    [pure_ev] INT NULL,
    [solar] INT NULL,
    [strong_hybrid_ev] INT NULL,
    [total] INT NULL
    );
    PRINT 'Created dbo.landing_fact_ev_data_by_rto_v2.';
END
GO

SELECT COUNT(*) AS landing_column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'landing_fact_ev_data_by_rto_v2';
GO
