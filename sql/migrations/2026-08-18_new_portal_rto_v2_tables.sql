-- V2 (new portal / analytics.parivahan.gov.in) RTO-grain tables.
--
-- Creates NEW tables only. Touches nothing that already exists: V1's
-- fact_ev_data_by_rto, rto_wise_ev_data and every *_backup_* table are left
-- exactly as they are (decision 1, confirmed with Monish 2026-08-18). There is
-- therefore no backup step here and nothing to roll back except dropping these
-- two tables again -- see the rollback block at the bottom.
--
-- Column notes:
--   * Real types, not V1's blanket NVARCHAR(MAX). V1's curated dbt models have
--     to CAST(... AS INT) every count, which is exactly what broke when a
--     preprocessing bug started emitting "12.0" instead of "12" (see CLAUDE.md's
--     August 2026 remediation, item 2). A brand-new table has no reason to
--     inherit that. FLAGGED FOR REVIEW: decision 2 said "flat, matching V1" --
--     that was about table *shape*, and this keeps the shape flat; it departs
--     from V1 only on column types.
--   * No `day` column: the source has no day-level granularity and inventing
--     one would be fabricating precision the portal does not provide.
--   * bio_diesel_b100 and bio_methane exist but the new portal never reports
--     them, so they stay NULL -- NULL, not 0, per CLAUDE.md safety rule 4.
--   * status_scope ('ACTIVE' | 'ALL_STATUSES') is part of the grain, so every
--     query MUST filter on it or it will double-count (decision 6).

SET NOCOUNT ON;
SET XACT_ABORT ON;

IF OBJECT_ID('dbo.fact_ev_data_by_rto_v2', 'U') IS NOT NULL
BEGIN
    RAISERROR('dbo.fact_ev_data_by_rto_v2 already exists. Aborting rather than redefining a live table.', 16, 1);
    RETURN;
END;

IF OBJECT_ID('dbo.staging_fact_ev_data_by_rto_v2', 'U') IS NOT NULL
BEGIN
    RAISERROR('dbo.staging_fact_ev_data_by_rto_v2 already exists. Aborting rather than redefining a live table.', 16, 1);
    RETURN;
END;

CREATE TABLE dbo.staging_fact_ev_data_by_rto_v2 (
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
    [total] INT NULL,
    [inserted_at] DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE dbo.fact_ev_data_by_rto_v2 (
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
    [total] INT NULL,
    [inserted_at] DATETIME NOT NULL DEFAULT GETDATE()
);

-- Ingestion deletes by (year, status_scope) and the curated layer reads by
-- month/RTO, so index both access paths.
CREATE INDEX ix_fact_ev_data_by_rto_v2_year_scope
    ON dbo.fact_ev_data_by_rto_v2 ([year], [status_scope]);

CREATE INDEX ix_fact_ev_data_by_rto_v2_grain
    ON dbo.fact_ev_data_by_rto_v2 ([date], [state_code], [rto_code], [status_scope]);

-- Rollback (nothing else was modified, so this is the whole undo):
--   DROP TABLE dbo.fact_ev_data_by_rto_v2;
--   DROP TABLE dbo.staging_fact_ev_data_by_rto_v2;
