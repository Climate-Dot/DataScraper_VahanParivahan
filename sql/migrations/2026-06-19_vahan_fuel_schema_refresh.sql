SET NOCOUNT ON;
SET XACT_ABORT ON;

IF OBJECT_ID('dbo.fact_ev_data_by_rto_backup_20260619', 'U') IS NOT NULL
BEGIN
    RAISERROR('Backup table dbo.fact_ev_data_by_rto_backup_20260619 already exists. Aborting to avoid overwriting a backup.', 16, 1);
    RETURN;
END;

IF OBJECT_ID('dbo.fact_ev_data_by_state_backup_20260619', 'U') IS NOT NULL
BEGIN
    RAISERROR('Backup table dbo.fact_ev_data_by_state_backup_20260619 already exists. Aborting to avoid overwriting a backup.', 16, 1);
    RETURN;
END;

IF OBJECT_ID('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'U') IS NOT NULL
BEGIN
    RAISERROR('Backup table dbo.fact_oem_data_by_state_and_category_backup_20260619 already exists. Aborting to avoid overwriting a backup.', 16, 1);
    RETURN;
END;

SELECT *
INTO dbo.fact_ev_data_by_rto_backup_20260619
FROM dbo.fact_ev_data_by_rto;

SELECT *
INTO dbo.fact_ev_data_by_state_backup_20260619
FROM dbo.fact_ev_data_by_state;

SELECT *
INTO dbo.fact_oem_data_by_state_and_category_backup_20260619
FROM dbo.fact_oem_data_by_state_and_category;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID('dbo.staging_fact_ev_data_by_rto', 'U') IS NOT NULL
        DROP TABLE dbo.staging_fact_ev_data_by_rto;

    IF OBJECT_ID('dbo.staging_fact_ev_data_by_state', 'U') IS NOT NULL
        DROP TABLE dbo.staging_fact_ev_data_by_state;

    IF OBJECT_ID('dbo.staging_fact_oem_data_by_state_and_category', 'U') IS NOT NULL
        DROP TABLE dbo.staging_fact_oem_data_by_state_and_category;

    IF OBJECT_ID('dbo.fact_ev_data_by_rto', 'U') IS NOT NULL
        DROP TABLE dbo.fact_ev_data_by_rto;

    IF OBJECT_ID('dbo.fact_ev_data_by_state', 'U') IS NOT NULL
        DROP TABLE dbo.fact_ev_data_by_state;

    IF OBJECT_ID('dbo.fact_oem_data_by_state_and_category', 'U') IS NOT NULL
        DROP TABLE dbo.fact_oem_data_by_state_and_category;

CREATE TABLE dbo.staging_fact_ev_data_by_rto (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [rto_name] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [rto_code] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

CREATE TABLE dbo.fact_ev_data_by_rto (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [rto_name] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [rto_code] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

CREATE TABLE dbo.staging_fact_ev_data_by_state (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

CREATE TABLE dbo.fact_ev_data_by_state (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

CREATE TABLE dbo.staging_fact_oem_data_by_state_and_category (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [maker] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

CREATE TABLE dbo.fact_oem_data_by_state_and_category (
    [year] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [month] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [day] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [date] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_class] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_category] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [vehicle_use_type] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [maker] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_cng_bio_gas] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_diesel_b100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [bio_methane] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [cng_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [diesel_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [di_methyl_ether] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_bio_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [dual_diesel_lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [electric_bov] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ethanol_e100] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_bio_diesel] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [flex_fuel_ethanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [fuel_cell_hydrogen] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hcng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [hydrogen_ice] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lpg_only] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [not_applicable] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_e20_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_hybrid_cng] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_lpg] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [petrol_methanol] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [plug_in_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [pure_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [solar] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [strong_hybrid_ev] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [total] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [inserted_at] DATETIME DEFAULT GETDATE() NULL
);

DECLARE @rto_insert_sql NVARCHAR(MAX) = N'
INSERT INTO dbo.fact_ev_data_by_rto (
    [year], [month], [day], [date], [state], [rto_name], [rto_code],
    [vehicle_type], [vehicle_category], [vehicle_use_type], [vehicle_class],
    [bio_cng_bio_gas], [bio_diesel_b100], [bio_methane], [cng_only], [diesel],
    [diesel_hybrid], [di_methyl_ether], [dual_diesel_bio_cng], [dual_diesel_cng],
    [dual_diesel_lng], [electric_bov], [ethanol_e100], [flex_fuel_bio_diesel],
    [flex_fuel_ethanol], [fuel_cell_hydrogen], [hcng], [hydrogen_ice], [lng],
    [lpg_only], [methanol], [not_applicable], [petrol], [petrol_cng], [petrol_e20],
    [petrol_e20_cng], [petrol_e20_hybrid], [petrol_e20_hybrid_cng], [petrol_e20_lpg],
    [petrol_hybrid], [petrol_hybrid_cng], [petrol_lpg], [petrol_methanol],
    [plug_in_hybrid_ev], [pure_ev], [solar], [strong_hybrid_ev], [total], [inserted_at]
)
SELECT
    [year], [month], [day], [date], [state], [rto_name], [rto_code],
    [vehicle_type], [vehicle_category], [vehicle_use_type], [vehicle_class],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'bio_cng_bio_gas') IS NOT NULL THEN N'[bio_cng_bio_gas]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'bio_diesel_b100') IS NOT NULL THEN N'[bio_diesel_b100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'bio_methane') IS NOT NULL THEN N'[bio_methane]'
        ELSE N'NULL'
    END + N',
    [cng_only],
    [diesel],
    [diesel_hybrid],
    [di_methyl_ether],
    [dual_diesel_bio_cng],
    [dual_diesel_cng],
    [dual_diesel_lng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'electric_bov') IS NOT NULL THEN N'[electric_bov]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'ethanol_e100') IS NOT NULL THEN N'[ethanol_e100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'flex_fuel_bio_diesel') IS NOT NULL THEN N'[flex_fuel_bio_diesel]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'flex_fuel_ethanol') IS NOT NULL THEN N'[flex_fuel_ethanol]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'fuel_cell_hydrogen') IS NOT NULL THEN N'[fuel_cell_hydrogen]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'hcng') IS NOT NULL THEN N'[hcng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'hydrogen_ice') IS NOT NULL THEN N'[hydrogen_ice]'
        ELSE N'NULL'
    END + N',
    [lng],
    [lpg_only],
    [methanol],
    [not_applicable],
    [petrol],
    [petrol_cng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_e20') IS NOT NULL THEN N'[petrol_e20]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_e20_cng') IS NOT NULL THEN N'[petrol_e20_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_e20_hybrid') IS NOT NULL THEN N'[petrol_e20_hybrid]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_e20_hybrid_cng') IS NOT NULL THEN N'[petrol_e20_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_e20_lpg') IS NOT NULL THEN N'[petrol_e20_lpg]'
        ELSE N'NULL'
    END + N',
    [petrol_hybrid],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'petrol_hybrid_cng') IS NOT NULL THEN N'[petrol_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    [petrol_lpg],
    [petrol_methanol],
    [plug_in_hybrid_ev],
    [pure_ev],
    [solar],
    [strong_hybrid_ev],
    [total],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'inserted_at') IS NOT NULL THEN N'[inserted_at]'
        WHEN COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'insert_date') IS NOT NULL THEN N'[insert_date]'
        ELSE N'GETDATE()'
    END + N'
FROM dbo.fact_ev_data_by_rto_backup_20260619;';
EXEC sys.sp_executesql @rto_insert_sql;

DECLARE @state_insert_sql NVARCHAR(MAX) = N'
INSERT INTO dbo.fact_ev_data_by_state (
    [year], [month], [day], [date], [state], [vehicle_type], [vehicle_category],
    [vehicle_use_type], [vehicle_class], [bio_cng_bio_gas], [bio_diesel_b100],
    [bio_methane], [cng_only], [diesel], [diesel_hybrid], [di_methyl_ether],
    [dual_diesel_bio_cng], [dual_diesel_cng], [dual_diesel_lng], [electric_bov],
    [ethanol_e100], [flex_fuel_bio_diesel], [flex_fuel_ethanol], [fuel_cell_hydrogen],
    [hcng], [hydrogen_ice], [lng], [lpg_only], [methanol], [not_applicable],
    [petrol], [petrol_cng], [petrol_e20], [petrol_e20_cng], [petrol_e20_hybrid],
    [petrol_e20_hybrid_cng], [petrol_e20_lpg], [petrol_hybrid], [petrol_hybrid_cng],
    [petrol_lpg], [petrol_methanol], [plug_in_hybrid_ev], [pure_ev], [solar],
    [strong_hybrid_ev], [total], [inserted_at]
)
SELECT
    [year], [month], [day], [date], [state], [vehicle_type], [vehicle_category],
    [vehicle_use_type], [vehicle_class],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'bio_cng_bio_gas') IS NOT NULL THEN N'[bio_cng_bio_gas]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'bio_diesel_b100') IS NOT NULL THEN N'[bio_diesel_b100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'bio_methane') IS NOT NULL THEN N'[bio_methane]'
        ELSE N'NULL'
    END + N',
    [cng_only],
    [diesel],
    [diesel_hybrid],
    [di_methyl_ether],
    [dual_diesel_bio_cng],
    [dual_diesel_cng],
    [dual_diesel_lng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'electric_bov') IS NOT NULL THEN N'[electric_bov]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'ethanol_e100') IS NOT NULL THEN N'[ethanol_e100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'flex_fuel_bio_diesel') IS NOT NULL THEN N'[flex_fuel_bio_diesel]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'flex_fuel_ethanol') IS NOT NULL THEN N'[flex_fuel_ethanol]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'fuel_cell_hydrogen') IS NOT NULL THEN N'[fuel_cell_hydrogen]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'hcng') IS NOT NULL THEN N'[hcng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'hydrogen_ice') IS NOT NULL THEN N'[hydrogen_ice]'
        ELSE N'NULL'
    END + N',
    [lng],
    [lpg_only],
    [methanol],
    [not_applicable],
    [petrol],
    [petrol_cng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_e20') IS NOT NULL THEN N'[petrol_e20]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_e20_cng') IS NOT NULL THEN N'[petrol_e20_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_e20_hybrid') IS NOT NULL THEN N'[petrol_e20_hybrid]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_e20_hybrid_cng') IS NOT NULL THEN N'[petrol_e20_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_e20_lpg') IS NOT NULL THEN N'[petrol_e20_lpg]'
        ELSE N'NULL'
    END + N',
    [petrol_hybrid],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'petrol_hybrid_cng') IS NOT NULL THEN N'[petrol_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    [petrol_lpg],
    [petrol_methanol],
    [plug_in_hybrid_ev],
    [pure_ev],
    [solar],
    [strong_hybrid_ev],
    [total],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'inserted_at') IS NOT NULL THEN N'[inserted_at]'
        WHEN COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'insert_date') IS NOT NULL THEN N'[insert_date]'
        ELSE N'GETDATE()'
    END + N'
FROM dbo.fact_ev_data_by_state_backup_20260619;';
EXEC sys.sp_executesql @state_insert_sql;

DECLARE @oem_insert_sql NVARCHAR(MAX) = N'
INSERT INTO dbo.fact_oem_data_by_state_and_category (
    [year], [month], [day], [date], [state], [vehicle_class], [vehicle_type],
    [vehicle_category], [vehicle_use_type], [maker], [bio_cng_bio_gas],
    [bio_diesel_b100], [bio_methane], [cng_only], [diesel], [diesel_hybrid],
    [di_methyl_ether], [dual_diesel_bio_cng], [dual_diesel_cng], [dual_diesel_lng],
    [electric_bov], [ethanol_e100], [flex_fuel_bio_diesel], [flex_fuel_ethanol],
    [fuel_cell_hydrogen], [hcng], [hydrogen_ice], [lng], [lpg_only], [methanol],
    [not_applicable], [petrol], [petrol_cng], [petrol_e20], [petrol_e20_cng],
    [petrol_e20_hybrid], [petrol_e20_hybrid_cng], [petrol_e20_lpg], [petrol_hybrid],
    [petrol_hybrid_cng], [petrol_lpg], [petrol_methanol], [plug_in_hybrid_ev],
    [pure_ev], [solar], [strong_hybrid_ev], [total], [inserted_at]
)
SELECT
    [year], [month], [day], [date], [state], [vehicle_class], [vehicle_type],
    [vehicle_category], [vehicle_use_type], [maker],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'bio_cng_bio_gas') IS NOT NULL THEN N'[bio_cng_bio_gas]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'bio_diesel_b100') IS NOT NULL THEN N'[bio_diesel_b100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'bio_methane') IS NOT NULL THEN N'[bio_methane]'
        ELSE N'NULL'
    END + N',
    [cng_only],
    [diesel],
    [diesel_hybrid],
    [di_methyl_ether],
    [dual_diesel_bio_cng],
    [dual_diesel_cng],
    [dual_diesel_lng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'electric_bov') IS NOT NULL THEN N'[electric_bov]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'ethanol_e100') IS NOT NULL THEN N'[ethanol_e100]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'flex_fuel_bio_diesel') IS NOT NULL THEN N'[flex_fuel_bio_diesel]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'flex_fuel_ethanol') IS NOT NULL THEN N'[flex_fuel_ethanol]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'fuel_cell_hydrogen') IS NOT NULL THEN N'[fuel_cell_hydrogen]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'hcng') IS NOT NULL THEN N'[hcng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'hydrogen_ice') IS NOT NULL THEN N'[hydrogen_ice]'
        ELSE N'NULL'
    END + N',
    [lng],
    [lpg_only],
    [methanol],
    [not_applicable],
    [petrol],
    [petrol_cng],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_e20') IS NOT NULL THEN N'[petrol_e20]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_e20_cng') IS NOT NULL THEN N'[petrol_e20_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_e20_hybrid') IS NOT NULL THEN N'[petrol_e20_hybrid]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_e20_hybrid_cng') IS NOT NULL THEN N'[petrol_e20_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_e20_lpg') IS NOT NULL THEN N'[petrol_e20_lpg]'
        ELSE N'NULL'
    END + N',
    [petrol_hybrid],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'petrol_hybrid_cng') IS NOT NULL THEN N'[petrol_hybrid_cng]'
        ELSE N'NULL'
    END + N',
    [petrol_lpg],
    [petrol_methanol],
    [plug_in_hybrid_ev],
    [pure_ev],
    [solar],
    [strong_hybrid_ev],
    [total],
    ' + CASE
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'inserted_at') IS NOT NULL THEN N'[inserted_at]'
        WHEN COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'insert_date') IS NOT NULL THEN N'[insert_date]'
        ELSE N'GETDATE()'
    END + N'
FROM dbo.fact_oem_data_by_state_and_category_backup_20260619;';
EXEC sys.sp_executesql @oem_insert_sql;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;

SELECT 'dbo.fact_ev_data_by_rto_backup_20260619' AS table_name, COUNT(*) AS row_count
FROM dbo.fact_ev_data_by_rto_backup_20260619
UNION ALL
SELECT 'dbo.fact_ev_data_by_rto', COUNT(*)
FROM dbo.fact_ev_data_by_rto
UNION ALL
SELECT 'dbo.fact_ev_data_by_state_backup_20260619', COUNT(*)
FROM dbo.fact_ev_data_by_state_backup_20260619
UNION ALL
SELECT 'dbo.fact_ev_data_by_state', COUNT(*)
FROM dbo.fact_ev_data_by_state
UNION ALL
SELECT 'dbo.fact_oem_data_by_state_and_category_backup_20260619', COUNT(*)
FROM dbo.fact_oem_data_by_state_and_category_backup_20260619
UNION ALL
SELECT 'dbo.fact_oem_data_by_state_and_category', COUNT(*)
FROM dbo.fact_oem_data_by_state_and_category;
