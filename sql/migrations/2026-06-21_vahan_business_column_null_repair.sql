SET NOCOUNT ON
SET XACT_ABORT ON

/*
Use this repair only before any new post-migration raw loads have been ingested.
It nulls whole business columns when the historical backup did not contain the
exact newer column and the prior migration incorrectly carried legacy values
forward into the new semantic field.
*/

IF OBJECT_ID('dbo.fact_ev_data_by_rto_backup_20260619', 'U') IS NULL
BEGIN
    RAISERROR('Required backup table dbo.fact_ev_data_by_rto_backup_20260619 does not exist. Aborting.', 16, 1)
    RETURN
END

IF OBJECT_ID('dbo.fact_ev_data_by_state_backup_20260619', 'U') IS NULL
BEGIN
    RAISERROR('Required backup table dbo.fact_ev_data_by_state_backup_20260619 does not exist. Aborting.', 16, 1)
    RETURN
END

IF OBJECT_ID('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'U') IS NULL
BEGIN
    RAISERROR('Required backup table dbo.fact_oem_data_by_state_and_category_backup_20260619 does not exist. Aborting.', 16, 1)
    RETURN
END

BEGIN TRY
    BEGIN TRANSACTION

    IF COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'electric_bov') IS NULL
       AND COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_ev_data_by_rto
        SET electric_bov = NULL
    END

    IF COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'ethanol_e100') IS NULL
       AND COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'ethanol') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_ev_data_by_rto
        SET ethanol_e100 = NULL
    END

    IF COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'electric_bov') IS NULL
       AND COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_ev_data_by_state
        SET electric_bov = NULL
    END

    IF COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'ethanol_e100') IS NULL
       AND COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'ethanol') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_ev_data_by_state
        SET ethanol_e100 = NULL
    END

    IF COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'electric_bov') IS NULL
       AND COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_oem_data_by_state_and_category
        SET electric_bov = NULL
    END

    IF COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'ethanol_e100') IS NULL
       AND COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'ethanol') IS NOT NULL
    BEGIN
        UPDATE dbo.fact_oem_data_by_state_and_category
        SET ethanol_e100 = NULL
    END

    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION

    DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE()
    DECLARE @error_severity INT = ERROR_SEVERITY()
    DECLARE @error_state INT = ERROR_STATE()

    RAISERROR(@error_message, @error_severity, @error_state)
END CATCH

SELECT 'dbo.fact_ev_data_by_rto' AS table_name, COUNT(*) AS non_null_rows
FROM dbo.fact_ev_data_by_rto
WHERE electric_bov IS NOT NULL OR ethanol_e100 IS NOT NULL

SELECT 'dbo.fact_ev_data_by_state' AS table_name, COUNT(*) AS non_null_rows
FROM dbo.fact_ev_data_by_state
WHERE electric_bov IS NOT NULL OR ethanol_e100 IS NOT NULL

SELECT 'dbo.fact_oem_data_by_state_and_category' AS table_name, COUNT(*) AS non_null_rows
FROM dbo.fact_oem_data_by_state_and_category
WHERE electric_bov IS NOT NULL OR ethanol_e100 IS NOT NULL
