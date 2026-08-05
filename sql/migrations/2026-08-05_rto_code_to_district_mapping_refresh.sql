SET NOCOUNT ON;
SET XACT_ABORT ON;

IF OBJECT_ID('dbo.rto_code_to_district_mapping_backup_20260805', 'U') IS NOT NULL
BEGIN
    RAISERROR('Backup table dbo.rto_code_to_district_mapping_backup_20260805 already exists. Aborting to avoid overwriting a backup.', 16, 1);
    RETURN;
END;

SELECT *
INTO dbo.rto_code_to_district_mapping_backup_20260805
FROM dbo.rto_code_to_district_mapping;

BEGIN TRY
    BEGIN TRANSACTION;

    DROP TABLE dbo.rto_code_to_district_mapping;

    CREATE TABLE dbo.rto_code_to_district_mapping (
        [rto_code] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        [district] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
    );

    -- Row data is loaded separately from rto_code_to_district_mapping_refresh_20260805.csv
    -- (see docs/runbooks/rto_district_mapping_refresh.md), not inline here, because it
    -- originates from an external spreadsheet rather than a repeatable SQL transform.

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;

SELECT 'dbo.rto_code_to_district_mapping_backup_20260805' AS table_name, COUNT(*) AS row_count
FROM dbo.rto_code_to_district_mapping_backup_20260805
UNION ALL
SELECT 'dbo.rto_code_to_district_mapping', COUNT(*)
FROM dbo.rto_code_to_district_mapping;
