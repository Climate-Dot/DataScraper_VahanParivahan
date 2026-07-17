SET NOCOUNT ON
SET XACT_ABORT ON

/*
Backfill historical electric_bov values in the raw tables from the preserved
legacy electric_vehicles columns. This intentionally restores the older
business meaning in raw history so the existing curated dbt models can be
full-refreshed without extra fallback logic.

The script is intentionally idempotent and updates one raw snapshot date at a
time to avoid exhausting tempdb on large historical joins.
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

DECLARE @rto_rows_updated INT = 0
DECLARE @state_rows_updated INT = 0
DECLARE @oem_rows_updated INT = 0
DECLARE @sql NVARCHAR(MAX)

BEGIN TRY
    IF COL_LENGTH('dbo.fact_ev_data_by_rto_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        SET @sql = N'
        DECLARE @current_date NVARCHAR(50);
        DECLARE date_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT [date]
            FROM dbo.fact_ev_data_by_rto_backup_20260619
            WHERE NULLIF(LTRIM(RTRIM(electric_vehicles)), '''') IS NOT NULL;

        OPEN date_cursor;
        FETCH NEXT FROM date_cursor INTO @current_date;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            UPDATE target
            SET electric_bov = NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''')
            FROM dbo.fact_ev_data_by_rto AS target
            INNER JOIN dbo.fact_ev_data_by_rto_backup_20260619 AS legacy_backup
                ON target.[date] = legacy_backup.[date]
               AND target.state = legacy_backup.state
               AND target.rto_name = legacy_backup.rto_name
               AND target.rto_code = legacy_backup.rto_code
               AND target.vehicle_type = legacy_backup.vehicle_type
               AND target.vehicle_category = legacy_backup.vehicle_category
               AND target.vehicle_use_type = legacy_backup.vehicle_use_type
               AND target.vehicle_class = legacy_backup.vehicle_class
            WHERE target.[date] = @current_date
              AND target.electric_bov IS NULL
              AND NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''') IS NOT NULL;

            SET @rows_updated = @rows_updated + @@ROWCOUNT;
            FETCH NEXT FROM date_cursor INTO @current_date;
        END

        CLOSE date_cursor;
        DEALLOCATE date_cursor;'

        EXEC sys.sp_executesql
            @sql,
            N'@rows_updated INT OUTPUT',
            @rows_updated = @rto_rows_updated OUTPUT
    END

    IF COL_LENGTH('dbo.fact_ev_data_by_state_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        SET @sql = N'
        DECLARE @current_date NVARCHAR(50);
        DECLARE date_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT [date]
            FROM dbo.fact_ev_data_by_state_backup_20260619
            WHERE NULLIF(LTRIM(RTRIM(electric_vehicles)), '''') IS NOT NULL;

        OPEN date_cursor;
        FETCH NEXT FROM date_cursor INTO @current_date;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            UPDATE target
            SET electric_bov = NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''')
            FROM dbo.fact_ev_data_by_state AS target
            INNER JOIN dbo.fact_ev_data_by_state_backup_20260619 AS legacy_backup
                ON target.[date] = legacy_backup.[date]
               AND target.state = legacy_backup.state
               AND target.vehicle_type = legacy_backup.vehicle_type
               AND target.vehicle_category = legacy_backup.vehicle_category
               AND target.vehicle_use_type = legacy_backup.vehicle_use_type
               AND target.vehicle_class = legacy_backup.vehicle_class
            WHERE target.[date] = @current_date
              AND target.electric_bov IS NULL
              AND NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''') IS NOT NULL;

            SET @rows_updated = @rows_updated + @@ROWCOUNT;
            FETCH NEXT FROM date_cursor INTO @current_date;
        END

        CLOSE date_cursor;
        DEALLOCATE date_cursor;'

        EXEC sys.sp_executesql
            @sql,
            N'@rows_updated INT OUTPUT',
            @rows_updated = @state_rows_updated OUTPUT
    END

    IF COL_LENGTH('dbo.fact_oem_data_by_state_and_category_backup_20260619', 'electric_vehicles') IS NOT NULL
    BEGIN
        SET @sql = N'
        DECLARE @current_date NVARCHAR(50);
        DECLARE date_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT [date]
            FROM dbo.fact_oem_data_by_state_and_category_backup_20260619
            WHERE NULLIF(LTRIM(RTRIM(electric_vehicles)), '''') IS NOT NULL;

        OPEN date_cursor;
        FETCH NEXT FROM date_cursor INTO @current_date;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            UPDATE target
            SET electric_bov = NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''')
            FROM dbo.fact_oem_data_by_state_and_category AS target
            INNER JOIN dbo.fact_oem_data_by_state_and_category_backup_20260619 AS legacy_backup
                ON target.[date] = legacy_backup.[date]
               AND target.state = legacy_backup.state
               AND target.maker = legacy_backup.maker
               AND target.vehicle_type = legacy_backup.vehicle_type
               AND target.vehicle_category = legacy_backup.vehicle_category
               AND target.vehicle_use_type = legacy_backup.vehicle_use_type
               AND target.vehicle_class = legacy_backup.vehicle_class
            WHERE target.[date] = @current_date
              AND target.electric_bov IS NULL
              AND NULLIF(LTRIM(RTRIM(legacy_backup.electric_vehicles)), '''') IS NOT NULL;

            SET @rows_updated = @rows_updated + @@ROWCOUNT;
            FETCH NEXT FROM date_cursor INTO @current_date;
        END

        CLOSE date_cursor;
        DEALLOCATE date_cursor;'

        EXEC sys.sp_executesql
            @sql,
            N'@rows_updated INT OUTPUT',
            @rows_updated = @oem_rows_updated OUTPUT
    END
END TRY
BEGIN CATCH
    DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE()
    DECLARE @error_severity INT = ERROR_SEVERITY()
    DECLARE @error_state INT = ERROR_STATE()

    RAISERROR(@error_message, @error_severity, @error_state)
END CATCH

SELECT 'dbo.fact_ev_data_by_rto' AS table_name, @rto_rows_updated AS electric_bov_rows_updated
UNION ALL
SELECT 'dbo.fact_ev_data_by_state', @state_rows_updated
UNION ALL
SELECT 'dbo.fact_oem_data_by_state_and_category', @oem_rows_updated
