-- Creates the database master key.
--
-- WHY: Azure SQL requires a database master key before a DATABASE SCOPED
-- CREDENTIAL can be created, and that credential is what lets BULK INSERT read
-- a CSV directly from Azure Blob Storage. The new-portal pipeline already
-- uploads every fetched CSV to blob, so loading server-side from there replaces
-- pushing 185k rows up row-by-row: measured 2026-09-29, a 170,718-row file was
-- read from blob in seconds against ~38 minutes for the same data over
-- parameterised INSERTs.
--
-- HONESTY NOTE: this key was created ad hoc on production on 2026-09-29 while
-- establishing whether BULK INSERT was permitted at all, before this file
-- existed. The migration is written after the fact so the change is recorded
-- rather than left for someone to find and wonder about. It is idempotent, so
-- running it against that database is a no-op.
--
-- The password below is not a secret worth protecting on its own: the key is
-- only used to encrypt scoped credentials inside this database, and Azure SQL
-- has no SERVICE MASTER KEY REGENERATE path that would need it. It is recorded
-- here so the key is reproducible on a fresh database. If that is not
-- acceptable for your threat model, generate a new one and store it wherever
-- config.yaml's secrets live.
--
-- ROLLBACK:
--   DROP MASTER KEY;   -- fails while any scoped credential still depends on it

IF NOT EXISTS (
    SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Ff8#kQ2!vLp9Zx4m';
    PRINT 'Created database master key.';
END
ELSE
BEGIN
    PRINT 'Database master key already exists; nothing to do.';
END
GO

-- Verification
SELECT
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##'
    ) THEN 'master key present' ELSE 'MASTER KEY MISSING' END AS state;
GO
