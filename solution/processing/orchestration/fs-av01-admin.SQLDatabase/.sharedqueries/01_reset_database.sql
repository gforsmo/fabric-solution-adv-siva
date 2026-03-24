-- ============================================
-- 01_reset_database.sql
-- Full drop and recreate of all schemas, tables,
-- users and security settings.
-- 
-- Run order: First - before all other scripts
-- Can be run multiple times safely (idempotent)
--
-- WARNING: This script drops ALL data!
-- Ensure you have a backup before running.
--
-- After running this script, execute in order:
--   02_create_schemas_and_tables.sql
--   03_create_indexes.sql
--   04_create_triggers.sql
--   05_create_users_and_security.sql
--   06_migrate_add_handler_and_pipeline_columns.sql
--   07_seed_initial_metadata.sql
-- ============================================

-- ============================================
-- DROP SEKVENS

-- Steg 1: Dropp triggers
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_ingestion_audit')
    DROP TRIGGER [instructions].[trg_ingestion_audit]
GO
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_loading_audit')
    DROP TRIGGER [instructions].[trg_loading_audit]
GO
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_transformations_audit')
    DROP TRIGGER [instructions].[trg_transformations_audit]
GO
IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_validations_audit')
    DROP TRIGGER [instructions].[trg_validations_audit]
GO

-- Steg 2: Dropp foreign key constraints
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_ingestion_log')
    ALTER TABLE [instructions].[ingestion] DROP CONSTRAINT [FK_ingestion_log]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_ingestion_source')
    ALTER TABLE [instructions].[ingestion] DROP CONSTRAINT [FK_ingestion_source]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_loading_log')
    ALTER TABLE [instructions].[loading] DROP CONSTRAINT [FK_loading_log]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_loading_store')
    ALTER TABLE [instructions].[loading] DROP CONSTRAINT [FK_loading_store]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_transform_log')
    ALTER TABLE [instructions].[transformations] DROP CONSTRAINT [FK_transform_log]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_validation_expectation')
    ALTER TABLE [instructions].[validations] DROP CONSTRAINT [FK_validation_expectation]
GO
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_validation_log')
    ALTER TABLE [instructions].[validations] DROP CONSTRAINT [FK_validation_log]
GO

-- Steg 3: Dropp tabeller
-- Steg 2b: Dropp foreign key i log
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_validation_run')
    ALTER TABLE [log].[validation_results] DROP CONSTRAINT [FK_validation_run]
GO

-- Steg 3: Dropp tabeller i riktig rekkefølge
-- log tabeller først (har FK til pipeline_runs)
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'validation_results')
    DROP TABLE [log].[validation_results]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'pipeline_runs')
    DROP TABLE [log].[pipeline_runs]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'metadata_changes')
    DROP TABLE [log].[metadata_changes]
GO




IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'ingestion')
    DROP TABLE [instructions].[ingestion]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'loading')
    DROP TABLE [instructions].[loading]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'transformations')
    DROP TABLE [instructions].[transformations]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'validations')
    DROP TABLE [instructions].[validations]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'source_store')
    DROP TABLE [metadata].[source_store]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'loading_store')
    DROP TABLE [metadata].[loading_store]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'transform_store')
    DROP TABLE [metadata].[transform_store]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'expectation_store')
    DROP TABLE [metadata].[expectation_store]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'column_mappings')
    DROP TABLE [metadata].[column_mappings]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'log_store')
    DROP TABLE [metadata].[log_store]
GO


-- Steg 4: Fjern roller fra bruker
IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'geir.forsmo.atea@siva.no')
BEGIN
    ALTER ROLE [db_datareader] DROP MEMBER [geir.forsmo.atea@siva.no]
    ALTER ROLE [db_datawriter] DROP MEMBER [geir.forsmo.atea@siva.no]
END
GO

-- Steg 5: Dropp skjemaer
-- Dropp default constraints på log-skjema
DECLARE @sql NVARCHAR(MAX) = ''
SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + dc.name + '];' + CHAR(13)
FROM sys.default_constraints dc
JOIN sys.tables t ON dc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('log', 'metadata', 'instructions')

EXEC sp_executesql @sql
GO


IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    DROP SCHEMA [instructions]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    DROP SCHEMA [metadata]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    DROP SCHEMA [log]
GO

-- Steg 6: Dropp bruker
-- Endre eierskap på skjemaer til dbo
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    ALTER AUTHORIZATION ON SCHEMA::[instructions] TO [dbo]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    ALTER AUTHORIZATION ON SCHEMA::[metadata] TO [dbo]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    ALTER AUTHORIZATION ON SCHEMA::[log] TO [dbo]
GO

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'geir.forsmo.atea@siva.no')
    DROP USER [geir.forsmo.atea@siva.no]
GO

-- ============================================
-- CREATE SEKVENS
-- ============================================

-- Steg 7: Opprett bruker
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'geir.forsmo.atea@siva.no')
    CREATE USER [geir.forsmo.atea@siva.no] FROM EXTERNAL PROVIDER
GO

-- Steg 8: Opprett skjemaer
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA [metadata] AUTHORIZATION [geir.forsmo.atea@siva.no]')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    EXEC('CREATE SCHEMA [instructions] AUTHORIZATION [geir.forsmo.atea@siva.no]')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA [log] AUTHORIZATION [geir.forsmo.atea@siva.no]')
GO

-- Steg 9: Tildel roller
IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'geir.forsmo.atea@siva.no')
BEGIN
    ALTER ROLE [db_datareader] ADD MEMBER [geir.forsmo.atea@siva.no]
    ALTER ROLE [db_datawriter] ADD MEMBER [geir.forsmo.atea@siva.no]
END
GO

-- Steg 10: Kjør create_schemas_and_tables.sql
-- Steg 11: Kjør create_indexes.sql  
-- Steg 12: Kjør create_triggers.sql
-- Steg 13: Kjør nb-av01-init-sql-database