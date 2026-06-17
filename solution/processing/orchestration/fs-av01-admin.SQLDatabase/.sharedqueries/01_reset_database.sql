-- ============================================
-- 01_reset_database.sql
-- Full drop and recreate of all schemas, tables,
-- and security settings.
--
-- CI/CD-safe: no hardcoded users, dbo ownership
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
-- ============================================

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
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_validation_run')
    ALTER TABLE [log].[validation_results] DROP CONSTRAINT [FK_validation_run]
GO

-- Steg 3: Dropp default constraints dynamisk
DECLARE @sql NVARCHAR(MAX) = ''
SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + dc.name + '];' + CHAR(13)
FROM sys.default_constraints dc
JOIN sys.tables t ON dc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('log', 'metadata', 'instructions')
IF LEN(@sql) > 0
    EXEC sp_executesql @sql
GO

-- Steg 4: Dropp tabeller i riktig rekkefølge
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

IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'sm_validation')
    DROP TABLE [instructions].[sm_validation]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'semantic_model')
    DROP TABLE [instructions].[semantic_model]
GO
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'validations')
    DROP TABLE [instructions].[validations]
GO
-- Semantic model metadata tables
IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'sm_expectation_store')
    DROP TABLE [metadata].[sm_expectation_store]
GO

IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'sm_store')
    DROP TABLE [metadata].[sm_store]
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

-- Steg 5: Dropp skjemaer
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    DROP SCHEMA [instructions]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    DROP SCHEMA [metadata]
GO
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    DROP SCHEMA [log]
GO


-- ============================================
-- CREATE SEKVENS
-- CI/CD-safe: alle schemas eies av dbo
-- Ingen hardkodede brukere
-- ============================================

-- Steg 6: Opprett skjemaer med dbo som owner
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA [metadata] AUTHORIZATION [dbo]')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    EXEC('CREATE SCHEMA [instructions] AUTHORIZATION [dbo]')
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA [log] AUTHORIZATION [dbo]')
GO

-- Steg 7: Kjør neste scripts i rekkefølge
-- 02_create_schemas_and_tables.sql
-- 03_create_indexes.sql
-- 04_create_triggers.sql
-- 05_create_users_and_security.sql
-- 06_migrate_add_handler_and_pipeline_columns.sql
-- 07_seed_initial_metadata.sql

-- ============================================
-- Re-create SPN user after reset
-- NOTE: CREATE USER FROM EXTERNAL PROVIDER is not supported
-- in Fabric SQL Database without Managed Identity.
-- Must be done manually once in Fabric UI:
--   CREATE USER [sp-av-github] FROM EXTERNAL PROVIDER
-- ============================================

-- IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'sp-av-github')
--     CREATE USER [sp-av-github] FROM EXTERNAL PROVIDER
-- GO

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'sp-av-github')
BEGIN
    IF NOT EXISTS (
        SELECT * FROM sys.database_role_members rm
        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
        WHERE r.name = 'db_owner' AND m.name = 'sp-av-github'
    )
        ALTER ROLE db_owner ADD MEMBER [sp-av-github]
END
GO
