-- ============================================
-- 03_create_indexes.sql
-- Create indexes for performance optimization
-- on all metadata, instructions and log tables.
--
-- Run order: Third - after 02_create_schemas_and_tables.sql
-- Can be run multiple times safely (idempotent)
--
-- Indexes created:
--   metadata.column_mappings     - IX_column_mappings_id
--   instructions.ingestion       - IX_ingestion_source
--   instructions.loading         - IX_loading_active
--   instructions.transformations - IX_transformations_active
--   instructions.validations     - IX_validations_table
--   log.pipeline_runs            - IX_pipeline_runs_status
--   log.validation_results       - IX_validation_results_run
--   log.metadata_changes         - IX_metadata_changes_table
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_column_mappings_id' AND object_id = OBJECT_ID('metadata.column_mappings'))
    CREATE INDEX IX_column_mappings_id ON metadata.column_mappings(mapping_id);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ingestion_source' AND object_id = OBJECT_ID('instructions.ingestion'))
    CREATE INDEX IX_ingestion_source ON instructions.ingestion(source_id, is_active);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_loading_active' AND object_id = OBJECT_ID('instructions.loading'))
    CREATE INDEX IX_loading_active ON instructions.loading(is_active);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_transformations_active' AND object_id = OBJECT_ID('instructions.transformations'))
    CREATE INDEX IX_transformations_active ON instructions.transformations(is_active);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_validations_table' AND object_id = OBJECT_ID('instructions.validations'))
    CREATE INDEX IX_validations_table ON instructions.validations(target_table, is_active);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pipeline_runs_status' AND object_id = OBJECT_ID('log.pipeline_runs'))
    CREATE INDEX IX_pipeline_runs_status ON log.pipeline_runs(status, started_at);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_validation_results_run' AND object_id = OBJECT_ID('log.validation_results'))
    CREATE INDEX IX_validation_results_run ON log.validation_results(run_id);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_metadata_changes_table' AND object_id = OBJECT_ID('log.metadata_changes'))
    CREATE INDEX IX_metadata_changes_table ON log.metadata_changes(table_name, changed_at);
GO