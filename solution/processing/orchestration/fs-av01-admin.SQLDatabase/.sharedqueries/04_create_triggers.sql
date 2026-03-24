-- ============================================
-- 04_create_triggers.sql
-- Create audit triggers for all metadata and
-- instructions tables. All changes are logged
-- to log.metadata_changes.
--
-- Run order: Fourth - after 03_create_indexes.sql
-- Can be run multiple times safely (idempotent)
--
-- Triggers created:
--   metadata.source_store      - trg_source_store_audit
--   metadata.loading_store     - trg_loading_store_audit
--   metadata.transform_store   - trg_transform_store_audit
--   metadata.expectation_store - trg_expectation_store_audit
--   metadata.log_store         - trg_log_store_audit
--   metadata.column_mappings   - trg_column_mappings_audit
--   instructions.ingestion     - trg_ingestion_audit
--   instructions.loading       - trg_loading_audit
--   instructions.transformations - trg_transformations_audit
--   instructions.validations   - trg_validations_audit
-- ============================================

-- ============================================
-- Drop existing triggers
-- ============================================
DROP TRIGGER IF EXISTS [metadata].[trg_source_store_audit]
GO
DROP TRIGGER IF EXISTS [metadata].[trg_loading_store_audit]
GO
DROP TRIGGER IF EXISTS [metadata].[trg_transform_store_audit]
GO
DROP TRIGGER IF EXISTS [metadata].[trg_expectation_store_audit]
GO
DROP TRIGGER IF EXISTS [metadata].[trg_log_store_audit]
GO
DROP TRIGGER IF EXISTS [metadata].[trg_column_mappings_audit]
GO
DROP TRIGGER IF EXISTS [instructions].[trg_ingestion_audit]
GO
DROP TRIGGER IF EXISTS [instructions].[trg_loading_audit]
GO
DROP TRIGGER IF EXISTS [instructions].[trg_transformations_audit]
GO
DROP TRIGGER IF EXISTS [instructions].[trg_validations_audit]
GO

-- ============================================
-- Create triggers
-- ============================================

-- Trigger for metadata.source_store
CREATE TRIGGER [metadata].[trg_source_store_audit]
ON metadata.source_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.source_store', i.source_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.source_id = i.source_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.source_store', i.source_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.source_id = d.source_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.source_store', d.source_id, 'delete',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER), NULL
    FROM deleted d
    WHERE NOT EXISTS (SELECT 1 FROM inserted i WHERE i.source_id = d.source_id);
END;
GO

-- Trigger for metadata.loading_store
CREATE TRIGGER [metadata].[trg_loading_store_audit]
ON metadata.loading_store
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.loading_store', i.loading_id, 'insert', NULL,
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM deleted d WHERE d.loading_id = i.loading_id);

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new_values)
    SELECT 'metadata.loading_store', i.loading_id, 'update',
        (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
        (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    INNER JOIN deleted d ON i.loading_id = d.loading_id;

    INSERT INTO log.metadata_changes (table_name, record_id, change_type, old_values, new