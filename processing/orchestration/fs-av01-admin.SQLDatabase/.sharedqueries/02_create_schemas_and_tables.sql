-- ============================================
-- 02_create_schemas_and_tables.sql
-- Create all schemas and tables for the
-- metadata and instructions database.
--
-- Run order: Second - after 01_reset_database.sql
-- Can be run multiple times safely (idempotent)
--
-- Schemas created:
--   metadata    - Source registry, function catalogs
--   instructions - Pipeline runtime instructions
--   log         - Execution history and audit trail
--
-- Tables created:
--   metadata.source_store
--   metadata.loading_store
--   metadata.transform_store
--   metadata.expectation_store
--   metadata.log_store
--   metadata.column_mappings
--   instructions.ingestion
--   instructions.loading
--   instructions.transformations
--   instructions.validations
--   log.pipeline_runs
--   log.validation_results
--   log.metadata_changes
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA metadata');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    EXEC('CREATE SCHEMA instructions');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA log');
GO

-- ============================================
-- metadata tables
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'source_store')
CREATE TABLE metadata.source_store (
    source_id           INT PRIMARY KEY,
    source_name         VARCHAR(100) NOT NULL,
    source_type         VARCHAR(50) NOT NULL,
    auth_method         VARCHAR(50),
    key_vault_url       VARCHAR(500),
    secret_name         VARCHAR(100),
    base_url            VARCHAR(500),
    description         VARCHAR(1000),
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE()
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'loading_store')
CREATE TABLE metadata.loading_store (
    loading_id          INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,
    description         VARCHAR(1000),
    expected_params     JSON
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'transform_store')
CREATE TABLE metadata.transform_store (
    transform_id        INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,
    description         VARCHAR(1000),
    expected_params     JSON
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'expectation_store')
CREATE TABLE metadata.expectation_store (
    expectation_id      INT PRIMARY KEY,
    expectation_name    VARCHAR(100) NOT NULL,
    gx_method           VARCHAR(100) NOT NULL,
    description         VARCHAR(1000),
    expected_params     JSON
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'log_store')
CREATE TABLE metadata.log_store (
    log_id              INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,
    description         VARCHAR(1000),
    expected_params     JSON
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'metadata' AND t.name = 'column_mappings')
CREATE TABLE metadata.column_mappings (
    mapping_id          VARCHAR(100) NOT NULL,
    column_order        INT NOT NULL,
    source_column       VARCHAR(255) NOT NULL,
    target_column       VARCHAR(100) NOT NULL,
    data_type           VARCHAR(50) NOT NULL,
    description         VARCHAR(500),
    PRIMARY KEY (mapping_id, column_order)
);
GO

-- ============================================
-- instructions tables
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'ingestion')
CREATE TABLE instructions.ingestion (
    ingestion_id        INT PRIMARY KEY,
    source_id           INT NOT NULL,
    endpoint_path       VARCHAR(500),
    landing_path        VARCHAR(500) NOT NULL,
    request_params      JSON,
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_ingestion_source FOREIGN KEY (source_id)
        REFERENCES metadata.source_store(source_id),
    CONSTRAINT FK_ingestion_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'loading')
CREATE TABLE instructions.loading (
    loading_instr_id    INT PRIMARY KEY,
    loading_id          INT NOT NULL,
    source_path         VARCHAR(500) NOT NULL,
    source_layer        VARCHAR(20) NOT NULL DEFAULT 'raw',
    target_table        VARCHAR(200) NOT NULL,
    target_layer        VARCHAR(20) NOT NULL DEFAULT 'bronze',
    key_columns         JSON NOT NULL,
    load_params         JSON,
    merge_condition     VARCHAR(500),
    merge_type          VARCHAR(20) DEFAULT 'update_all',
    merge_columns       JSON,
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_loading_store FOREIGN KEY (loading_id)
        REFERENCES metadata.loading_store(loading_id),
    CONSTRAINT FK_loading_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'transformations')
CREATE TABLE instructions.transformations (
    transform_instr_id  INT PRIMARY KEY,
    source_table        VARCHAR(200) NOT NULL,
    source_layer        VARCHAR(20) NOT NULL,
    dest_table          VARCHAR(200) NOT NULL,
    dest_layer          VARCHAR(20) NOT NULL,
    transform_pipeline  JSON NOT NULL,
    transform_params    JSON,
    merge_condition     VARCHAR(500),
    merge_type          VARCHAR(20) DEFAULT 'update_all',
    merge_columns       JSON,
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_transform_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'instructions' AND t.name = 'validations')
CREATE TABLE instructions.validations (
    validation_instr_id INT PRIMARY KEY,
    target_table        VARCHAR(200) NOT NULL,
    target_layer        VARCHAR(20) NOT NULL,
    expectation_id      INT NOT NULL,
    column_name         VARCHAR(100),
    validation_params   JSON,
    severity            VARCHAR(20) DEFAULT 'error',
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_validation_expectation FOREIGN KEY (expectation_id)
        REFERENCES metadata.expectation_store(expectation_id),
    CONSTRAINT FK_validation_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

-- ============================================
-- log tables
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'pipeline_runs')
CREATE TABLE log.pipeline_runs (
    run_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    started_at          DATETIME2 DEFAULT GETDATE(),
    completed_at        DATETIME2,
    status              VARCHAR(20) NOT NULL,
    records_processed   INT,
    error_message       NVARCHAR(MAX),
    action_type         VARCHAR(20),
    source_name         VARCHAR(100),
    instruction_detail  VARCHAR(500),
    notebook_name       VARCHAR(100)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'validation_results')
CREATE TABLE log.validation_results (
    result_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id              BIGINT,
    validation_instr_id INT,
    expectation_type    VARCHAR(100),
    column_name         VARCHAR(100),
    passed              BIT NOT NULL,
    observed_value      JSON,
    executed_at         DATETIME2 DEFAULT GETDATE(),
    lakehouse_name      VARCHAR(100),
    schema_name         VARCHAR(50),
    table_name          VARCHAR(100),
    CONSTRAINT FK_validation_run FOREIGN KEY (run_id)
        REFERENCES log.pipeline_runs(run_id)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'log' AND t.name = 'metadata_changes')
CREATE TABLE log.metadata_changes (
    change_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    table_name          VARCHAR(100) NOT NULL,
    record_id           INT NOT NULL,
    change_type         VARCHAR(20) NOT NULL,
    changed_by          VARCHAR(100) DEFAULT SYSTEM_USER,
    changed_at          DATETIME2 DEFAULT GETDATE(),
    old_values          JSON,
    new_values          JSON
);
GO