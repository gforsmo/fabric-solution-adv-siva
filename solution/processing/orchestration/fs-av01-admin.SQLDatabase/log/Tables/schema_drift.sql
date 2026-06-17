CREATE TABLE [log].[schema_drift] (
    [drift_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [run_id]            BIGINT          NULL,
    [detected_at]       DATETIME2 (7)   NULL,
    [column_mapping_id] NVARCHAR (100)  NULL,
    [source_path]       NVARCHAR (500)  NULL,
    [file_name]         NVARCHAR (255)  NULL,
    [column_name]       NVARCHAR (255)  NULL,
    [drift_type]        NVARCHAR (50)   NULL,
    [error_code]        NVARCHAR (10)   NULL,
    [expected_value]    NVARCHAR (255)  NULL,
    [actual_value]      NVARCHAR (255)  NULL,
    [severity]          NVARCHAR (20)   NULL,
    [suggested_action]  NVARCHAR (1000) NULL,
    [resolved]          BIT             DEFAULT ((0)) NULL,
    PRIMARY KEY CLUSTERED ([drift_id] ASC)
);


GO

