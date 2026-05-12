CREATE TABLE [instructions].[ingestion] (
    [ingestion_id]    INT            IDENTITY (1, 1) NOT NULL,
    [source_id]       INT            NOT NULL,
    [endpoint_path]   VARCHAR (500)  NOT NULL,
    [landing_path]    VARCHAR (500)  NOT NULL,
    [request_params]  NVARCHAR (MAX) NULL,
    [is_active]       BIT            DEFAULT ((1)) NOT NULL,
    [log_function_id] INT            NULL,
    [created_date]    DATETIME       DEFAULT (getdate()) NOT NULL,
    [modified_date]   DATETIME       DEFAULT (getdate()) NOT NULL,
    [pipeline_name]   VARCHAR (200)  DEFAULT ('data_pipeline') NOT NULL,
    [notebook_name]   VARCHAR (200)  DEFAULT ('nb-av01-0-ingest-api') NOT NULL,
    PRIMARY KEY CLUSTERED ([ingestion_id] ASC),
    CONSTRAINT [FK_ingestion_log] FOREIGN KEY ([log_function_id]) REFERENCES [metadata].[log_store] ([log_id]),
    CONSTRAINT [FK_ingestion_source] FOREIGN KEY ([source_id]) REFERENCES [metadata].[source_store] ([source_id])
);


GO

