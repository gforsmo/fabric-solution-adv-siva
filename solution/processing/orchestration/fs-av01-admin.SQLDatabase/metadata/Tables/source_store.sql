CREATE TABLE [metadata].[source_store] (
    [source_id]        INT           NOT NULL,
    [source_name]      VARCHAR (100) NOT NULL,
    [source_type]      VARCHAR (50)  NOT NULL,
    [auth_method]      VARCHAR (50)  NOT NULL,
    [key_vault_url]    VARCHAR (500) NULL,
    [secret_name]      VARCHAR (200) NULL,
    [base_url]         VARCHAR (500) NOT NULL,
    [description]      VARCHAR (500) NULL,
    [run_mode]         VARCHAR (30)  DEFAULT ('auto') NOT NULL,
    [created_date]     DATETIME      DEFAULT (getdate()) NOT NULL,
    [modified_date]    DATETIME      DEFAULT (getdate()) NOT NULL,
    [handler_function] VARCHAR (100) NOT NULL,
    PRIMARY KEY CLUSTERED ([source_id] ASC)
);


GO

