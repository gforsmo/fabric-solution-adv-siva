CREATE TABLE [instructions].[sm_validation] (
    [sm_val_instr_id] INT           IDENTITY (1, 1) NOT NULL,
    [dataset_name]    VARCHAR (200) NOT NULL,
    [expectation_id]  INT           NOT NULL,
    [check_params]    JSON          NULL,
    [severity]        VARCHAR (20)  DEFAULT ('error') NOT NULL,
    [is_active]       BIT           DEFAULT ((1)) NOT NULL,
    [log_function_id] INT           NULL,
    [created_date]    DATETIME2 (7) DEFAULT (getdate()) NULL,
    [modified_date]   DATETIME2 (7) DEFAULT (getdate()) NULL,
    [pipeline_name]   VARCHAR (100) NULL,
    [notebook_name]   VARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([sm_val_instr_id] ASC)
);


GO

