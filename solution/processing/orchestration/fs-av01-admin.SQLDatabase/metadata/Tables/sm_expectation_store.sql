CREATE TABLE [metadata].[sm_expectation_store] (
    [expectation_id]   INT           NOT NULL,
    [expectation_name] VARCHAR (100) NOT NULL,
    [check_function]   VARCHAR (100) NOT NULL,
    [description]      VARCHAR (500) NULL,
    [expected_params]  VARCHAR (MAX) NULL
);


GO

