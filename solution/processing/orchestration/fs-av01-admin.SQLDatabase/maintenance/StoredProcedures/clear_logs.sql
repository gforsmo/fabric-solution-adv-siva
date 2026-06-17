CREATE   PROCEDURE maintenance.clear_logs AS
BEGIN
    DELETE FROM log.validation_results
    DELETE FROM log.pipeline_runs
    SELECT 'done' AS status
END

GO

