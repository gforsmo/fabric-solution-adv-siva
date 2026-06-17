# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-5-sm-refresh
# **Purpose**: Trigger and poll Import Mode or Direct Lake refresh of semantic model in Fabric.
# 
# **Stage**: Semantic Model refresh
# 
# **Dependencies**: nb-av01-generic-functions
# 
# **Metadata**: instructions.semantic_model, metadata.sm_store

# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

SM_WORKSPACE_ID = get_workspace_id(variables.SM_WORKSPACE_NAME)
GOLD_BASE_PATH  = construct_abfs_path(
    variables.LH_WORKSPACE_NAME,
    variables.GOLD_LH_NAME,
    area="Tables"
)

print(f"SM workspace name : {variables.SM_WORKSPACE_NAME}")
print(f"SM workspace ID   : {SM_WORKSPACE_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Metadata

# CELL ********************

set_metadata_db_url(
    server   = variables.METADATA_SERVER,
    database = variables.METADATA_DB
)

sm_store        = load_sm_store(spark)
log_lookup      = load_log_store(spark)
sm_instructions = get_active_instructions(spark, "semantic_model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sm_instructions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute SM Refresh
# Expected fields in `instructions.semantic_model`:
# - `dataset_name` (str, required): Semantic model name in Fabric
# - `sm_mode` (str, required): 'import' | 'directlake'
# - `refresh_params` (JSON, required):
#   - `refresh_mode`: 'always_full' | 'always_automatic' | 'scheduled'
#   - `full_refresh_weekday`: 0=Monday ... 6=Sunday
#   - `notify_option`: 'NoNotification' | 'MailOnFailure' | 'MailOnCompletion'
#   - `poll_timeout_seconds`: max wait time in seconds
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

first_instr   = sm_instructions[0] if sm_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-5-sm-refresh")

'''
def sm_refresh_executor(spark, instr):
    """Thin executor - core logic in run_sm_refresh() in generic."""
    return run_sm_refresh(
        spark          = spark,
        instr          = instr,
        workspace_id   = SM_WORKSPACE_ID,
        gold_base_path = GOLD_BASE_PATH,
        pipeline_name  = PIPELINE_NAME,
        workspace_name = variables.SM_WORKSPACE_NAME
    )
'''

def sm_refresh_executor(spark, instr):
    dataset_name = instr["dataset_name"]
    sm_mode      = instr.get("sm_mode", "import")
    params       = json.loads(instr.get("refresh_params") or "{}")

    notify_option        = params.get("notify_option",        "NoNotification")
    timeout              = params.get("poll_timeout_seconds", 600)
    full_refresh_weekday = params.get("full_refresh_weekday", 0)
    refresh_mode         = params.get("refresh_mode",         "scheduled")

    if sm_mode == "directlake":
        print(f"  Direct Lake – no refresh needed: {dataset_name}")
        return (1, dataset_name, f"mode={sm_mode}")

    # Bestem refresh type
    if refresh_mode == "always_full":
        refresh_type = "full"
    elif refresh_mode == "always_automatic":
        refresh_type = "automatic"
    elif refresh_mode == "scheduled":
        refresh_type = determine_refresh_type(
            workspace_id         = SM_WORKSPACE_ID,
            dataset_name         = dataset_name,
            full_refresh_weekday = full_refresh_weekday
        )
    else:
        raise ValueError(f"Unknown refresh_mode: '{refresh_mode}'")

    print(f"  Dataset name  : {dataset_name}")
    print(f"  Refresh mode  : {refresh_mode}")
    print(f"  Refresh type  : {refresh_type}")
    print(f"  Notify option : {notify_option}")

    refresh_semantic_model(
        workspace_id  = SM_WORKSPACE_ID,
        dataset_name  = dataset_name,
        notify_option = notify_option,
        refresh_type  = refresh_type,
        timeout       = timeout
    )

    last = get_last_refresh_status(SM_WORKSPACE_ID, dataset_name)

    spark.createDataFrame([{
        "dataset_name":   dataset_name,
        "refresh_type":   last.get("refreshType", refresh_type),
        "refresh_mode":   refresh_mode,
        "status":         last.get("status"),
        "started_at":     last.get("startTime"),
        "completed_at":   last.get("endTime"),
        "request_id":     last.get("requestId"),
        "pipeline_name":  PIPELINE_NAME,
        "workspace_name": variables.SM_WORKSPACE_NAME
    }]).write.format("delta").mode("overwrite") \
    .save(GOLD_BASE_PATH + "siva/refresh_metadata")

    return (1, dataset_name, f"| mode={refresh_mode}")


execute_pipeline_stage(
    spark          = spark,
    instructions   = sm_instructions,
    stage_executor = sm_refresh_executor,
    notebook_name  = NOTEBOOK_NAME,
    pipeline_name  = PIPELINE_NAME,
    action_type    = ACTION_SM_REFRESH,
    log_lookup     = log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
