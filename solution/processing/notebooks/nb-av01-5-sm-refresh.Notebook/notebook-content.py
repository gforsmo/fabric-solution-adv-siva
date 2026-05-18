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
# **Purpose**: Trigger og poll Import Mode eller Direct Lake refresh av semantisk modell i Fabric.
# 
# **Stage**: Semantic Model refresh
# 
# **Dependencies**: nb-av01-generic-functions
# 
# **Metadata**: instructions.semantic_model, metadata.sm_store
# 
# **Merk**:
# - All Fabric/sempy-logikk ligg i nb-av01-generic-functions
# - Notebooken har ingen eksterne importer
# - SM-workspace resolves via get_workspace_id() frå generic
# - Bytt modus (import/directlake) i instructions.semantic_model utan kodeendring

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

# Load workspace-specific variables from Variable Library
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SM_WORKSPACE_ID = get_workspace_id(variables.SM_WORKSPACE_NAME)
GOLD_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.GOLD_LH_NAME, area="Tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"SM workspace namn : {variables.SM_WORKSPACE_NAME}")
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

# MARKDOWN ********************

# ## Execute SM Refresh
# Expected fields i `instructions.semantic_model`:
# - `sm_mode` (str, required): 'import' | 'directlake' – styrer vilket SM-namn som brukast
# - `notify_option` (str, optional): 'NoNotification' | 'MailOnFailure' | 'MailOnCompletion'
# - `poll_timeout_seconds` (int, optional): Maks ventetid i sekunder (default 600)
# - `log_function_id` (int, required): Lookup key i metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging
# 


# CELL ********************

first_instr   = sm_instructions[0] if sm_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-5-sm-refresh")

def sm_refresh_executor(spark, instr):
    import json

    # Les alt frå instructions
    dataset_name = instr["dataset_name"]          # ← frå tabell, ikkje Variable Library
    sm_mode      = instr.get("sm_mode", "import")
    params       = json.loads(instr.get("refresh_params") or "{}")

    refresh_mode         = params.get("refresh_mode",         "scheduled")
    notify_option        = params.get("notify_option",        "NoNotification")
    timeout              = params.get("poll_timeout_seconds", 600)
    full_refresh_weekday = params.get("full_refresh_weekday", 0)

    if sm_mode == "directlake":
        print(f"  Direct Lake – no refresh needed: {dataset_name}")
        return (1, dataset_name, f"mode={sm_mode}")

    if refresh_mode == "always_full":
        refresh_type = "full"
    elif refresh_mode == "always_automatic":
        refresh_type = "automatic"
    elif refresh_mode == "scheduled":
        refresh_type = "full" if datetime.now().weekday() == full_refresh_weekday \
                       else "automatic"
    else:
        raise ValueError(f"Unknown refresh_mode: '{refresh_mode}'")

    print(f"  Dataset name  : {dataset_name}")
    print(f"  SM workspace  : {variables.SM_WORKSPACE_NAME}")  # ← workspace frå VL
    print(f"  Refresh mode  : {refresh_mode}")
    print(f"  Refresh type  : {refresh_type}")
    print(f"  Notify option : {notify_option}")

    refresh_semantic_model(
        workspace_id  = SM_WORKSPACE_ID,      # ← resolved frå SM_WORKSPACE_NAME
        dataset_name  = dataset_name,
        notify_option = notify_option,
        refresh_type  = refresh_type,
        timeout       = timeout
    )

    # Verifiser siste refresh
    
    last = get_last_refresh_status(SM_WORKSPACE_ID, dataset_name)

    from pyspark.sql import Row

    refresh_metadata = spark.createDataFrame([Row(
        dataset_name   = dataset_name,
        refresh_type   = last.get("refreshType", refresh_type),
        refresh_mode   = refresh_mode,
        status         = last.get("status"),
        started_at     = last.get("startTime"),
        completed_at   = last.get("endTime"),
        request_id     = last.get("requestId"),
        pipeline_name  = PIPELINE_NAME,
        workspace_name = variables.SM_WORKSPACE_NAME
    )])

    refresh_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_BASE_PATH + "siva/refresh_metadata")

    print(f"  -> refresh_metadata written: {last.get('endTime')}")

    return (1, dataset_name, f"mode={refresh_mode}")

  
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
