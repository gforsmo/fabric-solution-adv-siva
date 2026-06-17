# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-6-sm-validate
# **Purpose**: Validate semantic model structure and content after refresh.
# 
# **Stage**: Semantic Model validation
# 
# **Dependencies**: nb-av01-generic-functions
# 
# **Metadata**: instructions.sm_validation, metadata.sm_expectation_store

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

variables       = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
SM_WORKSPACE_ID = get_workspace_id(variables.SM_WORKSPACE_NAME)

print(f"SM workspace: {variables.SM_WORKSPACE_NAME}")
print(f"SM workspace ID: {SM_WORKSPACE_ID}")

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

sm_expectation_lookup = load_sm_expectation_store(spark)
log_lookup            = load_log_store(spark)
val_instructions      = get_active_instructions(spark, "sm_validation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute SM Validation
# Expected fields in `instructions.sm_validation`:
# - `dataset_name` (str, required): Semantic model name
# - `expectation_id` (int, required): Lookup key in metadata.sm_expectation_store
# - `check_params` (JSON, optional): Parameters for the check
# - `severity` (str, optional): 'error' or 'warning' (default: 'error')
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

first_instr   = val_instructions[0] if val_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-6-sm-validate")

# Group validations by dataset_name to load SM metadata once per dataset
# Same pattern as nb-av01-4-validate grouping by target_table
validations_by_dataset = {}
for v in val_instructions:
    validations_by_dataset.setdefault(v["dataset_name"], []).append(v)

dataset_instructions = [
    {"dataset_name": d, "validations": vals, "log_function_id": 1}
    for d, vals in validations_by_dataset.items()
]


def sm_validate_executor(spark, instr):
    """Thin executor - core logic in run_sm_validation() in generic."""
    return run_sm_validation(
        spark                 = spark,
        instr                 = instr,
        workspace_id          = SM_WORKSPACE_ID,
        sm_expectation_lookup = sm_expectation_lookup
    )


execute_pipeline_stage(
    spark          = spark,
    instructions   = dataset_instructions,
    stage_executor = sm_validate_executor,
    notebook_name  = NOTEBOOK_NAME,
    pipeline_name  = PIPELINE_NAME,
    action_type    = ACTION_SM_VALIDATE,
    log_lookup     = log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric

rel_debug = fabric.evaluate_dax(
    dataset="sm_av01_bedrift",
    workspace=SM_WORKSPACE_ID,
    dax_string="""
EVALUATE
INFO.RELATIONSHIPS()
"""
)

display(rel_debug)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
