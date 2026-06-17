# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-0-reset-environment
# **Purpose**: Reset environment to initial state by clearing all data.
# 
# **When to run**: Before a clean pipeline run from scratch (dev/test only).
# 
# **What is reset**:
# - All Delta tables in Bronze / Silver / Gold (DELETE FROM - keeps structure)
# - Files/ landing zone in all lakehouses
# - Watermarks in lh_av01_admin
# - Pipeline logs and validation results
# - Semantic model refresh (empties cached data)
# 
# **What is NOT reset**:
# - Table structure and schemas
# - CDF and Delta protocol settings
# - Instructions and metadata SQL tables
# - maintenance_settings

# MARKDOWN ********************

# ## Parameters

# CELL ********************

# Parameters – mark this cell as Parameters in Fabric UI
ENVIRONMENT = "DEV"

RESET_TABLES     = True
RESET_FILES      = True
RESET_WATERMARKS = True
RESET_LOGS       = True
RESET_SM         = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

if ENVIRONMENT.upper() == "PROD":
    raise ValueError("Reset is blocked in PROD")

CATALOG = variables.LH_WORKSPACE_NAME
BRONZE  = variables.BRONZE_LH_NAME
SILVER  = variables.SILVER_LH_NAME
GOLD    = variables.GOLD_LH_NAME
ADMIN   = variables.ADMIN_LH_NAME

print(f"=== RESET ENVIRONMENT ===")
print(f"Environment      : {ENVIRONMENT}")
print(f"Catalog          : {CATALOG}")
print(f"Reset tables     : {RESET_TABLES}")
print(f"Reset files      : {RESET_FILES}")
print(f"Reset watermarks : {RESET_WATERMARKS}")
print(f"Reset logs       : {RESET_LOGS}")
print(f"Reset SM         : {RESET_SM}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 – Clear Delta Tables (Bronze / Silver / Gold)

# CELL ********************

from delta.tables import DeltaTable

def clear_delta_table(catalog, lakehouse, schema_name, table_name):
    """
    Clears a Delta table using DeltaTable.forPath().delete().
    Uses ABFS path directly - no default lakehouse context needed.
    Works for large tables without scanning all rows.
    """
    path = (
        f"abfss://{catalog}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse}.Lakehouse/Tables/{schema_name}/{table_name}"
    )
    try:
        dt = DeltaTable.forPath(spark, path)
        dt.delete()
        print(f"  OK {schema_name}.{table_name}")
    except Exception as e:
        print(f"  WARNING {schema_name}.{table_name}: {e}")

def clear_delta_table_fast(catalog, lakehouse, schema_name, table_name):
    path = (
        f"abfss://{catalog}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse}.Lakehouse/Tables/{schema_name}/{table_name}"
    )
    try:
        # Les schema utan data
        schema = spark.read.format("delta").load(path).schema

        # Overwrite med tom DataFrame – ren metadata-operasjon
        spark.createDataFrame([], schema) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "false") \
            .save(path)

        print(f"  OK {schema_name}.{table_name}")
    except Exception as e:
        print(f"  WARNING {schema_name}.{table_name}: {e}")

def get_tables_in_lakehouse(catalog, lakehouse):
    base_path = (
        f"abfss://{catalog}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse}.Lakehouse/Tables/"
    )
    result = []
    try:
        for schema in notebookutils.fs.ls(base_path):
            if not schema.isDir:
                continue
            schema_name = schema.name.rstrip("/")
            for table in notebookutils.fs.ls(schema.path):
                if table.isDir:
                    result.append({
                        "schema": schema_name,
                        "table":  table.name.rstrip("/")
                    })
    except Exception as e:
        print(f"  ERROR listing {lakehouse}: {e}")
    return result


if RESET_TABLES:
    print("=== Clearing Delta tables ===")

    for lakehouse in [GOLD, SILVER, BRONZE]:
        tables = get_tables_in_lakehouse(CATALOG, lakehouse)
        print(f"\n{lakehouse}: found {len(tables)} tables")
        for t in tables:
            clear_delta_table_fast(CATALOG, lakehouse, t["schema"], t["table"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 – Clear Files/ Landing Zone

# CELL ********************

def delete_files_recursive(path):
    """Recursively deletes all files and folders under path."""
    try:
        items = notebookutils.fs.ls(path)
    except Exception:
        print(f"  Empty or not found: {path}")
        return

    for item in items:
        try:
            notebookutils.fs.rm(item.path, recurse=True)
            print(f"  Deleted: {item.path}")
        except Exception as e:
            print(f"  WARNING: {item.path}: {e}")


if RESET_FILES:
    print("=== Clearing Files/ landing zone ===")

    for lakehouse in [BRONZE, SILVER, GOLD]:
        files_path = (
            f"abfss://{CATALOG}@onelake.dfs.fabric.microsoft.com/"
            f"{lakehouse}.Lakehouse/Files/"
        )
        print(f"\n{lakehouse}:")
        delete_files_recursive(files_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 – Reset Watermarks

# CELL ********************

if RESET_WATERMARKS:
    print("=== Resetting watermarks ===")

    WATERMARK_TABLE = f"`{CATALOG}`.`{ADMIN}`.metadata.watermark_store"

    spark.sql(f"""
        UPDATE {WATERMARK_TABLE}
        SET
            watermark_date = NULL,
            watermark_id   = NULL,
            updated_at     = NULL
    """)

    print("  source_id and endpoint_path kept, dates nullified")
    display(spark.sql(f"SELECT * FROM {WATERMARK_TABLE} ORDER BY source_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 – Clear Pipeline Logs

# CELL ********************

if RESET_LOGS:
    print("=== Pipeline logs ===")
    print("  Run manually in Fabric SQL Database:")
    print("    DELETE FROM log.validation_results")
    print("    DELETE FROM log.pipeline_runs")
    print("  Or use maintenance.clear_logs via Pipeline Stored Procedure activity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 – Refresh Semantic Model (empty cached data)

# CELL ********************

if RESET_SM:
    print("=== Refreshing semantic model (clear cached data) ===")

    SM_WORKSPACE_ID = get_workspace_id(variables.SM_WORKSPACE_NAME)
    DATASET_NAME    = variables.SM_NAME_IMPORTMODE

    print(f"  Refreshing: {DATASET_NAME}")

    refresh_semantic_model(
        workspace_id  = SM_WORKSPACE_ID,
        dataset_name  = DATASET_NAME,
        notify_option = "NoNotification",
        refresh_type  = "full",
        timeout       = 600
    )

    print(f"  OK {DATASET_NAME} refreshed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

print("=== Reset complete ===")
print(f"  Tables     : {'cleared' if RESET_TABLES     else 'skipped'}")
print(f"  Files      : {'cleared' if RESET_FILES      else 'skipped'}")
print(f"  Watermarks : {'reset'   if RESET_WATERMARKS else 'skipped'}")
print(f"  Logs       : {'cleared' if RESET_LOGS       else 'skipped'}")
print(f"  SM refresh : {'done'    if RESET_SM         else 'skipped'}")
print("\nReady to run pipeline from scratch.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
