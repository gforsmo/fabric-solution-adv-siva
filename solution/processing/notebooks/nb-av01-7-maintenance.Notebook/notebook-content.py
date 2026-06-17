# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-maintenance
# **Purpose**: Run OPTIMIZE and VACUUM on Bronze, Silver and Gold lakehouse tables,
# and clean up old Files/ landing zone archives.
# 
# **Stage**: Maintenance (run after pipeline or on separate schedule)
# 
# **Metadata**: lh_av01_admin.metadata.maintenance_settings
# 
# **Best practice defaults**:
# - OPTIMIZE: every 24h (compacts small files, improves read performance)
# - VACUUM: every 168h / 7 days (removes files older than retention period)
# - Retention: 168h minimum (required for time travel)
# - Files/: clean archived JSON older than 7 days

# MARKDOWN ********************

# ## Parameters

# CELL ********************

# Parameters – mark this cell as Parameters in Fabric UI
# Allows pipeline to override individual flags
RUN_OPTIMIZE           = True
RUN_VACUUM             = True
RUN_FILES_CLEANUP      = True
FILES_RETENTION_DAYS   = 7      # Delete archived JSON files older than this
ALLOW_VACUUM_ZERO_HOURS = False  # Safety: never allow VACUUM RETAIN 0 HOURS

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

CATALOG = variables.LH_WORKSPACE_NAME
BRONZE  = variables.BRONZE_LH_NAME
SILVER  = variables.SILVER_LH_NAME
GOLD    = variables.GOLD_LH_NAME
ADMIN   = variables.ADMIN_LH_NAME

SETTINGS_TABLE = f"`{CATALOG}`.`{ADMIN}`.metadata.maintenance_settings"

print(f"Catalog : {CATALOG}")
print(f"Settings: {SETTINGS_TABLE}")
print(f"Optimize: {RUN_OPTIMIZE}")
print(f"Vacuum  : {RUN_VACUUM}")
print(f"Files   : {RUN_FILES_CLEANUP}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 – OPTIMIZE and VACUUM (metadata-driven)

# CELL ********************

# Uses run_table_maintenance_from_settings() from nb-av01-generic-functions
# Reads maintenance_settings and runs OPTIMIZE/VACUUM based on:
#   - optimize_enabled + optimize_interval_hours + last_optimize_at
#   - vacuum_enabled  + vacuum_interval_hours  + last_vacuum_at

if RUN_OPTIMIZE or RUN_VACUUM:
    # Temporarily override flags if only one should run
    if not RUN_OPTIMIZE:
        print("NOTE: Skipping OPTIMIZE (disabled via parameter)")
    if not RUN_VACUUM:
        print("NOTE: Skipping VACUUM (disabled via parameter)")

    run_table_maintenance_from_settings(
        spark                   = spark,
        settings_table          = SETTINGS_TABLE,
        environment             = variables.LH_WORKSPACE_NAME,
        allow_vacuum_zero_hours = ALLOW_VACUUM_ZERO_HOURS
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 – Clean up Files/ landing zone

# CELL ********************

from datetime import datetime, timedelta

def cleanup_landing_zone(lakehouse, retention_days):
    """
    Delete JSON/JSONL files in archive/ subfolders older than retention_days.
    Only touches archive/ folders – never active landing files.
    """
    base_path = (
        f"abfss://{CATALOG}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse}.Lakehouse/Files/"
    )
    cutoff_ms = int((datetime.now() - timedelta(days=retention_days)).timestamp() * 1000)

    deleted  = 0
    scanned  = 0

    try:
        top_folders = notebookutils.fs.ls(base_path)
    except Exception:
        print(f"  {lakehouse}/Files/: empty or not found")
        return 0, 0

    for folder in top_folders:
        if not folder.isDir:
            continue
        # Look for archive/ subfolder
        archive_path = f"{folder.path}archive/"
        try:
            files = notebookutils.fs.ls(archive_path)
        except Exception:
            continue

        for f in files:
            if f.isDir or not f.name.endswith((".json", ".jsonl")):
                continue
            scanned += 1
            if f.modifyTime < cutoff_ms:
                try:
                    notebookutils.fs.rm(f.path)
                    deleted += 1
                except Exception as e:
                    print(f"  WARNING: could not delete {f.path}: {e}")

    return scanned, deleted


if RUN_FILES_CLEANUP:
    print(f"=== Files/ cleanup (retention: {FILES_RETENTION_DAYS} days) ===")

    total_scanned = 0
    total_deleted = 0

    for lakehouse in [BRONZE, SILVER, GOLD]:
        scanned, deleted = cleanup_landing_zone(lakehouse, FILES_RETENTION_DAYS)
        total_scanned += scanned
        total_deleted += deleted
        if scanned > 0:
            print(f"  {lakehouse}: {deleted}/{scanned} files deleted")

    print(f"\n  Total: {total_deleted}/{total_scanned} archive files deleted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 – Delta table statistics

# CELL ********************

def get_table_stats(catalog, lakehouse):
    """
    Show file count and size per table – helps identify tables needing OPTIMIZE.
    """
    base_path = (
        f"abfss://{catalog}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse}.Lakehouse/Tables/"
    )
    rows = []
    try:
        for schema in notebookutils.fs.ls(base_path):
            if not schema.isDir:
                continue
            for table in notebookutils.fs.ls(schema.path):
                if not table.isDir:
                    continue
                try:
                    detail = spark.sql(
                        f"DESCRIBE DETAIL `{catalog}`.`{lakehouse}`"
                        f".`{schema.name.rstrip('/')}`.`{table.name.rstrip('/')}`"
                    ).collect()[0]
                    rows.append({
                        "table":      f"{schema.name.rstrip('/')}.{table.name.rstrip('/')}",
                        "files":      detail["numFiles"],
                        "size_mb":    round(detail["sizeInBytes"] / 1024 / 1024, 1),
                        "partitions": detail.get("numPartitions", 0)
                    })
                except Exception:
                    pass
    except Exception:
        pass
    return rows


print("=== Delta table statistics ===")
for lakehouse in [BRONZE, SILVER, GOLD]:
    stats = get_table_stats(CATALOG, lakehouse)
    if stats:
        print(f"\n{lakehouse}:")
        print(f"  {'Table':<45} {'Files':>6} {'Size MB':>9}")
        print(f"  {'-'*45} {'-'*6} {'-'*9}")
        for s in sorted(stats, key=lambda x: x['size_mb'], reverse=True):
            warn = " ← many files" if s['files'] > 20 else ""
            print(f"  {s['table']:<45} {s['files']:>6} {s['size_mb']:>9.1f}{warn}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

print("=== Maintenance complete ===")
print(f"  OPTIMIZE + VACUUM : {'done' if RUN_OPTIMIZE or RUN_VACUUM else 'skipped'}")
print(f"  Files cleanup     : {'done' if RUN_FILES_CLEANUP else 'skipped'}")
print(f"  Statistics        : done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
