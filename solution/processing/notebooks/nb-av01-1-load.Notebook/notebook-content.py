# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-1-load
# **Purpose**: Load raw JSON files into Bronze Delta tables using column mappings.
#  
#  **Stage**: Raw (Files) → Bronze (Delta tables)
#  
#  **Dependencies**: nb-av01-generic-functions
#  
#  **Metadata**: instructions.loading, metadata.loading_store, metadata.column_mappings

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

# Build base paths
RAW_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Files")
BRONZE_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Tables")

# Øverst i load-notebooken — før execute_pipeline_stage
LOAD_START_TS = datetime.now()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Metadata

# CELL ********************

# Configure connection to metadata SQL database
set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

set_admin_lakehouse(
    workspace = variables.LH_WORKSPACE_NAME,
    lakehouse = "lh_av01_admin"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load loading store for function lookup (loading_id -> function_name)
loading_lookup = load_loading_store(spark)

# Load log store for logging
log_lookup = load_log_store(spark)

# Get all active loading instructions for bronze layer
loading_instructions = get_active_instructions(spark, "loading", layer="bronze")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(loading_instructions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Loading
#  Expected fields in each instruction from `instructions.loading`:
#  - `loading_id` (int, required): Lookup key in metadata.loading_store
#  - `source_path` (str, required): Path to raw JSON files in landing zone
#  - `target_table` (str, required): Delta table name in Bronze (e.g., 'youtube/channels')
#  - `merge_condition` (str, required): SQL MERGE condition (e.g., 'target.id = source.id')
#  - `merge_type` (str, required): 'update_all' or 'specific_columns'
#  - `merge_columns` (JSON str, optional): Column lists for specific_columns merge
#  - `load_params` (JSON str, optional): Additional parameters (e.g., column_mapping_id)
#  - `log_function_id` (int, required): Lookup key in metadata.log_store
#  - `pipeline_name` (str, optional): Pipeline name for logging
#  - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

# Read pipeline/notebook identity from instruction metadata
first_instr = loading_instructions[0] if loading_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-1-load")


def load_executor(spark, instr):
    """Execute a single loading instruction. Returns (row_count, source_name, detail)."""
    # Resolve loading function from metadata
    loading_meta = loading_lookup.get(instr["loading_id"])
    if not loading_meta:
        raise ValueError(f"Loading ID {instr['loading_id']} not found in loading_store")

    function_name = loading_meta["function_name"]
    loading_func = globals().get(function_name)
    if not loading_func:
        raise ValueError(f"Loading function '{function_name}' not found")

    # Build paths (build_source_path normalizes any 'Files/' prefix in metadata)
    source_path = build_source_path(RAW_BASE_PATH, instr["source_path"])
    target_path = f"{BRONZE_BASE_PATH}{instr['target_table']}"

    # ── Sjekk at det finnes filer ─────────────────────────────────────────────
    try:
        files = [f for f in notebookutils.fs.ls(source_path)
                 if not f.isDir and f.name.endswith((".json", ".jsonl"))]
        if not files:
            print(f"  -> Ingen nye filer i {instr['source_path']} – hopper over")
            return (0, instr["source_path"], instr["target_table"])
    except Exception:
        print(f"  -> Mappe finnes ikke – hopper over")
        return (0, instr["source_path"], instr["target_table"])

    # Parse optional JSON fields
    load_params = json.loads(instr["load_params"]) if instr.get("load_params") else {}
    merge_columns = json.loads(instr["merge_columns"]) if instr.get("merge_columns") else None

    print(merge_columns)

    if not instr.get("merge_type"):
        raise ValueError(f"merge_type is required in loading instruction for {instr['target_table']}")

    print(f"Loading: {instr['source_path']} -> {instr['target_table']}")

    #row_count = loading_func(
    #    spark=spark,
    #    source_path=source_path,
    #    target_path=target_path,
    #    column_mapping_id=load_params.get("column_mapping_id"),
    #    merge_condition=instr["merge_condition"],
    #    merge_type=instr["merge_type"],
    #    merge_columns=merge_columns
    #)

    row_count = loading_func(
        spark             = spark,
        source_path       = source_path,
        target_path       = target_path,
        column_mapping_id = load_params.get("column_mapping_id"),
        merge_condition   = instr["merge_condition"],
        merge_type        = instr["merge_type"],
        merge_columns     = merge_columns,
        key_columns       = json.loads(instr["key_columns"]) if instr.get("key_columns") else [],
        load_params       = load_params
    )

    print(f"  -> Loaded {row_count} rows")
    return (row_count, instr["source_path"], instr["target_table"])


execute_pipeline_stage(
    spark=spark,
    instructions=loading_instructions,
    stage_executor=load_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_LOADING,
    log_lookup=log_lookup
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Siste celle
LOAD_STATUS          = "OK"
LOAD_CRITICAL_ERRORS = 0
LOAD_WARNING_ERRORS  = 0

critical = spark.sql(f"""
    SELECT COUNT(*) as n 
    FROM `av01-dev-datastores`.`lh_av01_admin`.quarantine.loading_errors
    WHERE error_code = 'E005'
    AND _loading_ts >= '{LOAD_START_TS}'
""").collect()[0]["n"]

warnings = spark.sql(f"""
    SELECT COUNT(*) as n 
    FROM `av01-dev-datastores`.`lh_av01_admin`.quarantine.loading_errors
    WHERE error_code != 'E005'
    AND _loading_ts >= '{LOAD_START_TS}'
""").collect()[0]["n"]

if critical > 0:
    LOAD_STATUS          = "ERROR"
    LOAD_CRITICAL_ERRORS = critical
elif warnings > 0:
    LOAD_STATUS         = "WARNING"
    LOAD_WARNING_ERRORS = warnings

print(f"=== LOAD FULLFØRT ===")
print(f"  Status   : {LOAD_STATUS}")
print(f"  Kritiske : {LOAD_CRITICAL_ERRORS}")
print(f"  Advarsler: {LOAD_WARNING_ERRORS}")

#notebookutils.notebook.exit(LOAD_STATUS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapping = load_column_mappings(spark, "youtube_channels")
for m in mapping:
    print(m)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import inspect
print(inspect.getsource(_build_expected_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
#  SCHEMA DRIFT – MANUELL TESTCELLE
#  Kjøres KUN når RUN_DRIFT_TEST = True  (sett manuelt)
#  Skriver IKKE til log.schema_drift – bruker mock-funksjonar
#  Sender IKKE Teams-melding – printes til konsoll
#
#  NB: Sidan schema_drift_handler er henta via %run (ikkje import),
#  patcher vi globals() direkte i staden for sdh.funksjon
# =============================================================================

RUN_DRIFT_TEST = True   # ← sett True for å kjøre testen manuelt

if RUN_DRIFT_TEST:

    print("=" * 65)
    print("  SCHEMA DRIFT SIMULERINGSTEST  –  ikke produksjonsdata")
    print("=" * 65)

    # Scenario: "new_column" | "missing_column" | "rename" | "type_mismatch" | "combined"
    TEST_SCENARIO = "combined"

    # ── Simulert column_mapping ───────────────────────────────────────
    mock_mapping = [
        {"source": "video_id",             "target": "video_id",           "type": "string"},
        {"source": "snippet.title",        "target": "video_title",        "type": "string"},
        {"source": "snippet.publishedAt",  "target": "asset_publish_date", "type": "timestamp"},
        {"source": "statistics.viewCount", "target": "video_view_count",   "type": "bigint"},
        {"source": "statistics.likeCount", "target": "video_like_count",   "type": "bigint"},
        {"source": "_loading_ts",          "target": "loading_ts",         "type": "timestamp"},
    ]

    mock_load_params = {
        "column_mapping_id": "youtube_videos",
        "required_fields":   "video_id,snippet,statistics",
        "not_null_fields":   "video_id",
    }

    # ── Bygg simulert rådata ──────────────────────────────────────────
    from pyspark.sql import Row

    base_row = {
        "video_id":   "abc123",
        "snippet":    Row(title="Test video", publishedAt="2024-01-01T00:00:00Z"),
        "statistics": Row(viewCount="1000", likeCount="50"),
    }

    scenarios = {
        "new_column":     ([{**base_row, "contentDetails": Row(duration="PT4M13S")}],
                           "Nytt felt 'contentDetails' dukker opp i filen"),
        "missing_column": ([{"video_id": "abc123",
                             "snippet": Row(title="Test video", publishedAt="2024-01-01T00:00:00Z")}],
                           "Required felt 'statistics' mangler i filen"),
        "rename":         ([{"video_id":      "abc123",
                             "video_snippet": Row(title="Test video", publishedAt="2024-01-01T00:00:00Z"),
                             "statistics":    Row(viewCount="1000", likeCount="50")}],
                           "Feltet 'snippet' er renamed til 'video_snippet'"),
        "type_mismatch":  ([{"video_id":   "abc123",
                             "snippet":    Row(title="Test video", publishedAt="2024-01-01T00:00:00Z"),
                             "statistics": Row(viewCount=1000, likeCount=50)}],
                           "statistics.viewCount endret fra string til int"),
        "combined":       ([{"video_id":      "abc123",
                             "video_snippet": Row(title="Test video", publishedAt="2024-01-01T00:00:00Z"),
                             "statistics":    Row(viewCount=1000, likeCount=50),
                             "newField":      "ukjent_verdi"}],
                           "Kombinert: rename + type + nytt felt + manglende felt"),
    }

    if TEST_SCENARIO not in scenarios:
        print(f"  Ukjent scenario: '{TEST_SCENARIO}'")
    else:
        test_rows, desc = scenarios[TEST_SCENARIO]
        print(f"\n  Scenario    : {TEST_SCENARIO}")
        print(f"  Beskrivelse : {desc}\n")

        # Flat struktur direkte – detect_schema_drift håndterer begge format
        mock_raw_df = spark.createDataFrame(test_rows)


        # ── Mock-funksjonar ───────────────────────────────────────────
        # Patch globals() direkte – fungerer med %run-basert import
        _orig_log_drift   = globals()["log_drift"]
        _orig_notify      = globals()["notify_teams_drift"]

        def _mock_log_drift(spark, drift_records, column_mapping_id,
                            source_path=None, file_name=None, run_id=None, **ctx):
            print(f"\n  [MOCK] Ville skrevet {len(drift_records)} rad(er) til log.schema_drift:")
            for r in drift_records:
                print(f"         {r['severity']:6} | {r['drift_type']:20} "
                      f"| kolonne='{r['column_name']}'")
                print(f"                → {r['suggested_action'][:100]}")
            return len(drift_records)

        def _mock_notify(drift_report, source_path, file_name,
                         column_mapping_id, run_id=None,
                         pipeline_name="data_pipeline",
                         notebook_name="nb-av01-1-load",
                         teams_webhook_url=None):
            print(f"\n  [MOCK] Ville sendt Teams-varsling via Power Automate webhook")
            print(f"         {drift_report['high_count']} KRITISK, "
                  f"{drift_report['medium_count']} ADVARSEL/ANMERKNING")
            for r in drift_report["records"]:
                ec  = _DRIFT_TO_ERROR_CODE.get(r["drift_type"], "E005")
                sev = _SEVERITY[ec]
                print(f"         {sev['icon']} [{ec}] {r['drift_type']:20} "
                      f"'{r['column_name']}'  → {sev['label']}")

        globals()["log_drift"]          = _mock_log_drift
        globals()["notify_teams_drift"] = _mock_notify

        try:
            result = detect_schema_drift(
                spark             = spark,
                raw_df            = mock_raw_df,
                mapping           = mock_mapping,
                load_params       = mock_load_params,
                source_path       = "Files/youtube_data_v3/videos/",
                file_name         = f"TEST_{TEST_SCENARIO}_2026-05-29.json",
                column_mapping_id = "youtube_videos",
                run_id            = -1,
            )

            print(f"\n  {'─'*55}")
            print(f"  Resultat:")
            print(f"    has_drift   : {result['has_drift']}")
            print(f"    high_count  : {result['high_count']}")
            print(f"    medium_count: {result['medium_count']}")
            print(f"    compatible  : {result['compatible']}")
            print(f"  {'─'*55}")
            print("  TEST FULLFØRT – ingen ekte data blei berørt\n")

        finally:
            globals()["log_drift"]          = _orig_log_drift
            globals()["notify_teams_drift"] = _orig_notify

else:
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
