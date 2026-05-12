# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-0-ingest-api
# **Purpose**: Ingest data from external REST APIs to the Raw landing zone.
# 
# **Stage**: External APIs → Raw (Files in Bronze Lakehouse)
# 
# **Dependencies**: nb-av01-generic-functions, nb-av01-api-tools-youtube


# MARKDOWN ********************

# ## Imports & Setup

# PARAMETERS CELL ********************

# Parameters - passed via REST API execution
spn_tenant_id = ""
spn_client_id = ""
spn_client_secret = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

TEST_MODE = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure SPN credentials for Key Vault access if provided
if spn_tenant_id and spn_client_id and spn_client_secret:
    set_spn_credentials(spn_tenant_id, spn_client_id, spn_client_secret)

# Load workspace-specific variables from Variable Library
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# Build base path for raw files landing zone (Files area of Bronze LH)
RAW_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TEST_MODE = False

set_admin_lakehouse(
    workspace = variables.LH_WORKSPACE_NAME,
    lakehouse = "lh_av01_admin"
)
# Configure connection to metadata SQL database
set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-api-tools-youtube

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-api-tools-brreg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-api-tools-sharepoint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(METADATA_DB_URL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Metadata

# CELL ********************

# Load source store for API connection details (source_id -> base_url, key_vault_url, handler_function, etc.)
source_lookup = load_source_store(spark)

# Load log store for logging function lookup
log_lookup = load_log_store(spark)

# Get all active ingestion instructions
ingestion_instructions = get_active_instructions(spark, "ingestion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ingestion_instructions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Ingestion
#  Expected fields in each instruction from `instructions.ingestion`:
#  - `source_id` (int, required): Lookup key in metadata.source_store
#  - `endpoint_path` (str, required): API endpoint path (e.g., '/channels')
#  - `request_params` (JSON str, optional): Query parameters for the API call
#  - `landing_path` (str, required): Subfolder in Raw landing zone
#  - `log_function_id` (int, required): Lookup key in metadata.log_store
#  - `pipeline_name` (str, optional): Pipeline name for logging
#  - `notebook_name` (str, optional): Notebook name for logging
#  Expected fields in `metadata.source_store`:
#  - `source_name` (str): Human-readable source name
#  - `base_url` (str): API base URL
#  - `key_vault_url` (str): Azure Key Vault URL
#  - `secret_name` (str): Secret name in Key Vault
#  - `handler_function` (str): Ingestion handler function name (e.g., 'ingest_youtube')

# CELL ********************

# Read pipeline/notebook identity from instruction metadata
first_instr = ingestion_instructions[0] if ingestion_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-0-ingest-api")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# Shared context for cross-instruction dependencies
ingestion_context = {"raw_base_path": RAW_BASE_PATH}


def ingest_executor(spark, instr):
    """Execute a single ingestion instruction. Returns (row_count, source_name, detail)."""
    source_meta = source_lookup.get(instr["source_id"])
    if not source_meta:
        raise ValueError(f"Source ID {instr['source_id']} not found in source_store")

    print(f"Ingesting: {source_meta['source_name']}/{instr['endpoint_path']}")

    # Hent API-nøkkel kun hvis kilden krever autentisering
    # auth_method='none' (BRREG) → api_key=None, ingen Key Vault-kall
    # auth_method='api_key' / 'oauth_spn' → hent fra Key Vault
    key_vault_url = source_meta.get("key_vault_url")
    secret_name   = source_meta.get("secret_name")

    if source_meta.get("auth_method", "none") != "none" and key_vault_url and secret_name:
        api_key = get_api_key_from_keyvault(key_vault_url, secret_name)
    else:
        api_key = None

    handler_func = resolve_ingestion_handler(source_meta)
    items = handler_func(source_meta, instr, api_key, ingestion_context)

    if items:
        item_count = write_to_landing_zone(items, RAW_BASE_PATH, instr["landing_path"])
        print(f"  -> Saved {item_count} items to {instr['landing_path']}")
    else:
        item_count = 0
        print(f"  -> No items to save")

    return (item_count, source_meta["source_name"], instr["endpoint_path"])


execute_pipeline_stage(
    spark=spark,
    instructions=ingestion_instructions,
    stage_executor=ingest_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_INGESTION,
    log_lookup=log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

INGEST_STATUS    = "OK"
INGEST_ROW_COUNT = 0

print(f"=== INGEST FULLFØRT ===")
print(f"  Status    : {INGEST_STATUS}")
print(f"  Tidspunkt : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

notebookutils.notebook.exit(INGEST_STATUS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tøm alle Bronze Delta-tabeller
'''
tables = [
    "brreg.enheter",
    "sharepoint.meldingslogg",
    "sharepoint.regnskapbedrifter",
    "youtube.channel",
    "youtube.playlist_items",
    "youtube.videos",
    "quarantine.loading_errors",
]

for table in tables:
    spark.sql(f"DELETE FROM `av01-dev-datastores`.`lh_av01_bronze`.{table} WHERE 1=1")
    print(f"  Tømt: {table}")

# Nullstill alle watermarks – tvinger full load ved neste kjøring
spark.sql("""
    UPDATE `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
    SET watermark_date = NULL,
        watermark_id   = NULL,
        updated_at     = current_timestamp()
""")

print("Watermarks nullstilt – neste kjøring gjør full load")
spark.sql("SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store").show(truncate=False)

print("Ferdig")


'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
