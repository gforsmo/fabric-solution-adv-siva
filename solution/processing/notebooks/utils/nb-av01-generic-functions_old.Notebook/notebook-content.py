# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "1765cc0e-0b76-b9b7-4466-6b06460f318e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # av01 Generic Functions Library
# Metadata-driven utility functions for the av01 orchestration framework.
#  
#  **Design Principle**: All function names come from metadata. Python implements the functions; metadata controls which ones get called.
#  
#  - Transform functions: `metadata.transform_store.function_name` → `globals().get(function_name)`
#  - GX expectations: `metadata.expectation_store.gx_method` → `getattr(gxe, gx_method)

# MARKDOWN ********************

# # Imports & Setup

# CELL ********************

# Standard library
import json
from datetime import datetime


# Fabric/Spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, BooleanType
from delta.tables import DeltaTable
import notebookutils

# Great Expectations
import great_expectations as gx
import great_expectations.expectations as gxe

# HTTP
import requests

# MSAL for SPN authentication to Key Vault
import msal

import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run schema_drift_handler

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constants
# Standardized values used across all pipeline notebooks.

# CELL ********************

# Pipeline status constants
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_RUNNING = "running"

# Action type constants
ACTION_INGESTION = "ingestion"
ACTION_LOADING = "loading"
ACTION_TRANSFORMATION = "transformation"
ACTION_VALIDATION = "validation"

# Layer constants
LAYER_RAW = "raw"
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_GOLD = "gold"

# Valid layers for validation
VALID_LAYERS = {LAYER_RAW, LAYER_BRONZE, LAYER_SILVER, LAYER_GOLD}

ACTION_SM_REFRESH  = "sm_refresh"
ACTION_SM_VALIDATE = "sm_validate"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric SQL connector - enables .mssql() method for reading/writing
import com.microsoft.sqlserver.jdbc.spark

# SQL Database connection - must be set via set_metadata_db_url() before use
METADATA_DB_URL = None

# SPN credentials for Key Vault access (set when running via REST API)
SPN_TENANT_ID = None
SPN_CLIENT_ID = None
SPN_CLIENT_SECRET = None


def set_spn_credentials(tenant_id: str, client_id: str, client_secret: str):
    """Configure SPN credentials for Key Vault access when running via REST API."""
    global SPN_TENANT_ID, SPN_CLIENT_ID, SPN_CLIENT_SECRET
    SPN_TENANT_ID = tenant_id
    SPN_CLIENT_ID = client_id
    SPN_CLIENT_SECRET = client_secret
    print("SPN credentials configured for Key Vault access")


def set_metadata_db_url(server: str, database: str):
    """
    Configure the metadata database URL for the Fabric SQL connector.
    Call this once at notebook startup.
    
    Args:
        server: SQL server name (without .database.fabric.microsoft.com suffix)
        database: Database name
    """
    global METADATA_DB_URL
    # Fabric SQL Database format - note curly braces around database name
    #METADATA_DB_URL = f"jdbc:sqlserver://{server}.database.fabric.microsoft.com:1433;database={{{database}}};encrypt=true;trustServerCertificate=false"

    METADATA_DB_URL = f"jdbc:sqlserver://{server}.database.fabric.microsoft.com:1433;database={database};encrypt=true;trustServerCertificate=false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Admin lakehouse konfigurasjon – settes av orchestrator via set_admin_lakehouse()
ADMIN_LH_WORKSPACE = None
ADMIN_LH_NAME      = None

def set_admin_lakehouse(workspace: str, lakehouse: str):
    """
    Konfigurerer admin lakehouse for watermark-lesing/-skriving.
    Kalles én gang ved oppstart i orchestrator-notebooken.
    """
    global ADMIN_LH_WORKSPACE, ADMIN_LH_NAME
    ADMIN_LH_WORKSPACE = workspace
    ADMIN_LH_NAME      = lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  **Helper Functions**
# 
# Path construction and file discovery utilities.

# CELL ********************

def get_layer_lakehouse(layer: str, variables) -> str:
    """
    Map layer name to lakehouse from Variable Library.

    Args:
        layer: Layer name ('raw', 'bronze', 'silver', 'gold')
        variables: Variable Library object with lakehouse names

    Returns:
        Lakehouse name for the specified layer

    Raises:
        ValueError: If layer is not recognized
    """
    mapping = {
        LAYER_RAW: variables.BRONZE_LH_NAME,  # Raw files stored in Bronze LH Files area
        LAYER_BRONZE: variables.BRONZE_LH_NAME,
        LAYER_SILVER: variables.SILVER_LH_NAME,
        LAYER_GOLD: variables.GOLD_LH_NAME
    }
    result = mapping.get(layer)
    if result is None:
        raise ValueError(f"Unknown layer '{layer}'. Valid layers: {list(mapping.keys())}")
    return result


def construct_abfs_path(workspace: str, lakehouse: str, area: str = "Tables") -> str:
    """
    Build ABFS base path for a lakehouse.

    Args:
        workspace: Workspace name
        lakehouse: Lakehouse name
        area: 'Tables' for Delta tables, 'Files' for raw files

    Returns:
        ABFS path string

    Raises:
        ValueError: If workspace or lakehouse is empty
    """
    if not workspace or not lakehouse:
        raise ValueError("workspace and lakehouse must not be empty")
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/{area}/"


def get_most_recent_file(base_path: str, prefix: str):
    files = [f for f in notebookutils.fs.ls(base_path)
             if not f.isDir and f.name.endswith((".json", ".jsonl"))]
    if not files:
        return None
    return max(files, key=lambda f: f.modifyTime)



def build_source_path(base_path: str, source_path: str) -> str:
    """
    Build full source path, normalizing any redundant 'Files/' prefix in metadata paths.

    The metadata source_path may include a 'Files/' prefix that is already
    part of the base_path (from construct_abfs_path with area='Files').

    Args:
        base_path: Base ABFS path (already includes Files/ area)
        source_path: Source path from metadata (may include 'Files/' prefix)

    Returns:
        Normalized full path
    """
    cleaned = source_path.replace("Files/", "", 1) if source_path.startswith("Files/") else source_path
    return f"{base_path}{cleaned}"


def write_to_landing_zone(items: list, base_path: str, landing_path: str) -> int:
    """
    Write API response items to the raw landing zone as a timestamped JSON file.

    Wraps multiple items in {"items": [...]} for consistent downstream parsing
    by load_json_to_delta. Single items are written unwrapped.

    Args:
        items: List of items from API response
        base_path: Base ABFS path for landing zone (Files area)
        landing_path: Subfolder path within landing zone

    Returns:
        Number of items written
    """
    # org skal fjernes
    #item_count = len(items)
    #output_path = f"{base_path}{landing_path}"
    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #file_path = f"{output_path}{timestamp}.json"

    #output_data = {"items": items} if item_count > 1 else (items[0] if items else {})
    #json_content = json.dumps(output_data, indent=2)
    #notebookutils.fs.put(file_path, json_content, overwrite=True)

    #return item_count

    item_count  = len(items)
    file_path   = f"{base_path}{landing_path}{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_data = {"items": items}  # ← alltid wrapper, også for enkelt-item
    notebookutils.fs.put(file_path, json.dumps(output_data, ensure_ascii=False, indent=2), overwrite=True)
    print(f"  -> Skrevet {item_count} items → {file_path}")

    return item_count


def resolve_ingestion_handler(source_meta: dict):
    """
    Resolve the ingestion handler function from source metadata.

    Args:
        source_meta: Source metadata dict (must contain 'handler_function')

    Returns:
        Callable handler function

    Raises:
        ValueError: If handler_function is missing or not found
    """
    handler_name = source_meta.get("handler_function")
    if not handler_name:
        raise ValueError(f"No handler_function defined for source '{source_meta.get('source_name')}'")

    handler_func = globals().get(handler_name)
    if not handler_func:
        raise ValueError(f"Handler function '{handler_name}' not found")

    return handler_func


def get_api_key_from_keyvault(key_vault_url: str, secret_name: str) -> str:
    """
    Retrieve API key from Azure Key Vault.

    Uses MSAL with SPN credentials if configured, otherwise uses notebookutils (interactive).

    Args:
        key_vault_url: Key Vault URL (e.g., 'https://my-vault.vault.azure.net/')
        secret_name: Name of the secret to retrieve

    Returns: Secret value as string
    """
    if SPN_CLIENT_ID:
        # Use MSAL to get a token, then call Key Vault REST API directly
        app = msal.ConfidentialClientApplication(
            SPN_CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{SPN_TENANT_ID}",
            client_credential=SPN_CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=["https://vault.azure.net/.default"])

        if "access_token" not in result:
            raise Exception(f"Failed to get token: {result.get('error_description')}")

        # Call Key Vault REST API
        url = f"{key_vault_url.rstrip('/')}/secrets/{secret_name}?api-version=7.4"
        headers = {"Authorization": f"Bearer {result['access_token']}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()["value"]
    else:
        # Use notebookutils for interactive execution (requires user credentials)
        return notebookutils.credentials.getSecret(key_vault_url, secret_name)


def get_workspace_id(workspace_name: str) -> str:
    """Resolves workspace name to GUID via Fabric."""
    import sempy.fabric as fabric
    return fabric.resolve_workspace_id(workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# transform_id=8
def generate_date_dimension(df, start_year: int, end_year: int, **ctx):
    spark = ctx.get("spark")
    return spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_year}-01-01'),
            to_date('{end_year}-12-31'),
            interval 1 day
        )) AS dato
    """).select(
        F.date_format("dato", "yyyyMMdd").cast("int").alias("dato_surrogate_id"),
        F.col("dato"),
        F.year("dato").alias("aar"),
        F.quarter("dato").alias("kvartal"),
        F.month("dato").alias("maaned"),
        F.date_format("dato", "MMMM").alias("maaned_navn"),
        F.weekofyear("dato").alias("uke"),
        F.dayofmonth("dato").alias("dag"),
        F.dayofweek("dato").alias("ukedag_nr"),
        F.date_format("dato", "EEEE").alias("ukedag_navn"),
        F.when(F.dayofweek("dato").isin([1, 7]), True)
         .otherwise(False).alias("er_helg"),
        F.concat(F.lit("Q"), F.quarter("dato"), F.lit("-"), F.year("dato"))
         .alias("aar_kvartal"),
        F.date_format("dato", "yyyy-MM").alias("aar_maaned")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Logging Functions**
# 
# Function names must match `metadata.log_store.function_name`.

# CELL ********************

def log_standard(spark, pipeline_name, notebook_name, status,
                 rows_processed=0, error_message=None,
                 action_type=None, source_name=None,
                 instruction_detail=None, started_at=None, **ctx):
    completed_at = datetime.now()
    started_at   = started_at or completed_at

    schema = StructType([
        StructField("run_id",            LongType(),      False),
        StructField("pipeline_name",     StringType(),    False),
        StructField("started_at",        TimestampType(), True),
        StructField("completed_at",      TimestampType(), True),
        StructField("status",            StringType(),    False),
        StructField("records_processed", IntegerType(),   True),
        StructField("error_message",     StringType(),    True),
        StructField("action_type",       StringType(),    True),
        StructField("source_name",       StringType(),    True),
        StructField("instruction_detail",StringType(),    True),
        StructField("notebook_name",     StringType(),    True)
    ])

    if status == "running":
        completed_at = None

    log_df = spark.createDataFrame(
        [(0, pipeline_name, started_at, completed_at, status,
          rows_processed, error_message, action_type,
          source_name, instruction_detail, notebook_name)],
        schema
    )
    log_df.write.mode("append").option("url", METADATA_DB_URL).mssql("log.pipeline_runs")

    detail = f"{action_type}: {source_name or ''} -> {instruction_detail or ''}"
    print(f"  -> Logged: {detail} - {status} ({rows_processed} rows)")
  

    return rows_processed


def log_validation(spark, validation_result, target_table=None,
                   lakehouse_name=None, started_at=None, **ctx):
    executed_at = datetime.now()
    validation_instructions = validation_result.meta.get("validation_instructions", [])

    parts       = target_table.split("/") if target_table else []
    schema_name = parts[0] if len(parts) == 2 else None
    table_name  = parts[1] if len(parts) == 2 else target_table

    schema = StructType([
        StructField("result_id",           LongType(),    False),
        StructField("run_id",              LongType(),    True),
        StructField("validation_instr_id", IntegerType(), True),
        StructField("expectation_type",    StringType(),  True),
        StructField("column_name",         StringType(),  True),
        StructField("passed",              BooleanType(), False),
        StructField("observed_value",      StringType(),  True),
        StructField("executed_at",         TimestampType(),True),
        StructField("lakehouse_name",      StringType(),  True),
        StructField("schema_name",         StringType(),  True),
        StructField("table_name",          StringType(),  True)
    ])

    results_data = []
    for i, result in enumerate(validation_result.results):
        v_id = validation_instructions[i].get("validation_instr_id") if i < len(validation_instructions) else None
        results_data.append((
            0, None, v_id,
            result.expectation_config.type,
            result.expectation_config.kwargs.get("column"),
            result.success,
            json.dumps(result.result) if hasattr(result, "result") and result.result else None,
            executed_at, lakehouse_name, schema_name, table_name
        ))

    spark.createDataFrame(results_data, schema) \
        .write.mode("append").option("url", METADATA_DB_URL).mssql("log.validation_results")

    print(f"  -> Logged {len(results_data)} validation results for {target_table}")
    return len(results_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def les_delta_med_cdf(spark, source_path, source_table, use_cdf):
    if not use_cdf:
        return spark.read.format("delta").load(source_path), None

    siste_kjort = spark.read \
        .option("url", METADATA_DB_URL) \
        .mssql(f"""(
            SELECT MAX(completed_at) AS siste_kjort
            FROM log.pipeline_runs
            WHERE source_name = '{source_table}'
            AND   action_type = 'transformation'
            AND   status      = 'success'
        ) AS q""") \
        .collect()[0][0]

    if not siste_kjort:
        print("  CDF: ingen tidligere kjøring – leser alle rader")
        return spark.read.format("delta").load(source_path), None

    print(f"  CDF fra: {siste_kjort}")

    CDF_COLS = ["_change_type", "_commit_version", "_commit_timestamp"]

    try:
        # Les éin gong – split i to
        cdf_df = spark.read.format("delta") \
            .option("readChangeFeed",    "true") \
            .option("startingTimestamp", str(siste_kjort)) \
            .load(source_path) \
            .cache()   # ← cache slik at split ikkje les to gonger

        df = cdf_df \
            .filter(F.col("_change_type").isin("insert", "update_postimage")) \
            .drop(*CDF_COLS)   # ← fjern CDF-metadata

        slettet_df = cdf_df \
            .filter(F.col("_change_type") == "delete") \
            .drop(*CDF_COLS)   # ← fjern CDF-metadata

        antall_endret  = df.count()
        antall_slettet = slettet_df.count()

        print(f"  CDF: {antall_endret} endringar, {antall_slettet} slettar")

        cdf_df.unpersist()   # ← frigjer cache

        if antall_slettet == 0:
            slettet_df = None

        return df, slettet_df

    except Exception as e:
        if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in str(e):
            print("  CDF: ingen nye endringar sidan siste transformasjon")
            return spark.read.format("delta").load(source_path).limit(0), None

        elif any(x in str(e) for x in [
            "DELTA_MISSING_CHANGE_DATA", "changeDataFeed",
            "CDF", "change data feed", "not enabled"
        ]):
            print(f"  CDF ikkje tilgjengeleg frå {siste_kjort} – full last")
            return spark.read.format("delta").load(source_path), None

        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import msal  # OAuth 2.0 – brukes av ingest_sharepoint_* for token-henting
from datetime import datetime

# =============================================================================
# Watermark-funksjoner – leser/skriver til lh_av01_admin Delta-tabell
# Én rad per source_id + endpoint_path via DeltaTable.merge() (ekte UPSERT)
# Ingen append-vekst, ingen cleanup nødvendig
# =============================================================================

def _watermark_table() -> str:
    """Returnerer fullt kvalifisert tabellnavn for watermark_store."""
    return f"`{ADMIN_LH_WORKSPACE}`.`{ADMIN_LH_NAME}`.metadata.watermark_store"


def get_watermark(spark, source_id: int, endpoint_path: str) -> dict | None:
    """
    Henter watermark for en kilde og endepunkt fra Delta-tabell i lh_av01_admin.

    Én rad per source_id + endpoint_path – returnerer direkte uten max(updated_at).

    Logikk i ingest_brreg:
        watermark_id  er satt → bruk oppdateringsid (inkrementell kjøring N+1)
        watermark_date er satt → bruk dato (første inkrementell etter full load)
        begge er None         → ingen data hentet ennå → kjør full load (auto)
    """
    table = _watermark_table()
    df = spark.sql(f"""
        SELECT * FROM {table}
        WHERE source_id = {source_id}
          AND endpoint_path = '{endpoint_path}'
    """)
    rows = [r.asDict() for r in df.collect()]
    if not rows:
        return None
    return rows[0]


def update_watermark(spark, source_id: int, endpoint_path: str,
                     watermark_date: str = None, watermark_id: int = None):
    """
    Oppdaterer watermark via DeltaTable.merge() – ekte UPSERT.

    Alltid én rad per source_id + endpoint_path.
    Ingen append-vekst, ingen cleanup nødvendig.

    Send enten watermark_date eller watermark_id – ikke begge:
        watermark_date : ISO-8601 timestamp etter full load / ETag for Excel
        watermark_id   : oppdateringsid (int) etter inkrementell BRREG-kjøring
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

    schema = StructType([
        StructField("source_id",      IntegerType(), nullable=False),
        StructField("endpoint_path",  StringType(),  nullable=False),
        StructField("watermark_date", StringType(),  nullable=True),
        StructField("watermark_id",   IntegerType(), nullable=True),
        StructField("updated_at",     TimestampType(), nullable=True),
    ])

    df = spark.createDataFrame([(
        source_id,
        endpoint_path,
        watermark_date,
        watermark_id,
        datetime.now(),
    )], schema)

    table = _watermark_table()

    table = _watermark_table()  
    # table = "`av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store"

    DeltaTable.forName(spark, table) \
        .alias("target") \
        .merge(
            df.alias("source"),
            "target.source_id = source.source_id "
            "AND target.endpoint_path = source.endpoint_path"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print(f"  -> Watermark oppdatert: date={watermark_date}, id={watermark_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Loading Functions**
# 
# Function names must match `metadata.loading_store.function_name`.

# CELL ********************

# Etter:
def validate_and_quarantine(spark, source_df, key_columns, load_params,
                            source_path, column_mapping_id, target_table):


    quarantine_rows = []
    valid_df = source_df

    # ── 1. Schema-sjekk ───────────────────────────────────────────────────────
    required_fields_str = load_params.get("required_fields", "")
    if required_fields_str:
        source_cols    = [c.lower() for c in source_df.columns]
        required_lower = [f.strip().lower() for f in required_fields_str.split(",")]
        missing_fields = [f for f in required_lower if f not in source_cols]
        if missing_fields:
            print(f"  -> KRITISK: Påkrevde felt mangler: {missing_fields} → karantene")
            # Logg alle rader som schema-feil og stopp loading for denne kilden
            schema_error_df = source_df \
                .withColumn("validation_type", F.lit("SCHEMA_ERROR")) \
                .withColumn("error_code",      F.lit("E005")) \
                .withColumn("error_detail",    F.lit(f"Påkrevde felt mangler: {missing_fields}")) \
                .withColumn("source_path",     F.lit(source_path)) \
                .withColumn("column_mapping_id", F.lit(column_mapping_id)) \
                .withColumn("target_table",    F.lit(target_table)) \
                .withColumn("row_data",        F.to_json(F.struct(*source_df.columns))) \
                .withColumn("_loading_ts",     F.current_timestamp()) \
                .select("source_path", "column_mapping_id", "target_table",
                        "validation_type", "error_code", "error_detail",
                        "row_data", "_loading_ts")
            
            schema_error_df.write.format("delta").mode("append") \
                .saveAsTable("`av01-dev-datastores`.`lh_av01_admin`.quarantine.loading_errors")

            
            
            # Returner tom DataFrame → ingen rader til MERGE
            return source_df.filter(F.lit(False))
        
        print(f"  -> Schema OK: alle {len(required_lower)} påkrevde felt funnet")



    # ── 1b. Påkrevde felt ikke NULL ───────────────────────────────────────────
    not_null_fields = load_params.get("not_null_fields", "")
    if not_null_fields:
        for field in [f.strip() for f in not_null_fields.split(",")]:
            if field in valid_df.columns:
                null_df  = valid_df.filter(F.col(field).isNull())
                valid_df = valid_df.filter(F.col(field).isNotNull())
                n = null_df.count()
                if n > 0:
                    print(f"  -> ADVARSEL: {n} rader med NULL i '{field}' → karantene")
                    quarantine_rows.append(
                        null_df
                        .withColumn("validation_type", F.lit("NULL_VALUE"))
                        .withColumn("error_code",      F.lit("E004"))
                        .withColumn("error_detail",    F.lit(f"NULL-verdi i påkrevd felt: {field}"))
                    )


    # ── 2. NULL-nøkkel ────────────────────────────────────────────────────────
    if key_columns:
        null_condition = F.lit(False)
        for key in key_columns:
            null_condition = null_condition | F.col(key).isNull()

        null_df  = valid_df.filter(null_condition)
        valid_df = valid_df.filter(~null_condition)

        null_count = null_df.count()
        if null_count > 0:
            print(f"  -> ADVARSEL: {null_count} rader med NULL-nøkkel → karantene")
            quarantine_rows.append(
                null_df
                .withColumn("validation_type", F.lit("NULL_KEY"))
                .withColumn("error_code",      F.lit("E001"))
                .withColumn("error_detail",    F.lit(f"NULL i nøkkelkolonne(r): {key_columns}"))
            )

    # ── 3. Duplikate nøkler ───────────────────────────────────────────────────
    if key_columns:
        from pyspark.sql.window import Window
        w = Window.partitionBy(key_columns).orderBy(F.lit(1))
        valid_with_rank = valid_df.withColumn("_rank", F.row_number().over(w))

        dup_df   = valid_with_rank.filter(F.col("_rank") > 1).drop("_rank")
        valid_df = valid_with_rank.filter(F.col("_rank") == 1).drop("_rank")

        dup_count = dup_df.count()
        if dup_count > 0:
            print(f"  -> ADVARSEL: {dup_count} duplikate nøkler → karantene")
            quarantine_rows.append(
                dup_df
                .withColumn("validation_type", F.lit("DUPLICATE_KEY"))
                .withColumn("error_code",      F.lit("E002"))
                .withColumn("error_detail",    F.lit(f"Duplikat nøkkel: {key_columns}"))
            )

    # ── 4. Orgnr-validering ───────────────────────────────────────────────────────
    orgnr_column = load_params.get("orgnr_column")
    if orgnr_column and orgnr_column in valid_df.columns:

        # Normaliser først – fjern mellomrom
        valid_df = valid_df.withColumn(
            orgnr_column,
            F.regexp_replace(F.col(orgnr_column), r"\s+", "")
        )

        # Valider – må være eksakt 9 numeriske siffer
        invalid_condition = (
            F.col(orgnr_column).isNotNull() & (
                (F.length(F.col(orgnr_column)) != 9) |
                (~F.col(orgnr_column).rlike("^[0-9]{9}$"))
            )
        )

        invalid_orgnr = valid_df.filter(invalid_condition)
        valid_df      = valid_df.filter(~invalid_condition | F.col(orgnr_column).isNull())

        orgnr_count = invalid_orgnr.count()
        if orgnr_count > 0:
            print(f"  -> ADVARSEL: {orgnr_count} rader med ugyldig orgnr → karantene")
            quarantine_rows.append(
                invalid_orgnr
                .withColumn("validation_type", F.lit("INVALID_ORGNR"))
                .withColumn("error_code",      F.lit("E003"))
                .withColumn("error_detail",    F.concat(
                    F.lit(f"'{orgnr_column}' = '"),
                    F.col(orgnr_column),
                    F.lit("' er ikke 9 numeriske siffer")
                ))
            )

    # ── Skriv til quarantine ──────────────────────────────────────────────────
    if quarantine_rows:
        from functools import reduce
        quarantine_df = reduce(lambda a, b: a.union(b), quarantine_rows)
        quarantine_df = quarantine_df \
            .withColumn("source_path",       F.lit(source_path)) \
            .withColumn("column_mapping_id", F.lit(column_mapping_id)) \
            .withColumn("target_table",      F.lit(target_table)) \
            .withColumn("row_data",          F.to_json(F.struct(*[
                c for c in source_df.columns
            ]))) \
            .withColumn("_loading_ts",       F.current_timestamp()) \
            .select("source_path", "column_mapping_id", "target_table",
                    "validation_type", "error_code", "error_detail",
                    "row_data", "_loading_ts")

        # Etter:
        quarantine_df.write.format("delta").mode("append") \
            .saveAsTable("`av01-dev-datastores`.`lh_av01_admin`.quarantine.loading_errors")

        total_q = quarantine_df.count()
        print(f"  -> Karantene totalt: {total_q} rader → quarantine/loading_errors")

    print(f"  -> Gyldige rader til MERGE: {valid_df.count()}")
    return valid_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_json_to_delta(spark, source_path, target_path,
                       column_mapping_id, merge_condition,
                       merge_type="update_all", merge_columns=None,
                       key_columns=None, load_params=None,
                       **ctx):
    """
    Last JSON-filer frå Raw-sone til Delta-tabell med kolonne-mapping og MERGE.

    Støttar to JSON-format:
      1. {"items": [...]}  – frå write_to_landing_zone (YouTube, SharePoint)
      2. [{...}, {...}]    – JSON-array frå DuckDB full load (BRREG)

    Lastar ALLE ulasta filer i source_path (eldst fyrst).
    Køyrer schema drift-sjekk på kvar fil før mapping.
    Arkiverer kvar fil etter vellykka MERGE.
    Pipeline krasjar ALDRI pga. drift eller logging.
    """
    started_at = datetime.now()

    # ── 1. Kolonne-mapping frå metadata ──────────────────────────────────────
    mapping = load_column_mappings(spark, column_mapping_id)
    if not mapping:
        raise ValueError(f"Column mapping '{column_mapping_id}' not found")

    # ── 2. Hent alle filer (ekskluder archive/) ───────────────────────────────
    all_files = [f for f in notebookutils.fs.ls(source_path)
                 if not f.isDir
                 and f.name.endswith((".json", ".jsonl"))]

    if not all_files:
        print(f"  -> Ingen nye filer i {source_path} – hopper over")
        try:
            log_standard(
                spark              = spark,
                pipeline_name      = PIPELINE_NAME,
                notebook_name      = NOTEBOOK_NAME,
                status             = "success",
                rows_processed     = 0,
                action_type        = "loading",
                source_name        = column_mapping_id,
                instruction_detail = column_mapping_id,
                started_at         = started_at,
            )
        except Exception as e:
            print(f"  [log] Logging feilet (ikkje kritisk): {e}")
        return 0

    all_files = sorted(all_files, key=lambda f: f.modifyTime)
    print(f"  -> Fant {len(all_files)} fil(er) å laste")

    total_rows = 0

    # Akkumulerer drift på tvers av alle filer i dette kallet
    all_drift = {"has_drift": False, "high_count": 0,
                 "medium_count": 0,  "records": []}

    # ── 3. Behandle kvar fil ──────────────────────────────────────────────────
    for file in all_files:
        print(f"\n  -> Laster: {file.name}")

        # 3a. Les JSON
        raw_df = spark.read.option("multiLine", "true").json(file.path)

        # 3b. Schema drift-sjekk
        #     Samanliknar fila mot column_mapping + required_fields.
        #     Skriv avvik til log.schema_drift og varslar Teams ved funn.
        drift_report = check_and_report_drift(
            spark             = spark,
            raw_df            = raw_df,
            mapping           = mapping,
            load_params       = load_params or {},
            source_path       = source_path,
            file_name         = file.name,
            column_mapping_id = column_mapping_id,
        )
        if drift_report["has_drift"]:
            all_drift["has_drift"]     = True
            all_drift["high_count"]   += drift_report["high_count"]
            all_drift["medium_count"] += drift_report["medium_count"]
            all_drift["records"]      += drift_report["records"]

        # 3c. Suspender fil ved inkompatibelt skjema
        #     Fila vert IKKJE arkivert – vert forsøkt igjen neste køyring
        #     etter at column_mapping er oppdatert.
        if not drift_report["compatible"]:
            print(f"  ⛔ '{file.name}' suspendert – required felt manglar.")
            print(f"     Oppdater column_mapping og køyr pipeline på nytt.")
            continue

        # 3d. Håndter format-varianter
        if "items" in raw_df.columns:
            raw_df = raw_df.select(F.explode(F.col("items")).alias("item"))
            def get_col(source):
                col = F.col("item")
                for part in re.split(r'\.(?=[a-zA-Z])', source):
                    col = col.getField(part)
                return col
        else:
            def get_col(source):
                parts = re.split(r'\.(?=[a-zA-Z])', source)
                col   = F.col(f"`{parts[0]}`")
                for part in parts[1:]:
                    col = col.getField(part)
                return col

        # 3e. Bygg SELECT frå kolonne-mapping
        # Berre felt med include_in_load=True (default) vert lasta inn.
        # Felt med include_in_load=False er dokumenterte i mapping men droppast.
        select_exprs = []
        for col_map in mapping:
            if not col_map.get("include_in_load", True):
                continue   # kjent system-felt, dropp stille
            source   = col_map["source"]
            target   = col_map["target"]
            col_type = col_map["type"]

            if source == "_loading_ts":
                select_exprs.append(F.current_timestamp().alias(target))
            elif col_type == "timestamp":
                select_exprs.append(F.to_timestamp(get_col(source)).alias(target))
            elif col_type == "date":
                select_exprs.append(F.to_date(get_col(source)).alias(target))
            elif col_type == "int":
                select_exprs.append(get_col(source).cast("int").alias(target))
            elif col_type == "bigint":
                select_exprs.append(get_col(source).cast("bigint").alias(target))
            elif col_type == "double":
                select_exprs.append(
                    F.when(get_col(source) == "", None)
                     .otherwise(get_col(source).cast("double"))
                     .alias(target)
                )
            elif col_type == "boolean":
                select_exprs.append(get_col(source).cast("boolean").alias(target))
            else:
                select_exprs.append(
                    F.when(get_col(source) == "", None)
                     .otherwise(get_col(source))
                     .alias(target)
                )

        source_df = raw_df.select(*select_exprs)
        print(f"  -> Kolonner mappet: {len(select_exprs)}")

        # 3f. Validering og karantene (eksisterande logikk)
        if key_columns or (load_params and load_params.get("required_fields")):
            source_df = validate_and_quarantine(
                spark             = spark,
                source_df         = source_df,
                key_columns       = key_columns or [],
                load_params       = load_params or {},
                source_path       = source_path,
                column_mapping_id = column_mapping_id,
                target_table      = target_path
            )

        row_count = source_df.count()

        # 3g. MERGE til Delta-tabell
        print(f"  -> MERGE til {target_path.split('Tables/')[-1]}...")
        delta_table   = DeltaTable.forPath(spark, target_path)
        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        )

        if merge_type == "update_all":
            merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        elif merge_type == "specific_columns" and merge_columns:
            update_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("update", [])}
            insert_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("insert", [])}
            merge_builder.whenMatchedUpdate(set=update_cols) \
                         .whenNotMatchedInsert(values=insert_cols).execute()

        print(f"  -> MERGE fullført ({row_count} rader)")

        # 3h. Arkiver fil etter vellykka MERGE
        try:
            folder_path  = file.path.replace(file.name, "")
            archive_path = f"{folder_path}archive/{file.name}"
            notebookutils.fs.mkdirs(f"{folder_path}archive/")
            notebookutils.fs.mv(file.path, archive_path, overwrite=True)
            print(f"  -> Arkivert: {file.name} → archive/")
        except Exception as e:
            print(f"  -> ADVARSEL: Arkivering feilet: {e}")

        total_rows += row_count

    # ── 4. Logg til log.pipeline_runs ─────────────────────────────────────────
    status = "warning" if all_drift["high_count"] > 0 else "success"

    try:
        log_standard(
            spark              = spark,
            pipeline_name      = PIPELINE_NAME,
            notebook_name      = NOTEBOOK_NAME,
            status             = status,
            rows_processed     = total_rows,
            action_type        = "loading",
            source_name        = column_mapping_id,
            instruction_detail = column_mapping_id,
            started_at         = started_at,
        )
    except Exception as e:
        print(f"  [log] Logging feilet (ikkje kritisk): {e}")

    return total_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re

def load_json_to_delta_org(spark, source_path, target_path,
                       column_mapping_id, merge_condition,
                       merge_type="update_all", merge_columns=None,
                       key_columns=None, load_params=None,
                       **ctx):
    """
    Load JSON files from Raw zone to Delta table with column mapping and MERGE.

    Støtter to JSON-formater:
      1. {"items": [...]} – fra write_to_landing_zone (YouTube, SharePoint, inkrementell)
      2. [{...}, {...}]   – JSON array fra DuckDB full load (BRREG)

    Laster ALLE ulastede filer i source_path (eldst først).
    Arkiverer hver fil etter vellykket MERGE → ingen hull i data ved feil.
    """
    # ── Kolonne-mapping fra metadata ──────────────────────────────────────────
    mapping = load_column_mappings(spark, column_mapping_id)
    if not mapping:
        raise ValueError(f"Column mapping '{column_mapping_id}' not found")

    # ── Hent alle filer i mappen (ekskluder archive/) ─────────────────────────
    all_files = [f for f in notebookutils.fs.ls(source_path)
                 if not f.isDir
                 and f.name.endswith((".json", ".jsonl"))]

    if not all_files:
        print(f"  -> Ingen nye filer i {source_path} – hopper over")
        return 0

    # Sorter eldst først – sikrer riktig rekkefølge ved flere filer
    all_files = sorted(all_files, key=lambda f: f.modifyTime)
    print(f"  -> Fant {len(all_files)} fil(er) å laste")

    total_rows = 0

    for file in all_files:
        print(f"  -> Laster: {file.name}")

        # ── Les JSON-fil ──────────────────────────────────────────────────────
        raw_df = spark.read.option("multiLine", "true").json(file.path)

        # ── Håndter format-varianter ──────────────────────────────────────────
        if "items" in raw_df.columns:
            raw_df = raw_df.select(F.explode(F.col("items")).alias("item"))
            def get_col(source):
                col = F.col("item")
                for part in re.split(r'\.(?=[a-zA-Z])', source):
                    col = col.getField(part)
                return col
        else:
            def get_col(source):
                parts = re.split(r'\.(?=[a-zA-Z])', source)
                col   = F.col(f"`{parts[0]}`")
                for part in parts[1:]:
                    col = col.getField(part)
                return col

        # ── Bygg SELECT fra kolonne-mapping ───────────────────────────────────
        select_exprs = []
        for col_map in mapping:
            source   = col_map["source"]
            target   = col_map["target"]
            col_type = col_map["type"]

            if source == "_loading_ts":
                select_exprs.append(F.current_timestamp().alias(target))
            elif col_type == "timestamp":
                select_exprs.append(F.to_timestamp(get_col(source)).alias(target))
            elif col_type == "date":
                select_exprs.append(F.to_date(get_col(source)).alias(target))
            elif col_type == "int":
                select_exprs.append(get_col(source).cast("int").alias(target))
            elif col_type == "bigint":
                select_exprs.append(get_col(source).cast("bigint").alias(target))
            elif col_type == "double":
                select_exprs.append(
                    F.when(get_col(source) == "", None)
                     .otherwise(get_col(source).cast("double"))
                     .alias(target)
                )
            elif col_type == "boolean":
                select_exprs.append(get_col(source).cast("boolean").alias(target))
            else:
                select_exprs.append(
                    F.when(get_col(source) == "", None)
                     .otherwise(get_col(source))
                     .alias(target)
                )

        source_df = raw_df.select(*select_exprs)
        print(f"  -> Kolonner mappet: {len(select_exprs)} kolonner")

        # ── Validering og karantene ───────────────────────────────────────────
        if key_columns or (load_params and load_params.get("required_fields")):
            source_df = validate_and_quarantine(
                spark             = spark,
                source_df         = source_df,
                key_columns       = key_columns or [],
                load_params       = load_params or {},
                source_path       = source_path,
                column_mapping_id = column_mapping_id,
                target_table      = target_path
            )

        row_count = source_df.count()
        

        # ── MERGE til Delta-tabell ────────────────────────────────────────────
        print(f"  -> Starter MERGE til {target_path.split('Tables/')[-1]}...")
        delta_table  = DeltaTable.forPath(spark, target_path)
        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        )

        if merge_type == "update_all":
            merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        elif merge_type == "specific_columns" and merge_columns:
            update_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("update", [])}
            insert_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("insert", [])}
            merge_builder.whenMatchedUpdate(set=update_cols) \
                         .whenNotMatchedInsert(values=insert_cols) \
                         .execute()

        print(f"  -> MERGE fullført")
        # ── Arkiver fil etter vellykket MERGE ─────────────────────────────────
        try:
            folder_path  = file.path.replace(file.name, "")
            archive_path = f"{folder_path}archive/{file.name}"
            notebookutils.fs.mkdirs(f"{folder_path}archive/")
            notebookutils.fs.mv(file.path, archive_path, overwrite=True)
            print(f"  -> Arkivert: {file.name} → archive/")
        except Exception as e:
            print(f"  -> ADVARSEL: Arkivering feilet: {e}")

        total_rows += row_count

    return total_rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Transform Functions**
#  
# Function names must match `metadata.transform_store.function_name`.
# 
# The orchestrator queries metadata to get `function_name` for each `transform_id`, then calls `globals().get(function_name)` to resolve the Python function.


# CELL ********************

def filter_nulls(df, columns: list, **ctx):
    """
    Remove rows where specified columns are null.
    
    Corresponds to: metadata.transform_store.function_name = 'filter_nulls'
    Expected params: {"columns": ["col1", "col2"]}
    """
    condition = F.col(columns[0]).isNotNull()
    for col_name in columns[1:]:
        condition = condition & F.col(col_name).isNotNull()
    return df.filter(condition)


def dedupe_by_window(df, partition_cols: list, order_col: str, order_desc: bool = True, **ctx):
    """
    Deduplicate using window function - keeps most recent by order column.
    
    Corresponds to: metadata.transform_store.function_name = 'dedupe_by_window'
    Expected params: {"partition_cols": [...], "order_col": "...", "order_desc": true}
    
    partition_cols can include expressions like "to_date(loading_TS)" which will be parsed.
    """
    partition_exprs = []
    for col in partition_cols:
        if "to_date(" in col:
            inner_col = col.replace("to_date(", "").replace(")", "")
            partition_exprs.append(F.to_date(F.col(inner_col)))
        else:
            partition_exprs.append(F.col(col))

    order_expr = F.col(order_col).desc() if order_desc else F.col(order_col)
    window_spec = Window.partitionBy(*partition_exprs).orderBy(order_expr)

    return (df
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num"))


def rename_columns(df, column_mapping: dict, **ctx):
    """
    Rename columns according to mapping, preserving all other columns.
    
    Corresponds to: metadata.transform_store.function_name = 'rename_columns'
    Expected params: {"column_mapping": {"old_name": "new_name", ...}}
    """
    result_df = df
    for old_name, new_name in column_mapping.items():
        result_df = result_df.withColumnRenamed(old_name, new_name)
    return result_df

def add_computed_columns(df, columns: dict, **ctx):
    """
    Add columns with computed values using Spark SQL expressions.
    Expected params: {"columns": {"col_name": "spark_sql_expression"}}
    """
    result_df = df
    for col_name, expr_str in columns.items():
        result_df = result_df.withColumn(col_name, F.expr(expr_str))
    return result_df

def add_literal_columns(df, columns: dict, **ctx):
    """
    Add columns with literal/static values.
    
    Corresponds to: metadata.transform_store.function_name = 'add_literal_columns'
    Expected params: {"columns": {"col_name": value, ...}}
    """
    result_df = df
    for col_name, value in columns.items():
        result_df = result_df.withColumn(col_name, F.lit(value))
    return result_df

def generate_surrogate_key(df, key_column_name: str, order_by_col: str,
                           natural_key=None, max_from_table: str = None, **ctx):
    """
    Generate surrogate key for new records only, preserving existing keys.

    natural_key kan være str (enkel nøkkel) eller list (sammensatt nøkkel).
    """
    spark          = ctx.get("spark")
    dest_base_path = ctx.get("dest_base_path", "")
    max_id         = 0
    existing_lookup = None

    # ── Bygg concat-kolonne for sammensatt nøkkel ─────────────────────────────
    composite_key = isinstance(natural_key, list)
    if composite_key:
        concat_expr = F.concat_ws("_", *[F.col(c).cast("string") for c in natural_key])
        df = df.withColumn("_natural_key", concat_expr)
        join_key = "_natural_key"
    else:
        join_key = natural_key

    if max_from_table and spark:
        try:
            full_path = dest_base_path + max_from_table
            target_df = DeltaTable.forPath(spark, full_path).toDF()
            max_id = target_df.agg(
                F.coalesce(F.max(key_column_name), F.lit(0))
            ).collect()[0][0]

            if join_key:
                # Bygg samme concat på target hvis sammensatt nøkkel
                if composite_key:
                    target_df = target_df.withColumn(
                        "_natural_key",
                        F.concat_ws("_", *[F.col(c).cast("string") for c in natural_key])
                    )
                existing_lookup = target_df.select(
                    F.col(join_key).alias("_lookup_natural_key"),
                    F.col(key_column_name).alias("_existing_surrogate_id")
                )
        except Exception as e:
            print(f"  -> Note: Could not read max ID from {max_from_table}: {e}")
            max_id = 0

    if existing_lookup is not None and join_key:
        df_with_existing = df.join(
            existing_lookup,
            df[join_key] == existing_lookup["_lookup_natural_key"],
            "left"
        )

        existing_records = df_with_existing.filter(F.col("_existing_surrogate_id").isNotNull())
        new_records      = df_with_existing.filter(F.col("_existing_surrogate_id").isNull())

        existing_with_key = existing_records.withColumn(
            key_column_name, F.col("_existing_surrogate_id")
        ).drop("_lookup_natural_key", "_existing_surrogate_id")

        # Dropp midlertidig concat-kolonne
        if composite_key:
            existing_with_key = existing_with_key.drop("_natural_key")

        if new_records.count() > 0:
            window_spec = Window.orderBy(order_by_col)
            new_with_key = new_records.withColumn(
                key_column_name, F.row_number().over(window_spec) + max_id
            ).drop("_lookup_natural_key", "_existing_surrogate_id")

            if composite_key:
                new_with_key = new_with_key.drop("_natural_key")

            return existing_with_key.unionByName(new_with_key)
        else:
            return existing_with_key
    else:
        if composite_key:
            df = df.drop("_natural_key")
        window_spec = Window.orderBy(order_by_col)
        return df.withColumn(key_column_name, F.row_number().over(window_spec) + max_id)

        
def generate_surrogate_key_old(df, key_column_name: str, order_by_col: str,
                           natural_key: str = None, max_from_table: str = None, **ctx):
    """
    Generate surrogate key for new records only, preserving existing keys.

    Corresponds to: metadata.transform_store.function_name = 'generate_surrogate_key'
    Expected params: {"key_column_name": "...", "order_by_col": "...", "natural_key": "...", "max_from_table": "..."}

    Args:
        key_column_name: Name of surrogate key column to create
        order_by_col: Column to order by when assigning new keys
        natural_key: Column that identifies unique records (e.g., 'asset_natural_id')
                    If provided, looks up existing surrogate IDs from target
        max_from_table: Target table path to get max existing ID and existing mappings

    Note: max_from_table is a relative table name (e.g., 'marketing/assets').
    The full path is built using dest_base_path from ctx.
    """
    spark = ctx.get("spark")
    dest_base_path = ctx.get("dest_base_path", "")
    max_id = 0
    existing_lookup = None

    if max_from_table and spark:
        try:
            full_path = dest_base_path + max_from_table
            target_df = DeltaTable.forPath(spark, full_path).toDF()
            max_id = target_df.agg(
                F.coalesce(F.max(key_column_name), F.lit(0))
            ).collect()[0][0]

            # If natural_key provided, get existing natural_key -> surrogate_key mapping
            if natural_key:
                existing_lookup = target_df.select(
                    F.col(natural_key).alias("_lookup_natural_key"),
                    F.col(key_column_name).alias("_existing_surrogate_id")
                )
        except Exception as e:
            # Table may not exist yet on first run - start from 0
            print(f"  -> Note: Could not read max ID from {max_from_table}: {e}")
            max_id = 0

    if existing_lookup is not None and natural_key:
        # Join to find existing surrogate IDs
        df_with_existing = df.join(
            existing_lookup,
            df[natural_key] == existing_lookup["_lookup_natural_key"],
            "left"
        )

        # Split into existing and new records
        existing_records = df_with_existing.filter(F.col("_existing_surrogate_id").isNotNull())
        new_records = df_with_existing.filter(F.col("_existing_surrogate_id").isNull())

        # For existing: use existing surrogate ID
        existing_with_key = existing_records.withColumn(
            key_column_name, F.col("_existing_surrogate_id")
        ).drop("_lookup_natural_key", "_existing_surrogate_id")

        # For new: generate new surrogate IDs starting from max_id + 1
        if new_records.count() > 0:
            window_spec = Window.orderBy(order_by_col)
            new_with_key = new_records.withColumn(
                key_column_name, F.row_number().over(window_spec) + max_id
            ).drop("_lookup_natural_key", "_existing_surrogate_id")

            return existing_with_key.unionByName(new_with_key)
        else:
            return existing_with_key
    else:
        # No natural key - original behavior (generate for all rows)
        window_spec = Window.orderBy(order_by_col)
        return df.withColumn(key_column_name, F.row_number().over(window_spec) + max_id)


def lookup_join(df, lookup_table: str, source_key: str,
                lookup_key: str, select_cols: list, **ctx):
    """
    Join to lookup/dimension table to get surrogate key or other columns.
    
    Corresponds to: metadata.transform_store.function_name = 'lookup_join'
    Expected params: {"lookup_table": "...", "source_key": "...", "lookup_key": "...", "select_cols": [...]}
    
    Note: lookup_table is a relative table name (e.g., 'marketing/assets').
    The full path is built using dest_base_path from ctx.
    """
    spark = ctx.get("spark")
    dest_base_path = ctx.get("dest_base_path", "")
    
    full_path = dest_base_path + lookup_table
    lookup_df = DeltaTable.forPath(spark, full_path).toDF()
    lookup_select = lookup_df.select(lookup_key, *select_cols)

    return df.join(
        lookup_select,
        df[source_key] == lookup_select[lookup_key],
        "left"
    ).drop(lookup_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_transform_function(function_name: str):
    """
    Resolve transform function by name using globals().
    function_name comes from metadata.transform_store.function_name column.
    The function must be defined in this module with a matching name.
    """
    return globals().get(function_name)


def execute_transform_pipeline(spark, df, pipeline: list, params: dict,
                               transform_lookup: dict, dest_base_path: str = ""):
    result_df = df
    ctx        = {"spark": spark, "dest_base_path": dest_base_path}
    call_count = {}  # ← ny: teller kall per transform_id

    for transform_id in pipeline:
        transform_meta = transform_lookup.get(transform_id)
        if not transform_meta:
            raise ValueError(f"Transform ID {transform_id} not found in metadata")

        function_name  = transform_meta["function_name"]
        transform_func = get_transform_function(function_name)
        if not transform_func:
            raise ValueError(f"Function '{function_name}' not implemented")

        # ── Støtt liste av params per transform_id ────────────────────────
        key   = str(transform_id)
        count = call_count.get(key, 0)       # ← ny
        call_count[key] = count + 1          # ← ny

        p_raw = params.get(key, {})
        if isinstance(p_raw, list):          # ← ny
            transform_params = p_raw[count] if count < len(p_raw) else {}
        else:
            transform_params = p_raw         # ← erstatter gammel linje

        result_df = transform_func(result_df, **transform_params, **ctx)

    return result_df

def execute_transform_pipeline_old(spark, df, pipeline: list, params: dict,
                               transform_lookup: dict, dest_base_path: str = ""):
    """
    Execute ordered transform pipeline using metadata lookup.

    Args:
        spark: SparkSession
        df: Input DataFrame
        pipeline: List of transform_ids in execution order (e.g., [1, 2])
                  From: instructions.transformations.transform_pipeline JSON
        params: Params keyed by transform_id (e.g., {"1": {...}, "2": {...}})
                From: instructions.transformations.transform_params JSON
        transform_lookup: Dict from load_transform_store()
                         {transform_id: {"function_name": "...", ...}}
        dest_base_path: Base ABFS path for destination layer (e.g., GOLD_BASE_PATH)
                       Used by lookup_join and generate_surrogate_key for table paths

    Returns: Transformed DataFrame
    """
    result_df = df
    ctx = {"spark": spark, "dest_base_path": dest_base_path}

    for transform_id in pipeline:
        # Get function_name from metadata lookup
        transform_meta = transform_lookup.get(transform_id)
        if not transform_meta:
            raise ValueError(f"Transform ID {transform_id} not found in metadata")

        function_name = transform_meta["function_name"]
        transform_func = get_transform_function(function_name)
        if not transform_func:
            raise ValueError(f"Function '{function_name}' not implemented")

        # Get params for this transform
        transform_params = params.get(str(transform_id), {})

        # Execute transform (pass ctx for functions that need spark/paths)
        result_df = transform_func(result_df, **transform_params, **ctx)

    return result_df


def merge_to_delta(spark, source_df, target_path: str, merge_condition: str,
                   merge_type: str = "update_all", merge_columns: dict = None):
    """
    Generic MERGE operation for any layer transition.
    
    Args:
        source_df: DataFrame to merge into target
        target_path: ABFS path to target Delta table
        merge_condition: SQL condition (e.g., 'target.id = source.id')
        merge_type: 'update_all' or 'specific_columns'
        merge_columns: Dict with 'update' and 'insert' lists if merge_type='specific_columns'
    
    Returns: row count
    """
    # ── Tom-sjekk – unngår unødvendig Delta-scan ──────────────────────────────
    # Kritisk for CDF-delete scenario der df er tom og berre slettar skjer
    if source_df.isEmpty():
        print("  -> No changes – skipping merge")
        return 0

    delta_table = DeltaTable.forPath(spark, target_path)
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"), merge_condition
    )

    if merge_type == "update_all":
        merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    elif merge_type == "specific_columns" and merge_columns:
        update_set = {c: F.col(f"source.{c}") for c in merge_columns.get("update", [])}
        insert_vals = {c: F.col(f"source.{c}") for c in merge_columns.get("insert", [])}
        merge_builder.whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_vals).execute()

    return source_df.count()


def execute_pipeline_stage(spark, instructions: list, stage_executor,
                           notebook_name: str, pipeline_name: str,
                           action_type: str, log_lookup: dict):
    """
    Execute a metadata-driven pipeline stage with standardized logging.

    Reduces code duplication across load, clean, and model notebooks by
    centralizing the try/except/log pattern.

    Args:
        spark: SparkSession
        instructions: List of instruction dicts from metadata
        stage_executor: Callable(spark, instr) -> (row_count, source_name, detail)
        notebook_name: Name of calling notebook for logging
        pipeline_name: Pipeline identifier for logging
        action_type: One of ACTION_LOADING, ACTION_TRANSFORMATION, etc.
        log_lookup: Log store lookup dict from load_log_store()

    Raises:
        Exception: Re-raises any exception after logging failure
    """
    for instr in instructions:
        start_time = datetime.now()
        source_name = None
        detail = None

        try:
            row_count, source_name, detail = stage_executor(spark, instr)

            # Log success using metadata-driven function lookup
            log_func_id = instr.get("log_function_id")
            if log_func_id:
                log_meta = log_lookup.get(log_func_id)
                if log_meta:
                    log_func = globals().get(log_meta["function_name"])
                    if log_func:
                        log_func(
                            spark=spark,
                            pipeline_name=pipeline_name,
                            notebook_name=notebook_name,
                            status=STATUS_SUCCESS,
                            rows_processed=row_count,
                            action_type=action_type,
                            source_name=source_name,
                            instruction_detail=detail,
                            started_at=start_time
                        )
                    else:
                        print(f"  -> WARNING: Log function '{log_meta['function_name']}' not found")
                else:
                    print(f"  -> WARNING: log_function_id '{log_func_id}' not found in log_store")

        except Exception as e:
            print(f"  -> ERROR: {str(e)}")

            # Log failure using metadata-driven function lookup
            log_func_id = instr.get("log_function_id")
            if log_func_id:
                log_meta = log_lookup.get(log_func_id)
                if log_meta:
                    log_func = globals().get(log_meta["function_name"])
                    if log_func:
                        log_func(
                            spark=spark,
                            pipeline_name=pipeline_name,
                            notebook_name=notebook_name,
                            status=STATUS_FAILED,
                            rows_processed=0,
                            error_message=str(e),
                            action_type=action_type,
                            source_name=source_name,
                            instruction_detail=detail,
                            started_at=start_time
                        )
                    else:
                        print(f"  -> WARNING: Could not log failure - function '{log_meta['function_name']}' not found")
                else:
                    print(f"  -> WARNING: Could not log failure - log_function_id '{log_func_id}' not found")
            raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# **Validation Functions**
# 
#  Uses `metadata.expectation_store.gx_method` to dynamically resolve GX expectation classes via `getattr(gxe, gx_method)`.
#  
#  No hardcoded expectation mapping - the gx_method column stores the actual class name.

# CELL ********************


def get_expectation_class(gx_method: str):
    """
    Dynamically resolve GX expectation class from method name.
    gx_method comes from metadata.expectation_store.gx_method column.
    """
    return getattr(gxe, gx_method)


def build_expectation(gx_method: str, column_name: str = None, validation_params: dict = None):
    """
    Build a GX expectation instance from metadata.
    
    Args:
        gx_method: Class name from metadata.expectation_store.gx_method
        column_name: Column to validate (from instructions.validations.column_name)
        validation_params: Additional params (from instructions.validations.validation_params JSON)
    """
    exp_class = get_expectation_class(gx_method)

    # Build kwargs based on expectation type
    kwargs = {}
    if column_name:
        # Check if it's a compound columns expectation
        if "Compound" in gx_method:
            kwargs["column_list"] = [column_name]
        else:
            kwargs["column"] = column_name

    # Merge any additional params from validation_params JSON
    if validation_params:
        kwargs.update(validation_params)

    return exp_class(**kwargs)


def run_table_validations(datasource, df, table_name: str,
                          table_validations: list, expectation_lookup: dict):
    """
    Run GX validations for a single table. Handles expectation building,
    suite creation, asset/batch management, and validation execution.

    Args:
        datasource: GX Spark datasource
        df: DataFrame to validate
        table_name: Table identifier (e.g., 'marketing/channels')
        table_validations: List of instruction dicts for this table
        expectation_lookup: Dict mapping expectation_id -> {gx_method, expectation_name, ...}

    Returns: (validation_result, expectations_metadata)
        validation_result: GX ValidationResult object
        expectations_metadata: List of dicts with expectation details for logging
    """
    # Build expectations from metadata
    expectations = []
    for v in table_validations:
        exp_meta = expectation_lookup.get(v["expectation_id"])
        if not exp_meta:
            print(f"  -> WARNING: expectation_id {v['expectation_id']} not found, skipping")
            continue

        params = json.loads(v["validation_params"]) if v.get("validation_params") else {}
        exp = build_expectation(
            gx_method=exp_meta["gx_method"],
            column_name=v.get("column_name"),
            validation_params=params
        )
        expectations.append({
            "expectation": exp,
            "severity": v.get("severity", "error"),
            "column": v.get("column_name"),
            "expectation_name": exp_meta["expectation_name"],
            "validation_instr_id": v["validation_instr_id"]
        })

    # Create expectation suite
    safe_name = table_name.replace("/", "_")
    suite = gx.ExpectationSuite(name=f"{safe_name}_suite")
    for e in expectations:
        suite.add_expectation(e["expectation"])

    # Get or create dataframe asset and batch definition
    asset_name = f"{safe_name}_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except LookupError:
        asset = datasource.add_dataframe_asset(name=asset_name)

    batch_def_name = f"batch_def_{safe_name}"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except LookupError:
        batch_definition = asset.add_batch_definition_whole_dataframe(name=batch_def_name)

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Run validation
    validation_result = batch.validate(suite)

    # Attach metadata for downstream logging
    validation_result.meta["table_name"] = table_name
    validation_result.meta["validation_instructions"] = expectations

    return validation_result, expectations




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Metadata queries**
# Functions to query the SQL metadata store using the native Fabric Spark SQL connector.
#  
#  **Note**: Uses `mssql()` method with automatic Microsoft Entra ID authentication - no credentials needed.


# CELL ********************

def query_metadata_table(spark, schema_table: str) -> list:
    """
    Query a metadata table using the native Fabric SQL connector.
    Uses automatic Microsoft Entra ID authentication.
    
    Args:
        spark: SparkSession
        schema_table: Table in schema.table format (e.g., 'metadata.transform_store')
    
    Returns: List of row dicts
    """
    df = spark.read.option("url", METADATA_DB_URL).mssql(schema_table)
    return [row.asDict() for row in df.collect()]


def load_source_store(spark) -> dict:
    """
    Load metadata.source_store as lookup dict by source_id.
    Used by ingestion notebook to get API connection details.
    """
    rows = query_metadata_table(spark, "metadata.source_store")
    return {row["source_id"]: row for row in rows}


def load_transform_store(spark) -> dict:
    """
    Load metadata.transform_store as lookup dict by transform_id.
    Used by execute_transform_pipeline() to resolve function_name.
    """
    rows = query_metadata_table(spark, "metadata.transform_store")
    return {row["transform_id"]: row for row in rows}


def load_expectation_store(spark) -> dict:
    """
    Load metadata.expectation_store as lookup dict by expectation_id.
    Used by run_validations() to resolve gx_method.
    """
    rows = query_metadata_table(spark, "metadata.expectation_store")
    return {row["expectation_id"]: row for row in rows}


def load_loading_store(spark) -> dict:
    """
    Load metadata.loading_store as lookup dict by loading_id.
    Used to resolve loading function_name.
    """
    rows = query_metadata_table(spark, "metadata.loading_store")
    return {row["loading_id"]: row for row in rows}


def load_log_store(spark) -> dict:
    """
    Load metadata.log_store as lookup dict by log_id.
    Used to resolve logging function_name.
    """
    rows = query_metadata_table(spark, "metadata.log_store")
    return {row["log_id"]: row for row in rows}


def load_sm_store(spark):
    df = spark.read.option("url", METADATA_DB_URL).mssql("metadata.sm_store")
    return {row["sm_function_id"]: row["function_name"] for row in df.collect()}

def load_sm_expectation_store(spark):
    df = spark.read.option("url", METADATA_DB_URL).mssql("metadata.sm_expectation_store")
    return {row["expectation_id"]: row["check_function"] for row in df.collect()}


def refresh_semantic_model(workspace_id, dataset_name,
                           notify_option="NoNotification",
                           refresh_type="automatic",
                           timeout=600):
    import sempy.fabric as fabric
    import time

    client   = fabric.FabricRestClient()
    datasets = fabric.list_datasets(workspace=workspace_id)

    # Find dataset ID column (varies by sempy version)
    id_column = None
    for candidate in ["Dataset ID", "Dataset Id", "id", "datasetId", "DatasetId"]:
        if candidate in datasets.columns:
            id_column = candidate
            break

    if not id_column:
        raise ValueError(
            f"Could not find dataset ID column. "
            f"Available columns: {datasets.columns.tolist()}"
        )

    match = datasets[datasets["Dataset Name"] == dataset_name]

    if match.empty:
        raise ValueError(
            f"Dataset '{dataset_name}' not found in workspace {workspace_id}. "
            f"Available: {datasets['Dataset Name'].tolist()}"
        )

    dataset_id = match[id_column].values[0]

    resp = client.post(
        f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
        json={
            "notifyOption": notify_option,
            "type":         refresh_type
        }
    )
    if resp.status_code not in (200, 202):
        raise Exception(f"Refresh failed HTTP {resp.status_code}: {resp.text}")
    print(f"  Refresh started: HTTP {resp.status_code}")

    start       = time.time()
    last_status = None              # ← legg til

    while time.time() - start < timeout:
        history = client.get(
            f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
        ).json()
        status = history["value"][0]["status"]

        if status != last_status:  # ← berre print ved endring
            print(f"  Status: {status}")
            last_status = status

        if status == "Completed":
            return "Completed"
        if status == "Failed":
            raise Exception(f"Refresh failed: {history['value'][0]}")

        time.sleep(20)
    

    raise TimeoutError(f"Refresh timeout after {timeout}s for '{dataset_name}'")




def get_last_refresh_status(workspace_id, dataset_name):
    """Returns last refresh details from Fabric."""
    client = fabric.FabricRestClient()

    datasets   = fabric.list_datasets(workspace=workspace_id)
    id_column  = next(c for c in ["Dataset ID", "Dataset Id", "id"] 
                      if c in datasets.columns)
    dataset_id = datasets[datasets["Dataset Name"] == dataset_name][id_column].values[0]

    history = client.get(
        f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    ).json()

    last = history["value"][0]
    print(f"\n  Last refresh summary:")
    print(f"    Status      : {last.get('status')}")
    print(f"    Start       : {last.get('startTime')}")
    print(f"    End         : {last.get('endTime')}")
    print(f"    Refresh type: {last.get('refreshType')}")
    print(f"    Triggered by: {last.get('requestId')}")

    return last



def load_column_mappings(spark, mapping_id: str) -> list:
    df = (
        spark.read
             .option("url", METADATA_DB_URL)
             .mssql(f"""(
                 SELECT
                     column_order,
                     source_column,
                     target_column,
                     data_type,
                     ISNULL(include_in_load, 1) AS include_in_load
                 FROM metadata.column_mappings
                 WHERE mapping_id = '{mapping_id}'
             ) AS q""")
    )
    rows = df.collect()
    if not rows:
        return []
    return [
        {
            "source":          row["source_column"],
            "target":          row["target_column"],
            "type":            row["data_type"],
            "include_in_load": bool(row["include_in_load"]),
        }
        for row in sorted(rows, key=lambda r: r["column_order"])
    ]

def get_active_instructions(spark, instruction_type: str, layer: str = None) -> list:
    """
    Get active instructions from the appropriate instruction table.
    
    Args:
        spark: SparkSession
        instruction_type: 'loading', 'transformations', 'validations', 'ingestion'
        layer: Optional filter by target_layer or dest_layer
    
    Returns: List of instruction row dicts
    """
    table = f"instructions.{instruction_type}"
    rows = query_metadata_table(spark, table)
    
    # Filter active instructions
    result = [r for r in rows if r.get("is_active") == 1]
    
    # Filter by layer if specified
    if layer:
        if instruction_type == "loading":
            result = [r for r in result if r.get("target_layer") == layer]
        elif instruction_type == "transformations":
            result = [r for r in result if r.get("dest_layer") == layer]
        elif instruction_type == "validations":
            result = [r for r in result if r.get("target_layer") == layer]
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def determine_refresh_type(workspace_id, dataset_name, full_refresh_weekday=0):
    """
    Determine whether to use full or automatic refresh.
    Avoids repeated full refreshes on the same day when pipeline runs multiple times.
    """
    from datetime import date

    today = date.today()

    # Not the scheduled full refresh day → always automatic
    if today.weekday() != full_refresh_weekday:
        return "automatic"

    # Is the full refresh day – check if already done today
    last = get_last_refresh_status(workspace_id, dataset_name)
    last_end = last.get("endTime", "")[:10]   # "2026-05-19"

    if last_end == str(today) and last.get("status") == "Completed":
        print("  Full refresh already done today – using automatic")
        return "automatic"

    return "full"


def run_sm_refresh(spark, instr, workspace_id, gold_base_path,
                   pipeline_name, workspace_name):
    """
    Core SM refresh logic. Called from sm_refresh_executor in notebook.
    Mirrors run_table_validations() pattern from nb-av01-4-validate.

    Args:
        spark:          SparkSession
        instr:          Instruction dict from instructions.semantic_model
        workspace_id:   SM workspace GUID (resolved from Variable Library)
        gold_base_path: ABFS path to Gold lakehouse Tables
        pipeline_name:  For refresh_metadata logging
        workspace_name: For refresh_metadata logging

    Returns:
        (row_count, source_name, detail)
    """
    from pyspark.sql import Row
    import json

    dataset_name = instr["dataset_name"]
    sm_mode      = instr.get("sm_mode", "import")
    params       = json.loads(instr.get("refresh_params") or "{}")

    refresh_mode         = params.get("refresh_mode",         "scheduled")
    notify_option        = params.get("notify_option",        "NoNotification")
    timeout              = params.get("poll_timeout_seconds", 600)
    full_refresh_weekday = params.get("full_refresh_weekday", 0)

    # Direct Lake - no refresh needed
    if sm_mode == "directlake":
        print(f"  Direct Lake - no refresh needed: {dataset_name}")
        return (1, dataset_name, f"mode={sm_mode}")

    # Determine refresh type from metadata-driven refresh_mode
    if refresh_mode == "always_full":
        refresh_type = "full"
        print("  Mode: always full refresh")

    elif refresh_mode == "always_automatic":
        refresh_type = "automatic"
        print("  Mode: always automatic (incremental)")

    elif refresh_mode == "scheduled":
        if datetime.now().weekday() == full_refresh_weekday:
            refresh_type = "full"
            print(f"  Mode: scheduled - full refresh (weekday {full_refresh_weekday})")
        else:
            refresh_type = "automatic"
            print(f"  Mode: scheduled - automatic (full on weekday {full_refresh_weekday})")

    else:
        raise ValueError(
            f"Unknown refresh_mode: '{refresh_mode}'. "
            f"Valid: 'always_full' | 'always_automatic' | 'scheduled'"
        )

    print(f"  Dataset name  : {dataset_name}")
    print(f"  SM workspace  : {workspace_name}")
    print(f"  Refresh type  : {refresh_type}")
    print(f"  Notify option : {notify_option}")

    # Trigger and poll
    refresh_semantic_model(
        workspace_id  = workspace_id,
        dataset_name  = dataset_name,
        notify_option = notify_option,
        refresh_type  = refresh_type,
        timeout       = timeout
    )

    # Get actual refresh details from API
    last = get_last_refresh_status(workspace_id, dataset_name)

    # Write refresh_metadata to Gold
    spark.createDataFrame([Row(
        dataset_name   = dataset_name,
        refresh_type   = last.get("refreshType", refresh_type),
        refresh_mode   = refresh_mode,
        status         = last.get("status"),
        started_at     = last.get("startTime"),
        completed_at   = last.get("endTime"),
        request_id     = last.get("requestId"),
        pipeline_name  = pipeline_name,
        workspace_name = workspace_name
    )]).write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_base_path + "siva/refresh_metadata")

    print(f"  -> refresh_metadata written to Gold")


    return (1, dataset_name, f"| mode={refresh_mode}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_sm_validation(spark, instr, workspace_id, sm_expectation_lookup):
    """
    Core SM validation logic. Called from sm_validate_executor in notebook.
    Mirrors run_table_validations() pattern from nb-av01-4-validate.
    """
    import json

    dataset_name = instr["dataset_name"]
    validations  = instr["validations"]

    print(f"Validating: {dataset_name} ({len(validations)} checks)")

    tables, relationships, _ = load_sm_metadata(workspace_id, dataset_name)

    errors   = []
    warnings = []
    passed   = 0

    for v in validations:
        check_function = sm_expectation_lookup.get(v["expectation_id"])
        params         = json.loads(v.get("check_params") or "{}")
        severity       = v.get("severity", "error")

        try:
            run_check(
                check_type    = check_function,
                params        = params,
                severity      = severity,
                tables        = tables,
                relationships = relationships,
                workspace_id  = workspace_id,
                dataset_name  = dataset_name
            )
            passed += 1

        except ValueError as e:
            if severity == "warning":
                print(f"    WARNING: {e}")
                warnings.append(str(e))
            else:
                print(f"    ERROR: {e}")
                errors.append(str(e))

    print(f"  Results: {passed} passed, {len(warnings)} warnings, {len(errors)} errors")

    if errors:
        raise ValueError(
            f"SM validation failed for '{dataset_name}': {errors}"
        )

    return (passed, dataset_name, dataset_name)


def load_sm_metadata(workspace_id, dataset_name):
    import sempy.fabric as fabric

    tables_df = fabric.evaluate_dax(
        dataset=dataset_name,
        workspace=workspace_id,
        dax_string="EVALUATE INFO.TABLES()"
    )

    columns_df = fabric.evaluate_dax(
        dataset=dataset_name,
        workspace=workspace_id,
        dax_string="EVALUATE INFO.COLUMNS()"
    )

    rel_df = fabric.evaluate_dax(
        dataset=dataset_name,
        workspace=workspace_id,
        dax_string="EVALUATE INFO.RELATIONSHIPS()"
    )

    # Clean column names: "[ID]" -> "ID"
    tables_df.columns = [c.strip("[]") for c in tables_df.columns]
    columns_df.columns = [c.strip("[]") for c in columns_df.columns]
    rel_df.columns = [c.strip("[]") for c in rel_df.columns]

    table_id_to_name = dict(
        zip(tables_df["ID"], tables_df["Name"])
    )

    column_id_to_name = dict(
        zip(columns_df["ID"], columns_df["ExplicitName"])
    )

    tables = set(tables_df["Name"].dropna().astype(str).tolist())

    relationships = []

    for _, r in rel_df.iterrows():
        from_table = table_id_to_name.get(r["FromTableID"])
        to_table = table_id_to_name.get(r["ToTableID"])

        from_col = column_id_to_name.get(r["FromColumnID"])
        to_col = column_id_to_name.get(r["ToColumnID"])

        if from_table and to_table:
            relationships.append(
                {
                    "from_table": from_table,
                    "to_table": to_table,
                    "from_column": from_col,
                    "to_column": to_col,
                }
            )

    print(f"  Tables found  : {sorted(tables)}")
    print("  Relationships :")
    for rel in relationships:
        print(
            f"    {rel['from_table']}.{rel['from_column']} "
            f"-> {rel['to_table']}.{rel['to_column']}"
        )

    return tables, relationships, dataset_name


def run_check(check_type, params, severity, tables,
              relationships, workspace_id, dataset_name):
    """Run a single SM validation check."""
    import sempy.fabric as fabric

    if check_type == "check_table_exists":
        table_name = params["table_name"]
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found in semantic model")
        print(f"    OK table_exists: {table_name}")
        return 1

    elif check_type == "check_relationship":
        from_table = params["from_table"]
        to_table   = params["to_table"]

        if not any(
            r["from_table"] == from_table and r["to_table"] == to_table
            for r in relationships
        ):
            raise ValueError(
                f"Relationship '{from_table}' -> '{to_table}' not found"
            )

        print(f"    OK relationship: {from_table} -> {to_table}")
        return 1

    elif check_type == "check_row_count":
        table_name = params["table_name"]
        min_rows   = params.get("min_rows", 1)

        dax_table_name = f"'{table_name}'"

        result = fabric.evaluate_dax(
            dataset=dataset_name,
            workspace=workspace_id,
            dax_string=f"""
    EVALUATE
    ROW("Count", COUNTROWS({dax_table_name}))
    """
        )

        row_count = int(result["[Count]"].values[0])

        if row_count < min_rows:
            raise ValueError(
                f"Table '{table_name}' has {row_count:,} rows, "
                f"expected minimum {min_rows:,}"
            )

        print(f"    OK row_count: {table_name} ({row_count:,} >= {min_rows:,})")
        return row_count
    else:
        raise ValueError(f"Unknown check_type: '{check_type}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Maintenance function

# CELL ********************

from datetime import datetime, timezone


def should_run(last_run_at, interval_hours: int) -> bool:
    if last_run_at is None:
        return True

    now = datetime.now(timezone.utc)

    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)

    elapsed_hours = (now - last_run_at).total_seconds() / 3600

    return elapsed_hours >= interval_hours


def run_table_maintenance_from_settings(
    spark,
    settings_table: str,
    environment: str,
    allow_vacuum_zero_hours: bool = False
):
    print("=== Delta maintenance from settings ===")
    print(f"Settings table: {settings_table}")
    print(f"Environment: {environment}")

    rows = spark.sql(f"""
        SELECT *
        FROM {settings_table}
        WHERE is_active = true
    """).collect()

    print(f"Fant {len(rows)} aktive maintenance settings")

    for row in rows:
        catalog_name = row["catalog_name"]
        lakehouse_name = row["lakehouse_name"]
        schema_name = row["schema_name"]
        table_name = row["table_name"]

        full_table_name = (
            f"`{catalog_name}`.`{lakehouse_name}`."
            f"`{schema_name}`.`{table_name}`"
        )

        print("")
        print(f"Behandler: {full_table_name}")

        # -----------------------------
        # OPTIMIZE
        # -----------------------------
        if row["optimize_enabled"]:
            if should_run(row["last_optimize_at"], row["optimize_interval_hours"]):
                try:
                    spark.sql(f"OPTIMIZE {full_table_name}")
                    print("  ✅ OPTIMIZE kjørt")

                    spark.sql(f"""
                        UPDATE {settings_table}
                        SET last_optimize_at = current_timestamp()
                        WHERE setting_id = '{row["setting_id"]}'
                    """)

                except Exception as e:
                    print(f"  ⚠️ OPTIMIZE feilet: {e}")
            else:
                print("  OPTIMIZE hoppet over - ikke tid ennå")

        # -----------------------------
        # VACUUM
        # -----------------------------
        if row["vacuum_enabled"]:
            retention_hours = row["vacuum_retention_hours"]

            if retention_hours == 0 and not allow_vacuum_zero_hours:
                print("  ⚠️ VACUUM 0 HOURS blokkert")
                continue

            if should_run(row["last_vacuum_at"], row["vacuum_interval_hours"]):
                try:
                    spark.sql(
                        f"VACUUM {full_table_name} "
                        f"RETAIN {retention_hours} HOURS"
                    )

                    print(f"  ✅ VACUUM kjørt, retain {retention_hours} timer")

                    spark.sql(f"""
                        UPDATE {settings_table}
                        SET last_vacuum_at = current_timestamp()
                        WHERE setting_id = '{row["setting_id"]}'
                    """)

                except Exception as e:
                    print(f"  ⚠️ VACUUM feilet: {e}")
            else:
                print("  VACUUM hoppet over - ikke tid ennå")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
