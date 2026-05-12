# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-api-tools-sharepoint
#  **Purpose**: SharePoint helper functions via Microsoft Graph API.
#  **Usage**: `%run nb-av01-api-tools-sharepoint`
#  **Dependencies**: `nb-av01-generic-functions` (requests, json, msal, notebookutils,
#    spark, SPN_TENANT_ID, SPN_CLIENT_ID, get_watermark, update_watermark)
# 
#  **Autentisering**: OAuth 2.0 Client Credentials (SPN) via Microsoft Graph.
#    SPN må ha `Sites.Read.All` + `Files.Read.All` på Graph (Application permissions).
#    Scope: `https://graph.microsoft.com/.default`
# 
#  **Kilder**:
#    - Liste : `https://sivasf.sharepoint.com/sites/S-Data/data-meldingslogg`
#    - Excel : `Regnskapbedrifter.xlsx` (rot i dokumentbiblioteket)
# 
#  **Functions**:
#  - `ingest_sharepoint_list()` - Entry-point for liste (metadata dispatch)
#  - `ingest_sharepoint_excel()` - Entry-point for Excel (metadata dispatch)
#  - `fetch_list_items_paginated()` - Graph paginering via @odata.nextLink
#  - `fetch_excel_file()` - Last ned med ETag-sjekk (304 Not Modified)
#  - `parse_excel_to_records()` - Excel bytes → liste av dicts
#  - `_get_sharepoint_token()` - OAuth via MSAL (Graph scope)
# 
#  **Watermark-strategi**:
#    - Liste : `datetime` – lastModifiedDateTime fra siste item
#    - Excel  : `etag`    – HTTP ETag-header fra nedlasting


# PARAMETERS CELL ********************

TEST_MODE = globals().get('TEST_MODE', True)

print(TEST_MODE)  # Skriver True eller False?

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# TEST-MODUS – kjøres standalone uten orchestrator
# Kobler direkte til Delta lakehouse og skriver til Files
# =============================================================================

import time
import os
from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
import requests
import msal
import json
import json as _json

if TEST_MODE:

    # ── SPN-credentials for SharePoint Graph API ──────────────────────────────
    SPN_TENANT_ID     = ""
    SPN_CLIENT_ID     = ""
    SPN_CLIENT_SECRET = ""   # ← lim inn client secret

    # ── Variable Library og infrastruktur ─────────────────────────────────────
    variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

    RAW_BASE_PATH = (
        f"abfss://{variables.LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com"
        f"/{variables.BRONZE_LH_NAME}.Lakehouse/Files/"
    )

    def get_watermark(spark, source_id: int, endpoint_path: str) -> dict | None:
        df = spark.sql(f"""
            SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
            WHERE source_id = {source_id}
            AND endpoint_path = '{endpoint_path}'
        """)
        rows = [r.asDict() for r in df.collect()]
        return rows[0] if rows else None

    def update_watermark(spark, source_id: int, endpoint_path: str,
                         watermark_date: str = None, watermark_id: int = None):
        schema = StructType([
            StructField("source_id",      IntegerType(), nullable=False),
            StructField("endpoint_path",  StringType(),  nullable=False),
            StructField("watermark_date", StringType(),  nullable=True),
            StructField("watermark_id",   IntegerType(), nullable=True),
            StructField("updated_at",     TimestampType(), nullable=True),
        ])
        df = spark.createDataFrame([(
            source_id, endpoint_path, watermark_date, watermark_id, datetime.now()
        )], schema)
        DeltaTable.forName(spark, "`av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store") \
            .alias("target") \
            .merge(df.alias("source"),
                "target.source_id = source.source_id AND target.endpoint_path = source.endpoint_path") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"  -> Watermark oppdatert: date={watermark_date}, id={watermark_id}")

    def write_to_landing_zone(items: list, base_path: str, landing_path: str) -> int:
        item_count  = len(items)
        file_path   = f"{base_path}{landing_path}{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_data = {"items": items}
        notebookutils.fs.put(file_path, _json.dumps(output_data, ensure_ascii=False, indent=2), overwrite=True)
        print(f"  -> Skrevet {item_count} items → {file_path}")
        return item_count

    def sharepoint_cleanup(spark, raw_base_path: str, reset_watermark: bool = True):
        """
        Nullstiller SharePoint testmiljø:
        - Sletter JSON-filer fra sharepoint/meldingslogg/ og sharepoint/regnskapbedrifter/
        - Nullstiller watermark til startposisjon

        Args:
            reset_watermark: True  = nullstill til startposisjon
                             False = behold watermark som den er
        """
        print("=== SharePoint Cleanup ===")

        # ── 1. Slett Files ────────────────────────────────────────────────────
        for folder in ["sharepoint/meldingslogg/", "sharepoint/regnskapbedrifter/"]:
            try:
                files = [f for f in notebookutils.fs.ls(f"{raw_base_path}{folder}")
                         if f.name.endswith(".json")]
                for f in files:
                    notebookutils.fs.rm(f.path)
                print(f"  {folder}: slettet {len(files)} filer")
            except Exception:
                print(f"  {folder}: tom eller finnes ikke")

        # ── 2. Nullstill watermark ────────────────────────────────────────────
        if reset_watermark:
            schema = StructType([
                StructField("source_id",      IntegerType(), nullable=False),
                StructField("endpoint_path",  StringType(),  nullable=False),
                StructField("watermark_date", StringType(),  nullable=True),
                StructField("watermark_id",   IntegerType(), nullable=True),
                StructField("updated_at",     TimestampType(), nullable=True),
            ])
            df_reset = spark.createDataFrame([
                (3, 'data-meldingslogg',      '2026-03-18T00:00:00Z', None, None),
                (4, 'Regnskapbedrifter.xlsx',  None,                   None, None),
            ], schema)
            DeltaTable.forName(spark, "`av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store") \
                .alias("target") \
                .merge(df_reset.alias("source"),
                    "target.source_id = source.source_id "
                    "AND target.endpoint_path = source.endpoint_path") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            print("  Meldingslogg watermark: 2026-03-18T00:00:00Z ✓")
            print("  Regnskapbedrifter watermark: NULL (tvinger full nedlasting) ✓")
        else:
            print("  Watermark beholdt som den er")

        # ── Vis watermark ─────────────────────────────────────────────────────
        for sid, ep in [(3, 'data-meldingslogg'), (4, 'Regnskapbedrifter.xlsx')]:
            wm = spark.sql(f"""
                SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
                WHERE source_id = {sid} AND endpoint_path = '{ep}'
            """).first()
            if wm:
                print(f"  Watermark source_id={sid}: date={wm['watermark_date']}")

        print("=== Cleanup ferdig ===")

    print(f"TEST-MODUS aktivert")
    print(f"  RAW_BASE_PATH: {RAW_BASE_PATH}")
    print(f"  Fyll inn SPN_CLIENT_SECRET og kjør neste celle")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import io

# ── SharePoint konfigurasjon ──────────────────────────────────────────────────

SP_SITE_URL = "https://sivasf.sharepoint.com/sites/S-Data"

SP_HEADERS = {
    "Accept":       "application/json",
    "Content-Type": "application/json",
}


# ── Authentication ────────────────────────────────────────────────────────────

def _get_sharepoint_token() -> str:
    """
    Henter Graph-token via globale SPN-credentials.

    Bruker SPN_TENANT_ID, SPN_CLIENT_ID og SPN_CLIENT_SECRET som settes av
    set_spn_credentials() i orchestrator-notebooken (fra pipeline-parametere).
    Samme SPN brukes til Key Vault-tilgang – ingen separate SharePoint-secrets
    nødvendig i Key Vault.

    Krever at SPN har følgende Graph-tillatelser (Application, admin consent):
      - Sites.Read.All
      - Files.Read.All

    Returns:
        Bearer access token for Microsoft Graph
    """
    app = msal.ConfidentialClientApplication(
        SPN_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{SPN_TENANT_ID}",
        client_credential=SPN_CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise Exception(f"SharePoint token-feil: {result.get('error_description')}")
    return result["access_token"]


def _sp_auth_headers(token: str) -> dict:
    """Bygger HTTP-headere med Bearer token for Graph API."""
    return {**SP_HEADERS, "Authorization": f"Bearer {token}"}


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _sp_request_with_retry(url, max_retries=3, base_delay=1.0, **kwargs):
    """
    Make HTTP GET request with retry and exponential backoff for transient errors.

    Args:
        url: Request URL
        max_retries: Maximum retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 1.0)
        **kwargs: Additional arguments passed to requests.get

    Returns:
        Response object

    Raises:
        requests.HTTPError: After all retries exhausted
    """
    retryable_codes = {429, 500, 502, 503}
    for attempt in range(max_retries + 1):
        response = requests.get(url, **kwargs)
        if response.status_code not in retryable_codes or attempt == max_retries:
            if response.status_code != 304:
                response.raise_for_status()
            return response

        delay = base_delay * (2 ** attempt)
        print(f"  -> HTTP {response.status_code}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
        time.sleep(delay)


# ── Graph helper-funksjoner ───────────────────────────────────────────────────

def _get_site_id(headers: dict, site_url: str = SP_SITE_URL) -> str:
    """
    Henter Graph site_id for SharePoint-siten.
    Caches i context for å unngå gjentatte kall.
    """
    from urllib.parse import urlparse
    parsed = urlparse(site_url)
    host   = parsed.netloc
    path   = parsed.path
    url    = f"https://graph.microsoft.com/v1.0/sites/{host}:{path}"
    resp   = _sp_request_with_retry(url, headers=headers)
    return resp.json()["id"]


def fetch_list_items_paginated(site_id: str, list_name: str, headers: dict,
                                params: dict = None) -> list:
    """
    Henter alle items fra en SharePoint-liste via Microsoft Graph med paginering.

    Tilsvarer fetch_with_pagination() fra YouTube-mønsteret.
    Bruker @odata.nextLink for neste side.

    Args:
        site_id:    Graph site ID (fra _get_site_id)
        list_name:  Listenavn eller GUID fra metadata
        headers:    HTTP-headere inkl. Authorization
        params:     Ekstra OData-parametere ($filter, $select, $top, $orderby)

    Returns:
        Liste med alle list item-dicts
    """
    base_params = {"$expand": "fields", "$top": 5000}
    if params:
        base_params.update(params)

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_name}/items"

    all_items  = []
    page_count = 0

    while url:
        response = _sp_request_with_retry(url, headers=headers, params=base_params)
        data     = response.json()
        all_items.extend(data.get("value", []))
        page_count += 1
        url        = data.get("@odata.nextLink")
        base_params = {}
        if url:
            time.sleep(0.2)

    print(f"  Fetched {len(all_items)} list items ({page_count} sider)")
    return all_items


def fetch_excel_file(site_id: str, file_path: str, headers: dict,
                     etag: str = None) -> tuple:
    """
    Laster ned Excel-fil fra SharePoint via Microsoft Graph.

    Bruker If-None-Match (ETag) for å unngå nedlasting hvis filen er uendret.

    Args:
        site_id:   Graph site ID
        file_path: Server-relativ sti til filen (f.eks. "Regnskapbedrifter.xlsx")
        headers:   HTTP-headere inkl. Authorization
        etag:      ETag fra forrige kjøring. None = alltid last ned.

    Returns:
        (content_bytes, ny_etag) eller (None, None) hvis 304 Not Modified
    """
    download_headers = headers.copy()
    if etag:
        download_headers["If-None-Match"] = etag

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"


    response = _sp_request_with_retry(url, headers=download_headers)

    if response.status_code == 304:
        print("  -> Ingen endringer siden sist (ETag match) – hopper over")
        return None, None

    ny_etag = response.headers.get("ETag")

    
    print(f"  -> Excel lastet ned: {len(response.content) / 1e6:.1f} MB")
    return response.content, ny_etag


def parse_excel_to_records(content: bytes, sheet_name=0, header_row: int = 0) -> list:
    """
    Parser Excel-bytes til liste med dicts (én dict per rad).

    Args:
        content:    Excel-filinnhold som bytes
        sheet_name: Arknavn eller indeks (0 = første ark)
        header_row: Radnummer for kolonneoverskrifter (0-indeksert)

    Returns:
        Liste med dicts der nøkler er kolonnenavn
    """
    import pandas as pd
    df = pd.read_excel(
        io.BytesIO(content),
        engine='openpyxl',
        sheet_name=sheet_name,
        header=header_row,
        dtype=str,
        keep_default_na=False,
    )
    df      = df.dropna(how="all")
    records = df.to_dict(orient="records")
    print(f"  -> Excel parset: {len(records)} rader, {len(df.columns)} kolonner")
    return records


def _strip_year_suffix(df):
    """
    Fjerner årstall-suffiks fra kolonnenavn generelt.

    'Sum salgsinntekter, 2022' → 'Sum salgsinntekter'
    'Årstall'                  → 'Årstall'  (uendret)

    Aktiveres via request_params: {"strip_year_suffix": true}
    Nyttig for Excel-filer der årstall er inkludert i kolonnenavn
    men finnes som en egen kolonne (f.eks. Årstall-kolonnen).
    """
    import re
    rename_map = {
        col: re.sub(r',\s*\d{4}$', '', col).strip()
        for col in df.columns
    }
    return df.rename(columns=rename_map)


# ── Ingest entry-points (registrert i metadata.source_store) ─────────────────

def ingest_sharepoint_list(source_meta: dict, instr: dict, api_key: str, context: dict) -> list:
    """
    SharePoint List ingestion entry-point via Microsoft Graph API.

    Henter alle items fra en SharePoint-liste med watermark-støtte.
    Watermark basert på lastModifiedDateTime (datetime) i watermark_store.

    Tilsvarer ingest_youtube() / ingest_brreg() – samme signatur.

    Args:
        source_meta: Source metadata dict (base_url=site URL, source_id, ...)
        instr:       Ingestion instruction dict
                     (endpoint_path = listenavn/GUID,
                      request_params = {"$select": "...", "$orderby": "..."})
        api_key:     SPN client secret fra Key Vault
        context:     Shared dict for cross-instruction state

    Returns:
        Liste med list item-dicts klar for write_to_landing_zone()
    """
    source_id  = source_meta["source_id"]
    list_name  = instr["endpoint_path"]
    raw_params = json.loads(instr["request_params"]) if instr.get("request_params") else {}

    # api_key ignoreres – bruker globale SPN_CLIENT_ID/SECRET/TENANT_ID
    token   = _get_sharepoint_token()
    headers = _sp_auth_headers(token)

    # Hent site_id (cache i context for å unngå gjentatte kall)
    if "_sp_site_id" not in context:
        context["_sp_site_id"] = _get_site_id(headers, source_meta["base_url"])
    site_id = context["_sp_site_id"]

    # Watermark – bruker watermark_date (nytt format)
    odata_params = {k: v for k, v in raw_params.items() if k.startswith("$")}
    wm = get_watermark(spark, source_id, list_name)

    if wm and wm.get("watermark_date"):
        wm_date = wm["watermark_date"]
        # Graph API krever millisekunder: 2026-03-31T09:13:19.000Z
        # Normaliser hvis mangler (f.eks. 2026-03-31T09:13:19Z → 2026-03-31T09:13:19.000Z)
        if wm_date.endswith("Z") and "." not in wm_date:
            wm_date = wm_date.replace("Z", ".000Z")
        odata_params["$filter"] = f"fields/Modified ge '{wm_date}'"
        
        print(f"  -> Watermark: {wm_date}")
    else:
        print("  -> Ingen watermark – henter alle items")

    odata_params.setdefault("$orderby", "lastModifiedDateTime asc")

    # Ta checkpoint FØR henting – sikrer ingen gap ved neste inkrementelle kjøring
    checkpoint_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    items = fetch_list_items_paginated(site_id, list_name, headers, odata_params)

    # Sett watermark til checkpoint-tidspunkt (UTC) – ikke siste item
    if items:
        update_watermark(spark, source_id, list_name, watermark_date=checkpoint_ts)
        print(f"  -> Watermark oppdatert: {checkpoint_ts}")

    # Flat ut fields-objektet til top-level for konsistent JSON
    return [{"id": i.get("id"), **i.get("fields", {})} for i in items]


def ingest_sharepoint_excel(source_meta: dict, instr: dict, api_key: str, context: dict) -> list:
    """
    SharePoint Excel ingestion entry-point via Microsoft Graph API.

    Laster ned Excel-fil og parser til records.
    Watermark basert på ETag-header – hopper over hvis filen er uendret (304).

    Args:
        source_meta: Source metadata dict (source_id, ...)
        instr:       Ingestion instruction dict
                     (endpoint_path = server-relativ filsti,
                      request_params = {"sheet_name": 0, "header_row": 0})
        api_key:     SPN client secret fra Key Vault
        context:     Shared dict for cross-instruction state

    Returns:
        Liste med rad-dicts klar for write_to_landing_zone(),
        eller tom liste hvis ingen endringer siden sist (ETag match).
    """
    source_id  = source_meta["source_id"]
    file_path  = instr["endpoint_path"]
    raw_params = json.loads(instr["request_params"]) if instr.get("request_params") else {}

    # api_key ignoreres – bruker globale SPN_CLIENT_ID/SECRET/TENANT_ID
    token   = _get_sharepoint_token()
    headers = _sp_auth_headers(token)

    if "_sp_site_id" not in context:
        context["_sp_site_id"] = _get_site_id(headers, source_meta["base_url"])
    site_id = context["_sp_site_id"]

    # Hent ETag-watermark – lagret i watermark_date (etag som streng)
    wm   = get_watermark(spark, source_id, file_path)
    etag = wm["watermark_date"] if (wm and wm.get("watermark_date")) else None

    if etag:
        print(f"  -> ETag watermark: {etag[:40]}...")
    else:
        print("  -> Ingen ETag watermark – laster alltid ned")

    content, ny_etag = fetch_excel_file(site_id, file_path, headers, etag)

    if content is None:
        return []

    if ny_etag:
        update_watermark(spark, source_id, file_path, watermark_date=ny_etag)
        print("  -> ETag oppdatert")

    sheet_name = raw_params.get("sheet_name", 0)
    header_row = int(raw_params.get("header_row", 0))
    records    = parse_excel_to_records(content, sheet_name, header_row)

    # Strip årstall-suffiks hvis aktivert i request_params
    # 'Sum salgsinntekter, 2022' → 'Sum salgsinntekter'
    # Forutsetter at Årstall-kolonnen allerede finnes i dataene
    if raw_params.get("strip_year_suffix", False) and records:
        import pandas as pd
        df_tmp  = pd.DataFrame(records)
        df_tmp  = _strip_year_suffix(df_tmp)
        records = df_tmp.to_dict(orient="records")
        print(f"  -> Kolonner normalisert: årstall-suffiks fjernet")

    return records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test: Cleanup før test
# =============================================================================

if TEST_MODE:
    sharepoint_cleanup(spark, RAW_BASE_PATH, reset_watermark=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 1: Full henting – ingen watermark filter
# Henter alle items fra meldingslogg og laster ned Excel
# =============================================================================

if TEST_MODE:
    import pandas as pd

    _original_get_watermark = get_watermark
    get_watermark = lambda spark, s, e: None  # Ingen watermark → henter alt

    token   = _get_sharepoint_token()
    headers = _sp_auth_headers(token)
    site_id = _get_site_id(headers, "https://sivasf.sharepoint.com/sites/S-Data")
    print(f"  site_id: {site_id}")

    # Meldingslogg
    print("\n=== Test 1a: Meldingslogg – alle items ===")
    source_meta = {"source_id": 3, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr = {"endpoint_path": "data-meldingslogg", "request_params": '{"$orderby":"lastModifiedDateTime asc"}'}
    context = {"_sp_site_id": site_id}
    items = ingest_sharepoint_list(source_meta, instr, None, context)
    print(f"  Hentet {len(items)} items")
    if items:
        write_to_landing_zone(items, RAW_BASE_PATH, "sharepoint/meldingslogg/")
        required = ["Created", "Orgnr", "Selskapsnavn", "Hendelsesdato", "Meldingstype", "Meldingsinnhold", "URL"]
        missing  = [c for c in required if c not in items[0]]
        print(f"  Manglende kolonner: {missing}" if missing else "  ✓ Alle påkrevde kolonner til stede")

    # Excel
    print("\n=== Test 1b: Regnskapbedrifter – full nedlasting ===")
    source_meta_e = {"source_id": 4, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr_e = {"endpoint_path": "Regnskapbedrifter.xlsx", "request_params": '{"sheet_name":0,"header_row":0,"strip_year_suffix":true}'}
    context_e = {"_sp_site_id": site_id}
    records = ingest_sharepoint_excel(source_meta_e, instr_e, None, context_e)
    print(f"  Hentet {len(records)} rader")
    if records:
        write_to_landing_zone(records, RAW_BASE_PATH, "sharepoint/regnskapbedrifter/")

    get_watermark = _original_get_watermark  # Gjenopprett original

    # Vis watermark etter
    for sid, ep in [(3, 'data-meldingslogg'), (4, 'Regnskapbedrifter.xlsx')]:
        wm = spark.sql(f"""SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
            WHERE source_id = {sid} AND endpoint_path = '{ep}'""").first()
        print(f"  Watermark source_id={sid}: date={wm['watermark_date'] if wm else 'N/A'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 2: Inkrementell – bruker ekte watermark fra Delta
# Forventer ingen/få nye items siden Test 1 nettopp hentet alt
# =============================================================================

if TEST_MODE:
    import pandas as pd

    token   = _get_sharepoint_token()
    headers = _sp_auth_headers(token)
    site_id = _get_site_id(headers, "https://sivasf.sharepoint.com/sites/S-Data")

    print("=== Test 2: Meldingslogg – inkrementell (ekte watermark) ===")
    source_meta = {"source_id": 3, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr = {"endpoint_path": "data-meldingslogg", "request_params": '{"$orderby":"lastModifiedDateTime asc"}'}
    context = {"_sp_site_id": site_id}
    items = ingest_sharepoint_list(source_meta, instr, None, context)
    print(f"  Hentet {len(items)} items (forventer 0 eller få)")
    if items:
        write_to_landing_zone(items, RAW_BASE_PATH, "sharepoint/meldingslogg/")

    print("\n=== Test 2b: Regnskapbedrifter – ETag sjekk ===")
    source_meta_e = {"source_id": 4, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr_e = {"endpoint_path": "Regnskapbedrifter.xlsx", "request_params": '{"sheet_name":0,"header_row":0,"strip_year_suffix":true}'}
    context_e = {"_sp_site_id": site_id}
    records = ingest_sharepoint_excel(source_meta_e, instr_e, None, context_e)
    print(f"  Hentet {len(records)} rader (forventer 0 – ETag uendret)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 3: Inkrementell siste 24 timer – simulert watermark
# =============================================================================

if TEST_MODE:
    import pandas as pd
    from datetime import timedelta

    dato_24t = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    _original_get_watermark = get_watermark
    get_watermark = lambda spark, s, e: (
        {"source_id": s, "endpoint_path": e,
        "watermark_date": dato_24t, "watermark_id": None, "updated_at": datetime.now()}
        if s == 3 else _original_get_watermark(spark, s, e)
    )

    token   = _get_sharepoint_token()
    headers = _sp_auth_headers(token)
    site_id = _get_site_id(headers, "https://sivasf.sharepoint.com/sites/S-Data")

    print(f"=== Test 3: Meldingslogg – inkrementell siste 24t (fra {dato_24t}) ===")
    source_meta = {"source_id": 3, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr = {"endpoint_path": "data-meldingslogg", "request_params": '{"$orderby":"lastModifiedDateTime asc"}'}
    context = {"_sp_site_id": site_id}
    items = ingest_sharepoint_list(source_meta, instr, None, context)
    print(f"  Hentet {len(items)} items")
    if items:
        write_to_landing_zone(items, RAW_BASE_PATH, "sharepoint/meldingslogg/")
        df = pd.DataFrame(items)
        display(df[["Orgnr", "Selskapsnavn", "Meldingstype"]].head(5))

    print("\n=== Test 3b: Regnskapbedrifter – ETag sjekk (samme ETag → ingen nedlasting) ===")
    source_meta_e = {"source_id": 4, "base_url": "https://sivasf.sharepoint.com/sites/S-Data"}
    instr_e = {"endpoint_path": "Regnskapbedrifter.xlsx", "request_params": '{"sheet_name":0,"header_row":0,"strip_year_suffix":true}'}
    context_e = {"_sp_site_id": site_id}
    records = ingest_sharepoint_excel(source_meta_e, instr_e, None, context_e)
    print(f"  Hentet {len(records)} rader (forventer 0 – ETag uendret)")
    if records:
        write_to_landing_zone(records, RAW_BASE_PATH, "sharepoint/regnskapbedrifter/")

    get_watermark = _original_get_watermark  # Gjenopprett original

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test: Cleanup etter test
# =============================================================================

if TEST_MODE:
    sharepoint_cleanup(spark, RAW_BASE_PATH, reset_watermark=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
