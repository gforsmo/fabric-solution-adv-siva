# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-api-tools-brreg
#  **Purpose**: BRREG Enhetsregisteret-specific helper functions for API extraction.
#  **Usage**: `%run nb-av01-api-tools-brreg`
#  **Dependencies**: `nb-av01-generic-functions` (requests, json, notebookutils, spark,
#    write_to_landing_zone, get_watermark, update_watermark, SPN_*)
# 
#  **Functions**:
#  - `ingest_brreg()` - Entry-point handler, registrert som handler_function i metadata.source_store
#  - `hent_oppdateringer()` - Cursor-paginering gjennom alle oppdateringer
#  - `fetch_enheter_enkeltoppslag()` - Enkeltoppslag per orgnr
#  - `flatten_enhet()` - Flatten nested BRREG-objekt til skalarer
#  - `full_load_brreg()` - Full load via DuckDB (kalles direkte, ikke via rammeverket)
# 
#  **Watermark-strategi**:
#    - Etter full load : ISO-8601 timestamp → bruker `dato`-parameter
#    - Etter inkrementell: oppdateringsid (int) → bruker `oppdateringsid`-parameter
#    - Lagres i `metadata.watermark_store` via `update_watermark()` fra generic functions

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
# Kobler direkte til Fabric SQL og skriver til Files i data lakehouse
# =============================================================================

import time
import os
from datetime import datetime, timezone
from delta.tables import DeltaTable

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
import requests
import json as _json

if TEST_MODE:


    # ── Variable Library og infrastruktur ─────────────────────────────────────
    variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

    METADATA_DB_URL = (
        f"jdbc:sqlserver://{variables.METADATA_SERVER}.database.fabric.microsoft.com:1433"
        f";database={variables.METADATA_DB};encrypt=true;trustServerCertificate=false"
    )

    RAW_BASE_PATH = (
        f"abfss://{variables.LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com"
        f"/{variables.BRONZE_LH_NAME}.Lakehouse/Files/"
    )

    BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
    ingestion_context = {"raw_base_path": RAW_BASE_PATH}

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
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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

    # ── write_to_landing_zone – skriver til Files i data lakehouse ────────────
    def write_to_landing_zone(items: list, base_path: str, landing_path: str) -> int:
        item_count  = len(items)
        file_path   = f"{base_path}{landing_path}{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_data = {"items": items} if item_count > 1 else (items[0] if items else {})
        notebookutils.fs.put(file_path, _json.dumps(output_data, ensure_ascii=False, indent=2), overwrite=True)
        print(f"  -> Skrevet {item_count} items → {file_path}")
        return item_count

        
    def brreg_cleanup(spark, raw_base_path: str, reset_watermark: bool = True):
        """
        Nullstiller BRREG testmiljø:
        - Sletter alle JSON/JSONL-filer fra brreg/enheter/
        - Nullstiller watermark til NULL → neste auto-kjøring trigger full load

        Args:
            reset_watermark: True  = nullstill til NULL (full load neste gang)
                            False = behold watermark som den er
        """
        from delta.tables import DeltaTable
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        print("=== BRREG Cleanup ===")

        # ── 1. Slett Files ────────────────────────────────────────────────────────
        for folder in ["brreg/enheter/", "brreg/enheter/fullload/", "brreg/enheter/incremental/"]:
            try:
                files = [f for f in notebookutils.fs.ls(f"{raw_base_path}{folder}")
                        if f.name.endswith((".json", ".jsonl"))]
                for f in files:
                    notebookutils.fs.rm(f.path)
                print(f"  {folder}: slettet {len(files)} filer")
            except Exception:
                print(f"  {folder}: tom eller finnes ikke")

        # ── 2. Nullstill watermark ────────────────────────────────────────────────
        if reset_watermark:
            schema = StructType([
                StructField("source_id",      IntegerType(), nullable=False),
                StructField("endpoint_path",  StringType(),  nullable=False),
                StructField("watermark_date", StringType(),  nullable=True),
                StructField("watermark_id",   IntegerType(), nullable=True),
                StructField("updated_at",     TimestampType(), nullable=True),
            ])
            df_reset = spark.createDataFrame([
                (2, '/oppdateringer/enheter', None, None, None),  # updated_at = NULL
            ], schema)
 
            DeltaTable.forName(spark, "`av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store") \
                .alias("target") \
                .merge(df_reset.alias("source"),
                    "target.source_id = source.source_id "
                    "AND target.endpoint_path = source.endpoint_path") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            print("  Watermark nullstilt ✓")
        else:
            print("  Watermark beholdt som den er")

        wm = spark.sql("""
            SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
            WHERE source_id = 2 AND endpoint_path = '/oppdateringer/enheter'
        """).first()
        if wm:
            print(f"  Watermark nå: date={wm['watermark_date']}, id={wm['watermark_id']}")

    print("=== Cleanup ferdig ===")
    print(f"TEST-MODUS aktivert")
    print(f"  RAW_BASE_PATH : {RAW_BASE_PATH}")
    print(f"  BRREG_BASE_URL: {BRREG_BASE_URL}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# ── BRREG API konfigurasjon ───────────────────────────────────────────────────

BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"

BRREG_HEADERS = {
    "Accept": "application/vnd.brreg.enhetsregisteret.enhet.v2+json;charset=UTF-8"
}
BRREG_HEADERS_OPPDATERING = {
    "Accept": "application/vnd.brreg.enhetsregisteret.oppdatering.enhet.v1+json;charset=UTF-8"
}
BRREG_HEADERS_LASTNED = {
    "Accept": "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"
}

BRREG_OPPDATERING_PAGE_SIZE = 10_000

ENDRINGSTYPE_UPSERT = {"Ny", "Endring", "Ukjent"}
ENDRINGSTYPE_SLETT  = {"Sletting", "Fjernet"}


# ── HTTP helper ───────────────────────────────────────────────────────────────



def _brreg_request_with_retry(url, max_retries=3, base_delay=1.0, **kwargs):
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
            response.raise_for_status()
            return response
        delay = base_delay * (2 ** attempt)
        print(f"  -> HTTP {response.status_code}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
        time.sleep(delay)


# ── BRREG helper-funksjoner ───────────────────────────────────────────────────

def extract_organisasjonsnumre(oppdateringer: list) -> list:
    """
    Extract organisasjonsnummer from oppdateringer response.
    Tilsvarer extract_video_ids() fra YouTube-mønsteret.
    """
    if not oppdateringer:
        return []
    return [str(item["organisasjonsnummer"]) for item in oppdateringer if item.get("organisasjonsnummer")]

def fetch_enheter_batch(base_url: str, orgnr_liste: list, batch_size: int = 100) -> list:
    """
    Henter full enhet-data i batch via ?organisasjonsnummer=111,222,333
    22 391 enkeltoppslag → 224 batch-kall

    Args:
        base_url:   BRREG API base URL
        orgnr_liste: Liste med organisasjonsnummer å hente
        batch_size: Antall orgnr per kall (maks ~100 er trygt)

    Returns:
        Liste med fulle enhet-dicts
    """
    if not orgnr_liste:
        return []

    enheter      = []
    total        = len(orgnr_liste)
    antall_batch = (total + batch_size - 1) // batch_size

    for i in range(0, total, batch_size):
        batch    = orgnr_liste[i : i + batch_size]
        batch_nr = i // batch_size + 1

        response = _brreg_request_with_retry(
            f"{base_url}/enheter",
            params={
                "organisasjonsnummer" : ",".join(batch),
                "size"                : batch_size,
            },
            headers=BRREG_HEADERS
        )
        data          = response.json()
        batch_enheter = data.get("_embedded", {}).get("enheter", [])
        enheter.extend(batch_enheter)

        if batch_nr % 10 == 0 or batch_nr == antall_batch:
            print(f"  -> Batch {batch_nr}/{antall_batch} "
                  f"({len(enheter):,}/{total:,} enheter)")

        time.sleep(0.05)

    print(f"  Fetched {len(enheter):,} enheter totalt "
          f"({total - len(enheter):,} ikke funnet/slettet)")
    return enheter

def fetch_enheter_enkeltoppslag(base_url, orgnr_liste, headers=None):
    """
    DEPRECATED – bruk fetch_enheter_batch() i stedet.
    22 391 kall × 2 sek = 12 timer vs batch: 224 kall × 0.05 sek = 11 sek
    Beholdt for referanse.
    """
    raise DeprecationWarning(
        "fetch_enheter_enkeltoppslag er for tregt. Bruk fetch_enheter_batch()."
    )

def fetch_enheter_enkeltoppslag_old(base_url: str, orgnr_liste: list, headers: dict = None) -> list:
    """
    Fetch full enhet objects one by one for a list of organisasjonsnummer.
    BRREG har ikke batch-endepunkt – henter sekvensielt.
    Tilsvarer fetch_video_stats_batched() fra YouTube-mønsteret.
    """
    if not base_url:
        raise ValueError("base_url is required")
    if not orgnr_liste:
        return []

    hdrs    = headers or BRREG_HEADERS
    enheter = []
    total   = len(orgnr_liste)

    for i, orgnr in enumerate(orgnr_liste, 1):
        try:
            response = _brreg_request_with_retry(f"{base_url}/enheter/{orgnr}", headers=hdrs)
            enheter.append(response.json())
        except requests.HTTPError as exc:
            if exc.response.status_code in (404, 410):
                print(f"  Enhet {orgnr} slettet/fjernet (HTTP {exc.response.status_code})")
            else:
                raise
        if i % 500 == 0:
            print(f"  -> Enkeltoppslag {i}/{total}")
        time.sleep(0.05)

    print(f"  Fetched {len(enheter)} enheter total ({total - len(enheter)} slettet/fjernet)")
    return enheter


def hent_oppdateringer(fra_oppdateringsid: int = None, dato: str = None) -> list:
    """
    Henter oppdateringer fra /api/oppdateringer/enheter med cursor-paginering.

    Følger BRREG sin anbefalte bruk:
      - Første kjøring etter full load: bruk dato (ISO-8601 timestamp)
      - Påfølgende kjøringer          : bruk oppdateringsid (siste_id + 1)

    size=10 000 per request (maks tillatt for dette endepunktet).
    includeChanges=true gir endringstype: Ny/Endring/Ukjent → upsert,
    Sletting/Fjernet → slett fra Delta.
    """
    alle    = []
    kall_nr = 0
    url     = f"{BRREG_BASE_URL}/oppdateringer/enheter"

    params = {
        "size"           : BRREG_OPPDATERING_PAGE_SIZE,
        "sort"           : "id,ASC",
        "includeChanges" : "true",
    }
    if fra_oppdateringsid is not None:
        params["oppdateringsid"] = fra_oppdateringsid + 1
    elif dato:
        params["dato"] = dato

    while True:
        kall_nr += 1
        resp    = _brreg_request_with_retry(url, params=params, headers=BRREG_HEADERS_OPPDATERING)
        data    = resp.json()
        innslag = data.get("_embedded", {}).get("oppdaterteEnheter", [])

        if not innslag:
            print(f"  Kall {kall_nr}: ingen oppdateringer – ferdig")
            break

        alle.extend(innslag)
        siste_id = max(o["oppdateringsid"] for o in innslag)
        print(f"  Kall {kall_nr}: {len(innslag):,} oppdateringer (t.o.m. id {siste_id}, totalt {len(alle):,})")

        if len(innslag) < BRREG_OPPDATERING_PAGE_SIZE:
            break

        params = {
            "size"           : BRREG_OPPDATERING_PAGE_SIZE,
            "sort"           : "id,ASC",
            "includeChanges" : "true",
            "oppdateringsid" : siste_id + 1,
        }

    print(f"  Fetched {len(alle):,} oppdateringer totalt ({kall_nr} API-kall)")
    return alle


def flatten_enhet(e: dict) -> dict:
    """Flater ut nøstede BRREG-felt til skalarer for Spark."""
    def safe(obj, *keys, default=None):
        for k in keys:
            if not isinstance(obj, dict):
                return default
            obj = obj.get(k, default)
        return obj

    return {
        "organisasjonsnummer"                       : e.get("organisasjonsnummer"),
        "navn"                                      : e.get("navn"),
        "organisasjonsform_kode"                    : safe(e, "organisasjonsform", "kode"),
        "organisasjonsform_beskrivelse"             : safe(e, "organisasjonsform", "beskrivelse"),
        "registreringsdatoEnhetsregisteret"         : e.get("registreringsdatoEnhetsregisteret"),
        "stiftelsesdato"                            : e.get("stiftelsesdato"),
        "konkursdato"                               : e.get("konkursdato"),
        "underAvviklingDato"                        : e.get("underAvviklingDato"),
        "antallAnsatte"                             : e.get("antallAnsatte"),
        "harRegistrertAntallAnsatte"                : e.get("harRegistrertAntallAnsatte"),
        "registrertIMvaregisteret"                  : e.get("registrertIMvaregisteret"),
        "registrertIForetaksregisteret"             : e.get("registrertIForetaksregisteret"),
        "registrertIFrivillighetsregisteret"        : e.get("registrertIFrivillighetsregisteret"),
        "registrertIPartiregisteret"                : e.get("registrertIPartiregisteret"),
        "konkurs"                                   : e.get("konkurs"),
        "underAvvikling"                            : e.get("underAvvikling"),
        "underTvangsavviklingEllerTvangsopplosning" : e.get("underTvangsavviklingEllerTvangsopplosning"),
        "erIKonsern"                                : e.get("erIKonsern"),
        "overordnetEnhet"                           : e.get("overordnetEnhet"),
        "hjemmeside"                                : e.get("hjemmeside"),
        "epostadresse"                              : e.get("epostadresse"),
        "telefon"                                   : e.get("telefon"),
        "mobil"                                     : e.get("mobil"),
        "maalform"                                  : e.get("maalform"),
        "sisteInnsendteAarsregnskap"                : e.get("sisteInnsendteAarsregnskap"),
        "naeringskode1_kode"                        : safe(e, "naeringskode1", "kode"),
        "naeringskode1_beskrivelse"                 : safe(e, "naeringskode1", "beskrivelse"),
        "naeringskode2_kode"                        : safe(e, "naeringskode2", "kode"),
        "naeringskode2_beskrivelse"                 : safe(e, "naeringskode2", "beskrivelse"),
        "naeringskode3_kode"                        : safe(e, "naeringskode3", "kode"),
        "naeringskode3_beskrivelse"                 : safe(e, "naeringskode3", "beskrivelse"),
        "institusjonellSektorkode_kode"             : safe(e, "institusjonellSektorkode", "kode"),
        "institusjonellSektorkode_beskrivelse"      : safe(e, "institusjonellSektorkode", "beskrivelse"),
        "forretningsadresse_kommune"                : safe(e, "forretningsadresse", "kommune"),
        "forretningsadresse_kommunenummer"          : safe(e, "forretningsadresse", "kommunenummer"),
        "forretningsadresse_postnummer"             : safe(e, "forretningsadresse", "postnummer"),
        "forretningsadresse_poststed"               : safe(e, "forretningsadresse", "poststed"),
        "forretningsadresse_landkode"               : safe(e, "forretningsadresse", "landkode"),
        "kapital_belop"                             : safe(e, "kapital", "belop"),
        "kapital_valuta"                            : safe(e, "kapital", "valuta"),
        "kapital_antallAksjer"                      : safe(e, "kapital", "antallAksjer"),
        "underlagtLovgivningLandKode"               : e.get("underlagtLovgivningLandKode"),
        "underlagtLovgivningLand"                   : e.get("underlagtLovgivningLand"),
        "lastet_tidspunkt"                          : datetime.now(timezone.utc).isoformat(),
    }


# ── Full load – kun til Files (steg 1 av 2 i metadata-drevet flyt) ────────────
# Steg 2 (Files → Delta) er en separat loading-notebook.

DUCKDB_FLATTEN_SQL = """
    SELECT
        organisasjonsnummer::VARCHAR                      AS organisasjonsnummer,
        navn,
        organisasjonsform.kode                            AS organisasjonsform_kode,
        organisasjonsform.beskrivelse                     AS organisasjonsform_beskrivelse,
        registreringsdatoEnhetsregisteret,
        stiftelsesdato, konkursdato, underAvviklingDato,
        antallAnsatte, harRegistrertAntallAnsatte,
        registrertIMvaregisteret, registrertIForetaksregisteret,
        registrertIFrivillighetsregisteret, registrertIPartiregisteret,
        konkurs, underAvvikling, underTvangsavviklingEllerTvangsopplosning,
        erIKonsern, overordnetEnhet, hjemmeside, epostadresse,
        telefon, mobil, maalform, sisteInnsendteAarsregnskap,
        naeringskode1.kode        AS naeringskode1_kode,
        naeringskode1.beskrivelse AS naeringskode1_beskrivelse,
        naeringskode2.kode        AS naeringskode2_kode,
        naeringskode2.beskrivelse AS naeringskode2_beskrivelse,
        naeringskode3.kode        AS naeringskode3_kode,
        naeringskode3.beskrivelse AS naeringskode3_beskrivelse,
        institusjonellSektorkode.kode        AS institusjonellSektorkode_kode,
        institusjonellSektorkode.beskrivelse AS institusjonellSektorkode_beskrivelse,
        forretningsadresse.kommune           AS forretningsadresse_kommune,
        forretningsadresse.kommunenummer     AS forretningsadresse_kommunenummer,
        forretningsadresse.postnummer        AS forretningsadresse_postnummer,
        forretningsadresse.poststed          AS forretningsadresse_poststed,
        forretningsadresse.landkode          AS forretningsadresse_landkode,
        kapital.belop        AS kapital_belop,
        kapital.valuta       AS kapital_valuta,
        kapital.antallAksjer AS kapital_antallAksjer,
        underlagtLovgivningLandKode, underlagtLovgivningLand,
        '{lastet_tidspunkt}' AS lastet_tidspunkt
    FROM read_json('{gz_path}', format='array', compression='gzip')
"""


def download_to_files_brreg(
    raw_base_path: str,
    source_id: int,
    landing_path: str = "brreg/enheter/fullload/",
) -> int:
    """
    Full load av BRREG Enhetsregisteret – laster ned og lagrer i Files.

    Kalles av ingest_brreg() når run_mode er full_load eller full_and_incremental.
    Dette er steg 1 av 2 – loading til Delta-tabell er en separat notebook.

    Fase 1 – Nedlasting: GZ streames til /tmp med progress (~2 min)
    Fase 2 – DuckDB    : GZ → flatten SQL → JSONL på /tmp (~30 sek)
    Fase 3 – Files     : JSONL kopieres til ABFS landing zone
    Fase 4 – Watermark : ISO-8601 timestamp lagres i metadata.watermark_store

    Args:
        raw_base_path: ABFS Files-sti (fra construct_abfs_path)
        source_id:     source_id i metadata.source_store
        landing_path:  Relativ sti under raw_base_path

    Returns:
        Antall enheter lastet ned
    """
    #import subprocess
    #subprocess.run(["pip", "install", "duckdb", "--quiet"], check=True)
    import duckdb

    if not raw_base_path:
        raise ValueError(
            "raw_base_path er None – sett ingestion_context = {'raw_base_path': RAW_BASE_PATH} "
            "i orchestrator FØR %run nb-av01-api-tools-brreg"
        )

    TEMP_GZ    = "/tmp/brreg_enheter.json.gz"
    TEMP_JSONL = "/tmp/brreg_enheter.jsonl"
    LOG_EVERY_MB = 20

    url = f"{BRREG_BASE_URL}/enheter/lastned"

    # ── Fase 1: Nedlasting ────────────────────────────────────────────────────
    print("=== FULL LOAD – FASE 1/3: Nedlasting ===")
    chunk_size    = 1024 * 1024
    log_threshold = LOG_EVERY_MB * chunk_size
    downloaded = last_logged = 0

    with requests.get(url, headers=BRREG_HEADERS_LASTNED, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        total_bytes = int(resp.headers.get("Content-Length", 0))
        print(f"  Filstørrelse: {total_bytes / 1e6:.0f} MB (komprimert)" if total_bytes else "  Filstørrelse: ukjent")
        with open(TEMP_GZ, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded - last_logged >= log_threshold:
                    pst = f" ({downloaded / total_bytes * 100:.0f}%)" if total_bytes else ""
                    print(f"  Nedlastet: {downloaded / 1e6:.0f} MB{pst}")
                    last_logged = downloaded
    print(f"  Ferdig: {downloaded / 1e6:.1f} MB")

    # ── Fase 2: DuckDB GZ → JSONL ────────────────────────────────────────────
    print("=== FULL LOAD – FASE 2/3: DuckDB GZ → JSONL ===")
    lastet_tidspunkt = datetime.now(timezone.utc).isoformat()
    sql = DUCKDB_FLATTEN_SQL.format(gz_path=TEMP_GZ, lastet_tidspunkt=lastet_tidspunkt)

    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{TEMP_JSONL}' (FORMAT JSON, ARRAY false)")
    antall = con.execute(f"SELECT COUNT(*) FROM read_json('{TEMP_JSONL}')").fetchone()[0]
    con.close()
    os.remove(TEMP_GZ)
    print(f"  DuckDB ferdig: {antall:,} rader")

    # ── Fase 3: Kopier JSONL til fullload (lineage) ──────────────────────────
    print("=== FULL LOAD – FASE 3/4: JSONL → fullload (lineage) ===")
    jsonl_dest = f"{raw_base_path}{landing_path}alle_enheter.jsonl"
    notebookutils.fs.cp(f"file://{TEMP_JSONL}", jsonl_dest, recurse=False)
    print(f"  Kopiert → {jsonl_dest}")
    # TEMP_JSONL slettes ETTER fase 4 siden DuckDB trenger den for konvertering

    # ── Fase 4: Konverter JSONL → JSON og legg i loading-folder ──────────────
    # Loading-notebooken følger YouTube-mønsteret: leser nyeste JSON-fil per folder.
    # BRREG full load og inkrementell havner i samme folder – loading skiller ikke.
    # DuckDB konverterer JSONL til JSON array på /tmp (rask, ingen ny nedlasting)
    print("=== FULL LOAD – FASE 4/4: JSONL → JSON (loading-folder) ===")
    TEMP_JSON = "/tmp/brreg_enheter_loading.json"

    con2 = duckdb.connect()
    con2.execute(f"""
        COPY (SELECT * FROM read_json('{TEMP_JSONL}'))
        TO '{TEMP_JSON}'
        (FORMAT JSON, ARRAY true)
    """)
    con2.close()

    # Loading-folder: samme struktur som YouTube og SharePoint
    # Files/brreg/enheter/YYYYMMDD_HHMMSS.json
    loading_base  = landing_path.split("fullload/")[0]   # "brreg/enheter/"
    timestamp     = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_dest     = f"{raw_base_path}{loading_base}{timestamp}.json"
    notebookutils.fs.cp(f"file://{TEMP_JSON}", json_dest, recurse=False)
    os.remove(TEMP_JSONL)   # Slettes nå – etter at DuckDB er ferdig med den
    os.remove(TEMP_JSON)
    print(f"  JSON → {json_dest}")

    # ── Watermark: ISO-8601 timestamp for neste inkrementelle kjøring ─────────
    checkpoint_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    update_watermark(spark, source_id, "/oppdateringer/enheter", watermark_date=checkpoint_ts)
    print(f"  Watermark: {checkpoint_ts}")
    print(f"=== FULL LOAD fullført – {antall:,} enheter → {json_dest} ===")
    return antall


# ── Ingest entry-point ────────────────────────────────────────────────────────


def ingest_brreg(source_meta: dict, instr: dict, api_key: str, context: dict) -> list:
    """
    BRREG Enhetsregisteret ingestion entry-point.

    Tilsvarer ingest_youtube() – samme signatur og context-mekanisme.
    api_key ignoreres (BRREG er åpent API) men beholdes for framework-konsistens.

    Én instruksjonsrad i instructions.ingestion (endpoint=/oppdateringer/enheter).
    Håndterer begge steg internt:
      1. Hent oppdateringer via /oppdateringer/enheter
      2. Enkeltoppslag via /enheter per endret orgnr
      → returnerer flattede enheter klar for write_to_landing_zone()

    run_mode leses fra source_meta (metadata.source_store):
        full_load            – kaller download_to_files_brreg(), returnerer []
        full_and_incremental – kaller download_to_files_brreg(), kjører deretter inkrementell
        incremental          – kun inkrementell via watermark
        auto                 – watermark=NULL → full_load, ellers incremental

    Args:
        source_meta: Source metadata dict (base_url, source_id, run_mode, ...)
        instr:       Ingestion instruction dict (endpoint_path, landing_path, ...)
        api_key:     Ignorert – BRREG krever ingen nøkkel
        context:     Shared dict for cross-instruction state

    Returns:
        Liste med flattede enhetsdicts klar for write_to_landing_zone(), eller [] ved full load
    """
    base_url     = source_meta["base_url"].rstrip("/")
    source_id    = source_meta["source_id"]
    run_mode     = source_meta.get("run_mode", "incremental")

    # ── Bestem effektiv run_mode for auto ──────────────────────────────────────
    if run_mode == "auto":
        wm = get_watermark(spark, source_id, "/oppdateringer/enheter")
        if not wm or (wm.get("watermark_id") is None and not wm.get("watermark_date")):
            print("  -> run_mode=auto: ingen watermark → full_load")
            effective_mode = "full_load"
        else:
            wm_info = wm.get("watermark_id") or wm.get("watermark_date")
            print(f"  -> run_mode=auto: watermark={wm_info!r} → incremental")
            effective_mode = "incremental"
    else:
        effective_mode = run_mode

    # ── Full load ──────────────────────────────────────────────────────────────
    if effective_mode in ("full_load", "full_and_incremental"):
        print(f"  -> Kjører download_to_files_brreg() [run_mode={run_mode}]")
        download_to_files_brreg(
            raw_base_path = context.get("raw_base_path"),
            source_id     = source_id,
            landing_path  = "brreg/enheter/fullload/",
        )
        if effective_mode == "full_load":
            return []
        # full_and_incremental: fortsett til inkrementell

    # ── Inkrementell: hent oppdateringer ──────────────────────────────────────
    wm = get_watermark(spark, source_id, "/oppdateringer/enheter")

    if wm and wm.get("watermark_id") is not None:
        print(f"  -> Watermark id: {wm['watermark_id']}")
        oppdateringer = hent_oppdateringer(fra_oppdateringsid=wm["watermark_id"])
    elif wm and wm.get("watermark_date"):
        print(f"  -> Watermark date: {wm['watermark_date']}")
        oppdateringer = hent_oppdateringer(dato=wm["watermark_date"])
    else:
        print("  -> Ingen watermark – henter alle oppdateringer")
        oppdateringer = hent_oppdateringer()

    if not oppdateringer:
        print("  -> Ingen nye oppdateringer")
        return []

    ny_siste_id = max(o["oppdateringsid"] for o in oppdateringer)
    print(f"  -> {len(oppdateringer):,} oppdateringer (t.o.m. id {ny_siste_id})")

    # ── Skill upsert og slett ─────────────────────────────────────────────────
    orgnr_upsert = []
    orgnr_slett  = []
    for o in oppdateringer:
        orgnr     = str(o["organisasjonsnummer"])
        endringer = o.get("endringer", [])
        etype     = endringer[0].get("endringstype", "Ukjent") if endringer else "Ukjent"
        if etype in ENDRINGSTYPE_SLETT:
            orgnr_slett.append(orgnr)
        else:
            orgnr_upsert.append(orgnr)

    print(f"  -> Upsert: {len(orgnr_upsert):,}  |  Slett: {len(orgnr_slett):,}")
    context["_orgnr_slett"] = orgnr_slett

    # ── Batch-oppslag og flatten ───────────────────────────────────────────────────
    enheter_raw  = fetch_enheter_batch(base_url, orgnr_upsert)
    enheter_flat = [flatten_enhet(e) for e in enheter_raw]

    # ── Oppdater watermark ────────────────────────────────────────────────────
    update_watermark(spark, source_id, "/oppdateringer/enheter", watermark_id=ny_siste_id)
    print(f"  -> Watermark oppdatert: id={ny_siste_id}")

    return enheter_flat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Standalone test – kjøres kun i TEST_MODE
# Kaller ingest_brreg() direkte slik orchestrator gjør det
# Én instruksjonsrad – ingest_brreg håndterer begge steg internt
# =============================================================================

if TEST_MODE:
    import pandas as pd
    from datetime import timedelta

    # ── Watermark-simulering ──────────────────────────────────────────────────
    # Kommenter inn én av disse:

    # 1. Ingen watermark → auto → full load
    # get_watermark = lambda spark, s, e: None

    # 2. watermark_date → inkrementell (-1 time fra nå)
    #get_watermark = lambda spark, s, e: {
    #    "source_id"    : s, "endpoint_path": e,
    #    "watermark_date": (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    #    "watermark_id" : None, "updated_at": datetime.now()}

    # 3. watermark_id → inkrementell N+1
    # get_watermark = lambda spark, s, e: {
    #     "source_id": s, "endpoint_path": e,
    #     "watermark_date": None, "watermark_id": 185432,
    #     "updated_at": datetime.now()}

    # ── Én instruksjon – speiler instructions.ingestion ───────────────────────
    source_meta = {
        "source_id" : 2,
        "base_url"  : BRREG_BASE_URL,
        "run_mode"  : "auto",   # ← endre til full_load / incremental / full_and_incremental
    }

    instr = {
        "endpoint_path" : "/oppdateringer/enheter",
        "landing_path"  : "brreg/enheter/",
        "request_params": None,
    }

    context = {"raw_base_path": RAW_BASE_PATH}

    # ── Kjør ingest_brreg – håndterer alt internt ─────────────────────────────
    print("=== Test: ingest_brreg ===")
    result = ingest_brreg(source_meta, instr, None, context)
    print(f"  Returnerte {len(result)} enheter")

    if result:
        # Skriv til Files – identisk med hva ingest_executor gjør i produksjon
        write_to_landing_zone(result, RAW_BASE_PATH, instr["landing_path"])

        # Vis første 5 enheter
        df = pd.DataFrame(result[:5])
        print(f"\n  Første 5 enheter:")
        display(df[["organisasjonsnummer", "navn", "organisasjonsform_kode"]].head(5))
    else:
        print("  -> [] returnert (full_load eller ingen oppdateringer)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test: Cleanup før test
# 
# =============================================================================

if TEST_MODE:
    # Nullstill før test 1
    brreg_cleanup(spark, RAW_BASE_PATH, reset_watermark=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 1: Full load
# watermark=NULL → auto → full load (~10 min)
# =============================================================================

if TEST_MODE:
    import pandas as pd

    # Test 1 - lagre original først
    _original_get_watermark = get_watermark

    get_watermark = lambda spark, s, e: None  # Tvinger full load

    source_meta = {"source_id": 2, "base_url": BRREG_BASE_URL, "run_mode": "auto"}
    instr       = {"endpoint_path": "/oppdateringer/enheter", "landing_path": "brreg/enheter/", "request_params": None}
    context     = {"raw_base_path": RAW_BASE_PATH}

    print("=== Test 1: Full load ===")
    result = ingest_brreg(source_meta, instr, None, context)
    print(f"  Returnerte {len(result)} enheter")
    if result:
        write_to_landing_zone(result, RAW_BASE_PATH, instr["landing_path"])
    else:
        print("  -> [] – full load skriver direkte til Files")

    wm = spark.sql("""
        SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
        WHERE source_id = 2 AND endpoint_path = '/oppdateringer/enheter'
    """).first()
    print(f"  Watermark etter: date={wm['watermark_date'] if wm else 'N/A'}, id={wm['watermark_id'] if wm else 'N/A'}")

    # Gjenopprett original etter test 1
    get_watermark = _original_get_watermark


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 2: Inkrementell rett etter full load
# Bruker ekte watermark fra Delta → sannsynlig ingen nye oppdateringer
# =============================================================================

if TEST_MODE:
    import pandas as pd

    # Bruker ekte get_watermark fra celle 1 (Delta)

    source_meta = {"source_id": 2, "base_url": BRREG_BASE_URL, "run_mode": "auto"}
    instr       = {"endpoint_path": "/oppdateringer/enheter", "landing_path": "brreg/enheter/", "request_params": None}
    context     = {"raw_base_path": RAW_BASE_PATH}

    print("=== Test 2: Inkrementell rett etter full load ===")
    result = ingest_brreg(source_meta, instr, None, context)
    print(f"  Returnerte {len(result)} enheter")
    if result:
        write_to_landing_zone(result, RAW_BASE_PATH, instr["landing_path"])
        df = pd.DataFrame(result[:5])
        display(df[["organisasjonsnummer", "navn", "organisasjonsform_kode"]].head(5))
    else:
        print("  -> [] – ingen nye oppdateringer siden full load")

    wm = spark.sql("""
        SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
        WHERE source_id = 2 AND endpoint_path = '/oppdateringer/enheter'
    """).first()
    print(f"  Watermark: date={wm['watermark_date'] if wm else 'N/A'}, id={wm['watermark_id'] if wm else 'N/A'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test 3: Inkrementell siste 24 timer
# Simulerer watermark 24 timer tilbake → henter endringer siden i går
# =============================================================================

if TEST_MODE:
    import pandas as pd
    from datetime import timedelta

    dato_24t = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    get_watermark = lambda spark, s, e: {
        "source_id"     : s,
        "endpoint_path" : e,
        "watermark_date": dato_24t,
        "watermark_id"  : None,
        "updated_at"    : datetime.now()
    }

    source_meta = {"source_id": 2, "base_url": BRREG_BASE_URL, "run_mode": "auto"}
    instr       = {"endpoint_path": "/oppdateringer/enheter", "landing_path": "brreg/enheter/", "request_params": None}
    context     = {"raw_base_path": RAW_BASE_PATH}

    print(f"=== Test 3: Inkrementell siste 24 timer (fra {dato_24t}) ===")
    result = ingest_brreg(source_meta, instr, None, context)
    print(f"  Returnerte {len(result)} enheter")
    if result:
        write_to_landing_zone(result, RAW_BASE_PATH, instr["landing_path"])
        df = pd.DataFrame(result[:5])
        display(df[["organisasjonsnummer", "navn", "organisasjonsform_kode"]].head(5))
    else:
        print("  -> [] – ingen oppdateringer siste 24 timer")

    wm = spark.sql("""
        SELECT * FROM `av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store
        WHERE source_id = 2 AND endpoint_path = '/oppdateringer/enheter'
    """).first()
    print(f"  Watermark: date={wm['watermark_date'] if wm else 'N/A'}, id={wm['watermark_id'] if wm else 'N/A'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Test: Cleanup etter test
# 
# =============================================================================

if TEST_MODE:
    # Nullstill før test 1
    brreg_cleanup(spark, RAW_BASE_PATH, reset_watermark=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER = "lh_av01_silver"

TABELLER_MED_CDF_SILVER = [
    f"`{CATALOG}`.`{SILVER}`.brreg.enheter",
    f"`{CATALOG}`.`{SILVER}`.sharepoint.meldingslogg",
]

print("=== Aktiverer CDF på Silver ===")
for tabell in TABELLER_MED_CDF_SILVER:
    spark.sql(f"""
        ALTER TABLE {tabell}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"  ✅ CDF aktivert: {tabell}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
