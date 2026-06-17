# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# =============================================================================
#  schema_drift_handler.py  –  Microsoft Fabric / Synapse PySpark  (v5)
#
#  Forutsetninger (tilgjengelig via %run nb-av01-generic-functions):
#    - METADATA_DB_URL, log_standard(), log_validation()
#    - spark, notebookutils, json, datetime, StructType/Field, etc.
#
#  Teams-varsling:
#    - Bruker samme Power Automate webhook og requests-bibliotek
#      som nb-av01-notify-error
#    - Drift-avvik mappes til eksisterende feilkoder:
#        E005  SCHEMA_ERROR   → manglende/renamed felt       (KRITISK)
#        E004  NULL_VALUE     → required felt får NULL        (ADVARSEL)
#        E003  TYPE_MISMATCH  → type-konflikt i kolonne       (ADVARSEL)
#        E002  NEW_COLUMN     → ukjent nytt felt i filen      (ANMERKNING)
#
#  Logging:
#    - log_drift()      → log.schema_drift  via .mssql()
#    - log_standard()   → log.pipeline_runs (eksisterende, uendret)
# =============================================================================

import re
import json
import requests
from datetime import datetime
from difflib import SequenceMatcher

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, TimestampType, BooleanType
)


# ─────────────────────────────────────────────────────────────────────────────
#  KONFIGURASJON
#  TEAMS_WEBHOOK_URL hentes fra variables (samme som notify-notebooken)
# ─────────────────────────────────────────────────────────────────────────────

RENAME_SIMILARITY = 0.72

# Drift-type → eksisterende feilkode-mapping (fra nb-av01-notify-error)
_DRIFT_TO_ERROR_CODE = {
    "MISSING_COLUMN":  "E005",   # SCHEMA_ERROR  – KRITISK
    "POSSIBLE_RENAME": "E005",   # SCHEMA_ERROR  – KRITISK
    "TYPE_MISMATCH":   "E003",   # TYPE_MISMATCH – ADVARSEL
    "NEW_COLUMN":      "E002",   # NEW_COLUMN    – ANMERKNING
}

# Samme SEVERITY-map som nb-av01-notify-error bruker
_SEVERITY = {
    "E005": {"label": "KRITISK",    "color": "Attention", "icon": "⛔"},
    "E003": {"label": "ADVARSEL",   "color": "Warning",   "icon": "⚠️"},
    "E004": {"label": "ADVARSEL",   "color": "Warning",   "icon": "⚠️"},
    "E002": {"label": "ANMERKNING", "color": "Accent",    "icon": "📋"},
    "E001": {"label": "ANMERKNING", "color": "Accent",    "icon": "📋"},
}

# Fallback-råd for drift-typer (supplerer suggested_action fra detect)
_DRIFT_RÅD = {
    "MISSING_COLUMN":  [
        "Sjekk om kildesystemet har fjernet eller renamed feltet",
        "Delta-kolonnen fylles med NULL inntil mapping er oppdatert",
        "Se log.schema_drift (resolved=0) for berørte kolonner",
    ],
    "POSSIBLE_RENAME": [
        "Oppdater column_mapping: source='gammelt_navn' → source='nytt_navn'",
        "Sett resolved=1 i log.schema_drift etter oppdatering",
        "Verifiser at Delta-historikk er konsistent etter rename",
    ],
    "TYPE_MISMATCH":   [
        "Sjekk om kildesystemet endret datatype på kolonnen",
        "Cast prøves automatisk – sjekk for NULL etter lasting",
        "Oppdater column_mapping type-feltet ved permanent endring",
    ],
    "NEW_COLUMN":      [
        "Vurder om feltet skal legges til i column_mapping",
        "Feltet droppes inntil mapping og Delta-tabell er oppdatert",
        "Koordiner med kildesystem-eier om dette er en bevisst endring",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
#  LOG_DRIFT  –  samme mønster som log_standard / log_validation
# ─────────────────────────────────────────────────────────────────────────────

def log_drift(spark, drift_records, column_mapping_id,
              source_path=None, file_name=None, run_id=None, **ctx):
    """
    Skriver til log.schema_drift via .mssql() – samme mønster som
    log_standard() og log_validation().

    DDL (kjør én gang i SQL Fabric):
    ─────────────────────────────────────────────────────────────
    CREATE TABLE log.schema_drift (
        drift_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id             BIGINT,
        detected_at        DATETIME2,
        column_mapping_id  NVARCHAR(100),
        source_path        NVARCHAR(500),
        file_name          NVARCHAR(255),
        column_name        NVARCHAR(255),
        drift_type         NVARCHAR(50),
        error_code         NVARCHAR(10),
        expected_value     NVARCHAR(255),
        actual_value       NVARCHAR(255),
        severity           NVARCHAR(20),
        suggested_action   NVARCHAR(1000),
        resolved           BIT DEFAULT 0
    );
    ─────────────────────────────────────────────────────────────
    """
    if not drift_records:
        return 0

    schema = StructType([
        StructField("drift_id",           LongType(),      False),  # 0 → IDENTITY
        StructField("run_id",             LongType(),      True),
        StructField("detected_at",        TimestampType(), True),
        StructField("column_mapping_id",  StringType(),    True),
        StructField("source_path",        StringType(),    True),
        StructField("file_name",          StringType(),    True),
        StructField("column_name",        StringType(),    True),
        StructField("drift_type",         StringType(),    True),
        StructField("error_code",         StringType(),    True),
        StructField("expected_value",     StringType(),    True),
        StructField("actual_value",       StringType(),    True),
        StructField("severity",           StringType(),    True),
        StructField("suggested_action",   StringType(),    True),
        StructField("resolved",           BooleanType(),   True),
    ])

    rows = [
        (
            0,
            run_id,
            r.get("detected_at"),
            column_mapping_id,
            source_path,
            file_name,
            r.get("column_name"),
            r.get("drift_type"),
            _DRIFT_TO_ERROR_CODE.get(r.get("drift_type"), "E005"),
            r.get("expected_value"),
            r.get("actual_value"),
            r.get("severity"),
            r.get("suggested_action"),
            False,
        )
        for r in drift_records
    ]

    spark.createDataFrame(rows, schema) \
         .write.mode("append") \
         .option("url", METADATA_DB_URL) \
         .mssql("log.schema_drift")

    high   = sum(1 for r in drift_records if r.get("severity") == "HIGH")
    medium = sum(1 for r in drift_records if r.get("severity") == "MEDIUM")
    print(f"  -> Logged {len(rows)} drift-avvik for '{column_mapping_id}' "
          f"({high} HIGH, {medium} MEDIUM)")
    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
#  TEAMS-VARSLING  –  bruker samme kortstruktur som nb-av01-notify-error
# ─────────────────────────────────────────────────────────────────────────────

def notify_teams_drift(
    drift_report:      dict,
    source_path:       str,
    file_name:         str,
    column_mapping_id: str,
    run_id:            int  = None,
    pipeline_name:     str  = "data_pipeline",
    notebook_name:     str  = "nb-av01-1-load",
    teams_webhook_url: str  = None,
):
    """
    Sender Teams-varsling via Power Automate webhook.

    Kortstrukturen følger nb-av01-notify-error:
      - Ett container-blokk per avvik
      - SEVERITY-fargekoding (Attention/Warning/Accent)
      - Råd-liste under hvert avvik
      - Header med pipeline-navn og tidspunkt

    Henter TEAMS_WEBHOOK_URL fra:
      1. teams_webhook_url-parameteren
      2. variables.TEAMS_WEBHOOK_URL (fra vl-av01-variables)
      3. Global TEAMS_WEBHOOK_URL hvis definert
    Feiler stille – stopper aldri pipeline.
    """
    if not drift_report.get("has_drift"):
        return

    # ── Hent webhook URL ──────────────────────────────────────────────
    url = teams_webhook_url
    if not url:
        try:
            url = variables.TEAMS_WEBHOOK_URL
        except Exception:
            pass
    if not url:
        try:
            url = TEAMS_WEBHOOK_URL
        except Exception:
            print("  [teams] Ingen webhook URL konfigurert – hopper over varsling")
            return

    records      = drift_report["records"]
    high_count   = drift_report["high_count"]
    medium_count = drift_report["medium_count"]
    compatible   = drift_report["compatible"]
    ts_str       = datetime.now().strftime("%Y-%m-%d %H:%M")
    source_short = source_path.split("Files/")[-1].rstrip("/") \
                   if "Files/" in source_path else source_path

    # ── Bygg én container per avvik (maks 6, resten i footer) ────────
    containers = []
    for r in records[:6]:
        error_code = _DRIFT_TO_ERROR_CODE.get(r["drift_type"], "E005")
        sev        = _SEVERITY.get(error_code, _SEVERITY["E005"])
        råd        = _DRIFT_RÅD.get(r["drift_type"], [r["suggested_action"]])

        containers.append({
            "type":    "Container",
            "style":   "default",
            "spacing": "Medium",
            "items": [
                # Ikon + feiltype-linje
                {"type": "ColumnSet", "columns": [
                    {"type": "Column", "width": "auto", "items": [
                        {"type": "TextBlock", "text": sev["icon"], "size": "Large"}
                    ]},
                    {"type": "Column", "width": "stretch", "items": [
                        {"type": "TextBlock",
                         "text":    f"{sev['label']} · {error_code} · {ts_str}",
                         "size":    "Small",
                         "color":   sev["color"],
                         "weight":  "Bolder",
                         "spacing": "None"},
                        {"type":    "TextBlock",
                         "text":    r["suggested_action"][:120],
                         "size":    "Default",
                         "weight":  "Bolder",
                         "wrap":    True,
                         "spacing": "None"},
                        {"type":    "TextBlock",
                         "text":    f"{column_mapping_id} · kolonne: {r['column_name']}",
                         "size":    "Small",
                         "isSubtle": True,
                         "spacing": "None"},
                    ]},
                ]},
                # FactSet
                {"type": "FactSet", "facts": [
                    {"title": "Kilde",      "value": source_short},
                    {"title": "Fil",        "value": file_name},
                    {"title": "Drift-type", "value": r["drift_type"]},
                    {"title": "Forventet",  "value": r["expected_value"]},
                    {"title": "Faktisk",    "value": r["actual_value"]},
                ]},
                # Råd-liste
                {"type": "TextBlock", "text": "🤖 Foreslåtte tiltak",
                 "weight": "Bolder", "size": "Small", "spacing": "Small"},
                *[
                    {"type": "TextBlock", "text": råd_tekst, "wrap": True,
                     "size": "Small", "color": "Default", "spacing": "None"}
                    for råd_tekst in råd
                ],
                # Footer
                {"type": "TextBlock",
                 "text":     f"log.schema_drift → resolved = 0 · run_id = {run_id or '–'}",
                 "size":     "Small",
                 "isSubtle": True,
                 "wrap":     True,
                 "spacing":  "Small"},
            ]
        })

    overflow = len(records) - 6
    status_line = (
        "✅ Pipeline kjørte OK – avvik logget til log.schema_drift"
        if compatible else
        "⛔ Required felt mangler – lastede kolonner vil ha NULL-verdier"
    )

    card_body = [
        # Tittel-linje (identisk format med nb-av01-notify-error)
        {"type": "TextBlock",
         "text": f"Data Pipeline Schema Drift · {ts_str}",
         "size": "Large", "weight": "Bolder", "wrap": True},
        {"type": "TextBlock",
         "text": (f"{notebook_name} oppdaget {len(records)} skjema-avvik "
                  f"({high_count} kritisk, {medium_count} advarsel) "
                  f"i '{column_mapping_id}'"),
         "size": "Small", "isSubtle": True, "spacing": "None"},
        # Status
        {"type": "TextBlock",
         "text":    status_line,
         "size":    "Small",
         "color":   "Good" if compatible else "Attention",
         "spacing": "Small",
         "wrap":    True},
        # Avviks-containere
        *containers,
    ]

    if overflow > 0:
        card_body.append({
            "type":     "TextBlock",
            "text":     f"_…og {overflow} flere avvik. Se log.schema_drift (resolved = 0)._",
            "isSubtle": True,
            "size":     "Small",
            "wrap":     True,
            "spacing":  "Medium",
        })

    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type":    "AdaptiveCard",
                "version": "1.4",
                "body":    card_body,
            }
        }]
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        print(f"  -> Teams drift-varsling sendt: HTTP {resp.status_code}")
    except Exception as e:
        print(f"  [teams] Drift-varsling feilet (ikke kritisk): {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  DETECT_SCHEMA_DRIFT  (uendret logikk fra v4)
# ─────────────────────────────────────────────────────────────────────────────

def detect_schema_drift(
    spark,
    raw_df,
    mapping:           list,
    load_params:       dict,
    source_path:       str,
    file_name:         str,
    column_mapping_id: str,
    run_id:            int = None,
) -> dict:
    """
    Sammenligner JSON-fil mot column_mapping + required_fields.

    Scenariene:
      MISSING_COLUMN   – forventet felt mangler  → E005 KRITISK
      POSSIBLE_RENAME  – sannsynlig rename        → E005 KRITISK
      NEW_COLUMN       – ukjent nytt felt         → E002 ANMERKNING
      TYPE_MISMATCH    – uforenlig type           → E003 ADVARSEL
    """
    ts = datetime.now()

    if "items" in raw_df.columns:
        work_df = (raw_df
                   .select(F.explode(F.col("items")).alias("item"))
                   .select("item.*"))
    else:
        work_df = raw_df

    actual_cols = {f.name: f.dataType.simpleString()
                   for f in work_df.schema.fields}
    expected    = _build_expected_schema(mapping)
    required    = _parse_required_fields(load_params, mapping)
    records     = []

    missing_in_file   = {c for c in expected if c not in actual_cols}
    new_in_file       = {c for c in actual_cols if c not in expected}
    rename_candidates = _find_rename_candidates(missing_in_file, new_in_file)
    confirmed_renames = {old for old, _ in rename_candidates}
    unmatched_missing = missing_in_file - confirmed_renames

    for col in unmatched_missing:
        is_req = col in required
        records.append(_record(ts, col,
            drift_type     = "MISSING_COLUMN",
            expected_value = expected[col]["type"],
            actual_value   = "ABSENT",
            severity       = "HIGH" if is_req else "MEDIUM",
            suggested_action = (
                f"REQUIRED felt '{col}' mangler – fylles med NULL i Delta. "
                f"Sjekk om kildesystemet har fjernet/renamed feltet."
            ) if is_req else (
                f"Valgfritt felt '{col}' mangler – fylles med NULL. "
                f"Vurder om mapping-raden kan deaktiveres."
            )
        ))

    for old_col, new_col in rename_candidates:
        sim = _similarity(old_col, new_col)
        records.append(_record(ts, old_col,
            drift_type     = "POSSIBLE_RENAME",
            expected_value = old_col,
            actual_value   = new_col,
            severity       = "HIGH" if old_col in required else "MEDIUM",
            suggested_action = (
                f"'{old_col}' mangler, '{new_col}' er nytt (likhet {sim:.0%}). "
                f"Oppdater column_mapping: source='{old_col}' → '{new_col}'."
            )
        ))

    unmatched_new = new_in_file - {n for _, n in rename_candidates}
    for col in unmatched_new:
        records.append(_record(ts, col,
            drift_type     = "NEW_COLUMN",
            expected_value = "NOT_IN_MAPPING",
            actual_value   = actual_cols[col],
            severity       = "MEDIUM",
            suggested_action = (
                f"Nytt felt '{col}' ({actual_cols[col]}) i kildefilen – "
                f"droppes inntil det legges til i column_mapping."
            )
        ))

    for col in expected:
        if col in actual_cols and col not in missing_in_file:
            issue = _check_type_compatibility(expected[col]["type"], actual_cols[col])
            if issue:
                records.append(_record(ts, col,
                    drift_type     = "TYPE_MISMATCH",
                    expected_value = expected[col]["type"],
                    actual_value   = actual_cols[col],
                    severity       = "HIGH",
                    suggested_action = (
                        f"Type-konflikt på '{col}': {issue}. "
                        f"Cast prøves – sjekk for NULL etter lasting."
                    )
                ))

    high   = sum(1 for r in records if r["severity"] == "HIGH")
    medium = sum(1 for r in records if r["severity"] == "MEDIUM")

    if records:
        log_drift(spark        = spark,
                  drift_records = records,
                  column_mapping_id = column_mapping_id,
                  source_path  = source_path,
                  file_name    = file_name,
                  run_id       = run_id)
        print(f"  ⚠️  Schema drift i '{file_name}': {high} HIGH, {medium} MEDIUM")
        for r in records:
            icon = "🔴" if r["severity"] == "HIGH" else "🟡"
            print(f"      {icon} {r['drift_type']:20} '{r['column_name']}' "
                  f"– {r['suggested_action'][:70]}…")
    else:
        print(f"  ✅ Skjema OK for '{file_name}'")

    return {
        "has_drift":    len(records) > 0,
        "high_count":   high,
        "medium_count": medium,
        "records":      records,
        "compatible":   len([r for r in records
                             if r["drift_type"] == "MISSING_COLUMN"
                             and r["severity"] == "HIGH"]) == 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HJELPE-FUNKSJONER  (interne)
# ─────────────────────────────────────────────────────────────────────────────

def _build_expected_schema(mapping):
    schema = {}
    for m in mapping:
        src = m.get("source", "")
        if src == "_loading_ts":
            continue
        top = src.split(".")[0]
        if top not in schema:
            schema[top] = {"type": m.get("type", "string"), "nested": "." in src}
    return schema


def _parse_required_fields(load_params, mapping=None):
    """
    Finn required-felt frå to kjelder, prioritert slik:

    1. load_params["required_fields"] / ["not_null_fields"]
       Eksplisitt overstyring – t.d. BRREG der berre ein delmengde er required.

    2. Fallback: alle top-level felt frå column_mapping
       For kjelder utan explicit required_fields (t.d. YouTube).
       column_mapping er sannheitskjelda for kva felt som skal inn i Bronze.

    Effekt:
      youtube_channels  (ingen required_fields) → {"id","snippet","statistics"}
      brreg_enheter     (required_fields satt)  → {"organisasjonsnummer","navn",...}
    """
    required = set()
    for key in ("required_fields", "not_null_fields"):
        val = (load_params or {}).get(key, "") or ""
        required.update(f.strip() for f in val.split(",") if f.strip())

    # Fallback: utled frå column_mapping viss ingen required_fields er satt
    if not required and mapping:
        for m in mapping:
            src = m.get("source", "")
            if src and src != "_loading_ts":
                required.add(src.split(".")[0])

    return required


def _find_rename_candidates(missing, new):
    candidates, used_new = [], set()
    for old in sorted(missing):
        best_score, best_new = RENAME_SIMILARITY - 0.01, None
        for n in new:
            if n in used_new:
                continue
            s = _similarity(old, n)
            if s > best_score:
                best_score, best_new = s, n
        if best_new:
            candidates.append((old, best_new))
            used_new.add(best_new)
    return candidates


def _similarity(a, b):
    def norm(s): return re.sub(r'[_\-\s]', '', s.lower())
    return SequenceMatcher(None, norm(a), norm(b)).ratio()


def _check_type_compatibility(expected_type, actual_spark_type):
    numeric = {"int", "bigint", "double", "float", "long"}
    if expected_type in numeric and "string" in actual_spark_type:
        return f"forventet {expected_type}, fikk string"
    if expected_type == "boolean" and "string" in actual_spark_type:
        return "forventet boolean, fikk string"
    return None


def _record(ts, col_name, drift_type, expected_value,
            actual_value, severity, suggested_action):
    return {
        "detected_at":     ts,
        "column_name":     col_name,
        "drift_type":      drift_type,
        "expected_value":  str(expected_value),
        "actual_value":    str(actual_value),
        "severity":        severity,
        "suggested_action": suggested_action,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  CONVENIENCE-WRAPPER  (drop-in i load_json_to_delta)
# ─────────────────────────────────────────────────────────────────────────────

def check_and_report_drift(
    spark,
    raw_df,
    mapping:           list,
    load_params:       dict,
    source_path:       str,
    file_name:         str,
    column_mapping_id: str,
    run_id:            int = None,
) -> dict:
    """
    Deteksjon + log.schema_drift + Teams i ett kall.

    I load_json_to_delta, rett etter raw_df er lest:
    ──────────────────────────────────────────────────────────────
    drift_report = check_and_report_drift(
        spark             = spark,
        raw_df            = raw_df,
        mapping           = mapping,
        load_params       = load_params or {},
        source_path       = source_path,
        file_name         = file.name,
        column_mapping_id = column_mapping_id,
        run_id            = run_id,
    )
    # Akkumuler for log_standard() på slutten av load-løkken:
    if drift_report["has_drift"]:
        all_drift["has_drift"]     = True
        all_drift["high_count"]   += drift_report["high_count"]
        all_drift["medium_count"] += drift_report["medium_count"]
        all_drift["records"]      += drift_report["records"]

    # Til slutt i load_json_to_delta (erstatter din nåværende log_standard):
    status = "warning" if all_drift["high_count"] > 0 else "success"
    log_standard(spark, pipeline_name=PIPELINE_NAME, notebook_name=NOTEBOOK_NAME,
                 status=status, rows_processed=total_rows,
                 action_type="loading", source_name=source_path,
                 instruction_detail=column_mapping_id, started_at=started_at)
    ──────────────────────────────────────────────────────────────
    """
    drift_report = detect_schema_drift(
        spark             = spark,
        raw_df            = raw_df,
        mapping           = mapping,
        load_params       = load_params or {},
        source_path       = source_path,
        file_name         = file_name,
        column_mapping_id = column_mapping_id,
        run_id            = run_id,
    )
    if drift_report["has_drift"]:
        notify_teams_drift(
            drift_report      = drift_report,
            source_path       = source_path,
            file_name         = file_name,
            column_mapping_id = column_mapping_id,
            run_id            = run_id,
        )
    return drift_report

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
