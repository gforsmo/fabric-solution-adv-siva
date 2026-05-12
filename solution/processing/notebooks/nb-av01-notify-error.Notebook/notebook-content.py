# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-notify-error
# **Purpose**: Varsler om kritiske pipeline-feil via Teams og bruker Claude AI for å foreslå løsninger.
# 
# **Trigges av**: Fabric Data Pipeline ved feil i nb-av01-1-load
# 
# **Avhengigheter**: nb-av01-generic-functions


# CELL ********************

import requests as _requests

TEAMS_WEBHOOK_URL = "https://default65f510677d654aa9b9964cc43a0d71.11.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/7b98dba936764585b514884587195d86/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=AI_1-sXcklh4dXBg2B-9u4CbWcvlmCApu6ioVRnPWks"

# Test med enkel melding
payload = {
    "type": "message",
    "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "contentUrl": None,
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [{
                "type": "TextBlock",
                "text": "✅ Test fra Fabric notebook",
                "weight": "Bolder"
            }]
        }
    }]
}

resp = _requests.post(TEAMS_WEBHOOK_URL, json=payload)
print(f"HTTP {resp.status_code}: {resp.text}")

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

# =============================================================================
# Konfigurasjon
# =============================================================================

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# Teams webhook URL – sett i Key Vault eller hardkod for dev
TEAMS_WEBHOOK_URL = ""   # ← sett inn webhook URL

# Tidsvindu for feil-sjekk (timer tilbake)
LOOKBACK_HOURS = 1

# Claude AI konfigurasjon
CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Hent kritiske feil fra quarantine
# =============================================================================

errors_df = spark.sql(f"""
    SELECT 
        error_code,
        validation_type,
        target_table,
        error_detail,
        COUNT(*) as antall_rader,
        MIN(_loading_ts) as foerste_feil,
        MAX(_loading_ts) as siste_feil
    FROM `av01-dev-datastores`.`lh_av01_bronze`.quarantine.loading_errors
    WHERE _loading_ts >= current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOUR
    GROUP BY error_code, validation_type, target_table, error_detail
    ORDER BY error_code, target_table
""")

errors = errors_df.collect()

if not errors:
    print("Ingen kritiske feil funnet – avslutter")
else:
    print(f"Fant {len(errors)} feiltyper i quarantine")
    for e in errors:
        print(f"  [{e['error_code']}] {e['target_table']}: {e['error_detail']} ({e['antall_rader']} rader)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# AI-analyse via Claude API
# =============================================================================

import requests as _requests
import json as _json

def get_ai_suggestions(errors: list) -> str:
    """
    Sender feilinformasjon til Claude API og returnerer forslag til løsning.
    """
    feil_tekst = "\n".join([
        f"- [{e['error_code']}] Tabell: {e['target_table']} | "
        f"Type: {e['validation_type']} | "
        f"Detalj: {e['error_detail']} | "
        f"Antall rader: {e['antall_rader']}"
        for e in errors
    ])

    prompt = f"""Du er en datakvalitets-ekspert for et Microsoft Fabric dataplattform.
Vi har fått følgende feil i vår data pipeline (nb-av01-1-load) som laster data fra Files til Bronze Delta-tabeller:

Feilkoder:
- E001: NULL_KEY – nøkkelkolonne er NULL
- E002: DUPLICATE_KEY – duplikat nøkkel i source
- E003: INVALID_ORGNR – organisasjonsnummer er ikke 9 numeriske siffer
- E004: NULL_VALUE – påkrevd felt er NULL
- E005: SCHEMA_ERROR – påkrevde felt mangler i JSON-filen

Følgende feil ble oppdaget:
{feil_tekst}

Gi konkrete, korte forslag til hvordan disse feilene kan løses. 
Svar på norsk. Maks 5 punkter per feiltype. Vær spesifikk om hvilke steg som må tas."""

    response = _requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"Content-Type": "application/json"},
        json={
            "model": CLAUDE_MODEL,
            "max_tokens": CLAUDE_MAX_TOKENS,
            "messages": [{"role": "user", "content": prompt}]
        }
    )

    data = response.json()
    return data["content"][0]["text"] if data.get("content") else "Ingen AI-forslag tilgjengelig"


if errors:
    print("Henter AI-forslag fra Claude...")
    ai_suggestions = get_ai_suggestions(errors)
    print("\nAI-forslag:")
    print(ai_suggestions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Send Teams-varsling
# =============================================================================

def send_teams_alert(errors: list, ai_suggestions: str, webhook_url: str):
    """
    Sender strukturert Teams-varsling med feiloversikt og AI-forslag.
    """
    if not webhook_url:
        print("  -> ADVARSEL: TEAMS_WEBHOOK_URL ikke satt – varsling hoppes over")
        return

    # Bygg feiloversikt
    feil_liste = "\n".join([
        f"• [{e['error_code']}] **{e['target_table']}**: {e['error_detail']} ({e['antall_rader']} rader)"
        for e in errors
    ])

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "⚠️ Data Pipeline Feil – av01",
        "themeColor": "FF0000",
        "title": "⚠️ Data Pipeline Feil – nb-av01-1-load",
        "sections": [
            {
                "activityTitle": "Feiloversikt",
                "activityText": feil_liste
            },
            {
                "activityTitle": "🤖 AI-forslag til løsning",
                "activityText": ai_suggestions
            },
            {
                "activityTitle": "📊 Se quarantine-tabell",
                "activityText": "lh_av01_bronze → quarantine.loading_errors"
            }
        ]
    }

    resp = _requests.post(webhook_url, json=payload)
    print(f"  -> Teams varsling sendt: HTTP {resp.status_code}")


if errors:
    send_teams_alert(errors, ai_suggestions, TEAMS_WEBHOOK_URL)
    print("\nVarsling fullført")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
