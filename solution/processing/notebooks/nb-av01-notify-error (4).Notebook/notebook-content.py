# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-notify-error
# **Purpose**: Varsler om pipeline-feil via Teams med AI-forslag til løsning.
# **Trigges av**: Fabric Data Pipeline ved feil i nb-av01-1-load


# PARAMETERS CELL ********************

# Parameters – settes av Fabric Pipeline eller manuelt ved test
TEST_MODE      = True   # True = bruk testdata, False = les fra quarantine
DISPLAY_PREVIEW = True  # Vis Adaptive Card preview i notebook

# Pipeline action settes av Fabric Pipeline som parameter
try:
    PIPELINE_ACTION = notebookutils.runtime.context.get('parameters', {}).get('action', 'STOP')
except:
    PIPELINE_ACTION = 'STOP'


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

import requests as _req
import json as _json
from datetime import datetime

variables = notebookutils.variableLibrary.getLibrary('vl-av01-variables')

TEAMS_WEBHOOK_URL = "https://default65f510677d654aa9b9964cc43a0d71.11.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/5888292e0ec54b24867c58bc20cd4b26/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=rHnpc6vidV0Q8-a8IK-O2Ixfjr4rK9J5pZSIoD46M44"

LOOKBACK_HOURS    = 1
CLAUDE_MODEL      = 'claude-sonnet-4-20250514'
CLAUDE_MAX_TOKENS = 800
MAX_AI_CALLS      = 5

# Feilkode → type og alvorlighetsgrad
SEVERITY = {
    'E005': {'label': 'KRITISK',    'color': 'Attention', 'icon': '⛔'},
    'E003': {'label': 'ADVARSEL',   'color': 'Warning',   'icon': '⚠️'},
    'E004': {'label': 'ADVARSEL',   'color': 'Warning',   'icon': '⚠️'},
    'E001': {'label': 'ANMERKNING', 'color': 'Accent',    'icon': '📋'},
    'E002': {'label': 'ANMERKNING', 'color': 'Accent',    'icon': '📋'},
}

FALLBACK_RÅD = {
    'E001': ['Sjekk nøkkelkolonne i kildedata', 'Se quarantine for berørte rader', 'Korriger kildedata og kjør loading på nytt'],
    'E002': ['Dedupliser kildedata i ingestion', 'Sjekk ingest-logikk for duplikater', 'Se quarantine for hvilke nøkler som er duplisert'],
    'E003': ['Korriger orgnr i kildesystem (9 siffer)', 'Verifiser mot BRREG Enhetsregisteret', 'Se quarantine for hvilke selskap som har ugyldig orgnr'],
    'E004': ['Fyll inn manglende felt i kildedata', 'Sjekk required_fields i instructions.loading', 'Se quarantine for hvilke rader som mangler verdi'],
    'E005': ['Verifiser JSON-fil i Files/landing zone', 'Sjekk kolonne-mapping i metadata.column_mappings', 'Kjør ingestion på nytt og se om feltet dukker opp'],
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Testdata – brukes når TEST_MODE = True
# =============================================================================

if TEST_MODE:
    from collections import namedtuple
    from datetime import datetime

    # Simuler quarantine-rader som namedtuples (samme struktur som Spark Row)
    ErrorRow = namedtuple('ErrorRow', [
        'error_code', 'validation_type', 'target_table',
        'column_mapping_id', 'error_detail', 'antall_rader',
        'foerste_feil', 'siste_feil'
    ])

    errors = [
        ErrorRow('E005', 'SCHEMA_ERROR',  'sharepoint/meldingslogg',   'sp_meldingslogg',    "Påkrevde felt mangler: ['dummy']",              46, datetime.now(), datetime.now()),
        ErrorRow('E003', 'INVALID_ORGNR', 'sharepoint/meldingslogg',   'sp_meldingslogg',    "'orgnr' = '1234567' er ikke 9 numeriske siffer", 3,  datetime.now(), datetime.now()),
        ErrorRow('E004', 'NULL_VALUE',    'sharepoint/meldingslogg',   'sp_meldingslogg',    'NULL-verdi i påkrevd felt: selskapsnavn',         2,  datetime.now(), datetime.now()),
        ErrorRow('E001', 'NULL_KEY',      'sharepoint/regnskapbedrifter','sp_regnskapbedrifter','NULL i nøkkelkolonne: orgnr',                  43, datetime.now(), datetime.now()),
    ]
    print(f'TEST_MODE: {len(errors)} testrader lastet')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Hent feil fra quarantine (kun ved TEST_MODE = False)
# =============================================================================

if not TEST_MODE:
    errors_df = spark.sql(f'''
        SELECT
            error_code,
            validation_type,
            target_table,
            column_mapping_id,
            error_detail,
            COUNT(*)           AS antall_rader,
            MIN(_loading_ts)   AS foerste_feil,
            MAX(_loading_ts)   AS siste_feil
        FROM `av01-dev-datastores`.`lh_av01_admin`.quarantine.loading_errors
        WHERE _loading_ts >= current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOUR
        GROUP BY error_code, validation_type, target_table, column_mapping_id, error_detail
        ORDER BY error_code, target_table
    ''')
    errors = errors_df.collect()

if not errors:
    print('Ingen feil funnet – avslutter')
    notebookutils.notebook.exit('OK')
else:
    print(f'Fant {len(errors)} feiltyper:')
    for e in errors:
        print(f'  [{e["error_code"]}] {e["target_table"]}: {e["error_detail"]} ({e["antall_rader"]} rader)')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# AI-analyse via Claude API – én kall per unik feilkode (cache-strategi)
# =============================================================================

def get_ai_suggestions(error_code: str, eksempler: list) -> list:
    """
    Én AI-kall per unik feilkode – svaret gjenbrukes for alle forekomster.
    Maks 5 kall uavhengig av antall feil eller rader.
    """
    tabeller    = list(set([e['target_table'] for e in eksempler]))
    detaljer    = list(set([e['error_detail'] for e in eksempler]))
    total_rader = sum([e['antall_rader'] for e in eksempler])

    feilkoder_ref = """
- E001: NULL_KEY – nøkkelkolonne er NULL → raden kan ikke matches ved MERGE
- E002: DUPLICATE_KEY – duplikat nøkkel i source JSON-filen
- E003: INVALID_ORGNR – organisasjonsnummer er ikke 9 numeriske siffer
- E004: NULL_VALUE – påkrevd felt er NULL i kildedata
- E005: SCHEMA_ERROR – påkrevde felt mangler helt i JSON-filen fra landing zone
"""

    prompt = f"""Du er datakvalitetsekspert for Microsoft Fabric dataplattform.
En pipeline (nb-av01-1-load) har oppdaget følgende feil:

Feilkode:     {error_code}
Tabeller:     {", ".join(tabeller)}
Detalj:       {", ".join(detaljer)}
Totalt rader: {total_rader}

Feilkodeoversikt:
{feilkoder_ref}

Gi maks 3 konkrete og korte løsningsforslag på norsk.
Svar kun med en nummerert liste. Hvert punkt maks 15 ord. Vær svært spesifikk."""

    try:
        resp = _req.post(
            'https://api.anthropic.com/v1/messages',
            headers={'Content-Type': 'application/json'},
            json={
                'model'     : CLAUDE_MODEL,
                'max_tokens': CLAUDE_MAX_TOKENS,
                'messages'  : [{'role': 'user', 'content': prompt}]
            },
            timeout=30
        )
        text  = resp.json()['content'][0]['text']
        lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
        return lines
    except Exception as ex:
        return FALLBACK_RÅD.get(error_code, [f'Kunne ikke hente AI-forslag: {ex}'])


if errors:
    # Grupper feil per unik feilkode
    feil_per_kode = {}
    for e in errors:
        kode = e['error_code']
        if kode not in feil_per_kode:
            feil_per_kode[kode] = []
        feil_per_kode[kode].append(e)

    print(f"Unike feilkoder: {list(feil_per_kode.keys())}")

    # Cache – én AI-kall per unik feilkode
    ai_cache = {}
    if len(feil_per_kode) > MAX_AI_CALLS:
        print(f"  -> {len(feil_per_kode)} feiltyper > MAX_AI_CALLS={MAX_AI_CALLS} – bruker fallback-råd")
        for kode in feil_per_kode:
            ai_cache[kode] = FALLBACK_RÅD.get(kode, ['Se quarantine.loading_errors for detaljer'])
    else:
        for kode, eksempler in feil_per_kode.items():
            print(f"  AI-kall for {kode} ({len(eksempler)} forekomster)...")
            ai_cache[kode] = get_ai_suggestions(kode, eksempler)

    # Map ai_results per feil – gjenbruk fra cache
    ai_results = {}
    for e in errors:
        key              = f"{e['error_code']}_{e['target_table']}"
        ai_results[key]  = ai_cache[e['error_code']]

    print(f"  -> AI-analyse ferdig: {len(ai_cache)} kall for {len(errors)} feil")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Forhåndsvisning + send til Teams via Power Automate
# =============================================================================

def build_pa_payload(errors: list, ai_results: dict) -> dict:
    """
    Bygger JSON-payload som sendes til Power Automate.
    Power Automate bygger Adaptive Card og sender til Teams.
    Payload-format matcher test_payload eksakt.
    """
    antall_kritiske  = sum(1 for e in errors if e['error_code'] == 'E005')
    antall_advarsler = sum(1 for e in errors if e['error_code'] in ('E003', 'E004'))
    antall_rader     = sum(e['antall_rader'] for e in errors)

    feil_liste = []
    for e in errors:
        key = f"{e['error_code']}_{e['target_table']}"
        feil_liste.append({
            'error_code'       : e['error_code'],
            'validation_type'  : e['validation_type'],
            'target_table'     : e['target_table'],
            'column_mapping_id': e['column_mapping_id'],
            'error_detail'     : e['error_detail'],
            'antall_rader'     : int(e['antall_rader']),
            'ai_forslag'       : ai_results.get(key, [])
        })

    return {
        'tidspunkt'        : datetime.now().strftime('%Y-%m-%d %H:%M'),
        'antall_feil'      : len(errors),
        'antall_kritiske'  : antall_kritiske,
        'antall_advarsler' : antall_advarsler,
        'antall_rader'     : int(antall_rader),
        'pipeline_status'  : 'Stopped' if antall_kritiske > 0 else 'Warning',
        'feil'             : feil_liste
    }


def send_teams_alert(pa_payload: dict, webhook_url: str):
    """Sender data-payload til Power Automate som bygger og poster Adaptive Card."""
    if not webhook_url:
        print('  -> ADVARSEL: TEAMS_WEBHOOK_URL ikke satt – hoppes over')
        return
    resp = _req.post(webhook_url, json=pa_payload, timeout=15)
    print(f'  -> Teams varsling sendt: HTTP {resp.status_code}')
    if resp.status_code not in (200, 202):
        print(f'  -> Respons: {resp.text}')


def build_preview_card(payload: dict) -> dict:
    """
    Bygger Adaptive Card KUN for preview i notebook.
    Sendes ikke til Teams — Power Automate lager den ekte kortet.
    """
    cards = []
    for e in payload['feil']:
        sev = SEVERITY.get(e['error_code'], SEVERITY['E001'])
        style = 'attention' if e['error_code'] == 'E005' else \
                'warning'   if e['error_code'] in ('E003', 'E004') else 'accent'
        cards.append({
            'type': 'Container', 'spacing': 'Small', 'style': style,
            'items': [
                {'type': 'ColumnSet', 'columns': [
                    {'type': 'Column', 'width': 'auto', 'items': [
                        {'type': 'TextBlock', 'text': sev['icon'], 'size': 'Large'}
                    ]},
                    {'type': 'Column', 'width': 'stretch', 'items': [
                        {'type': 'TextBlock',
                         'text': f"{sev['label']} · {e['error_code']}",
                         'weight': 'Bolder', 'color': sev['color'],
                         'size': 'Small', 'spacing': 'None'},
                        {'type': 'TextBlock', 'text': e['error_detail'],
                         'wrap': True, 'weight': 'Bolder', 'spacing': 'None'},
                        {'type': 'TextBlock',
                         'text': f"{e['target_table']} · {e['antall_rader']} rader",
                         'size': 'Small', 'isSubtle': True, 'spacing': 'None'}
                    ]}
                ]},
                {'type': 'ActionSet', 'spacing': 'Small', 'actions': [{
                    'type': 'Action.ShowCard',
                    'title': '🤖 Detaljer og AI-forslag',
                    'card': {'type': 'AdaptiveCard', 'body': [
                        {'type': 'FactSet', 'facts': [
                            {'title': 'Tabell',   'value': e['target_table']},
                            {'title': 'Mapping',  'value': e['column_mapping_id']},
                            {'title': 'Feilkode', 'value': f"{e['error_code']} - {e['validation_type']}"},
                            {'title': 'Rader',    'value': str(e['antall_rader'])}
                        ]},
                        {'type': 'TextBlock', 'text': '🤖 AI-forslag',
                         'weight': 'Bolder', 'size': 'Small', 'spacing': 'Medium'},
                        {'type': 'TextBlock',
                         'text': '\n'.join(e.get('ai_forslag', [])),
                         'wrap': True, 'size': 'Small'},
                        {'type': 'TextBlock',
                         'text': 'lh_av01_admin - quarantine.loading_errors',
                         'fontType': 'Monospace', 'size': 'Small',
                         'isSubtle': True, 'spacing': 'Medium'}
                    ]}
                }]}
            ]
        })

    return {
        '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
        'type': 'AdaptiveCard', 'version': '1.5',
        'body': [
            {'type': 'Container', 'style': 'emphasis', 'bleed': True, 'items': [
                {'type': 'ColumnSet', 'columns': [
                    {'type': 'Column', 'width': 'auto', 'items': [
                        {'type': 'TextBlock', 'text': '⚡', 'size': 'Large'}
                    ]},
                    {'type': 'Column', 'width': 'stretch', 'items': [
                        {'type': 'TextBlock', 'text': 'Data Pipeline Alert',
                         'weight': 'Bolder', 'size': 'Medium', 'spacing': 'None'},
                        {'type': 'TextBlock',
                         'text': f"{payload['tidspunkt']} · nb-av01-1-load",
                         'size': 'Small', 'isSubtle': True, 'spacing': 'None'}
                    ]}
                ]}
            ]},
            {'type': 'ColumnSet', 'spacing': 'Medium', 'columns': [
                {'type': 'Column', 'width': 'stretch', 'items': [
                    {'type': 'TextBlock', 'text': '⛔ Kritiske',
                     'size': 'Small', 'weight': 'Bolder', 'color': 'Attention'},
                    {'type': 'TextBlock', 'text': str(payload['antall_kritiske']),
                     'size': 'ExtraLarge', 'weight': 'Bolder', 'color': 'Attention', 'spacing': 'None'}
                ]},
                {'type': 'Column', 'width': 'stretch', 'items': [
                    {'type': 'TextBlock', 'text': '⚠️ Advarsler',
                     'size': 'Small', 'weight': 'Bolder', 'color': 'Warning'},
                    {'type': 'TextBlock', 'text': str(payload['antall_advarsler']),
                     'size': 'ExtraLarge', 'weight': 'Bolder', 'color': 'Warning', 'spacing': 'None'}
                ]},
                {'type': 'Column', 'width': 'stretch', 'items': [
                    {'type': 'TextBlock', 'text': '📋 Anmerkning',
                     'size': 'Small', 'weight': 'Bolder', 'color': 'Accent'},
                    {'type': 'TextBlock',
                     'text': str(payload['antall_feil'] - payload['antall_kritiske'] - payload['antall_advarsler']),
                     'size': 'ExtraLarge', 'weight': 'Bolder', 'color': 'Accent', 'spacing': 'None'}
                ]},
                {'type': 'Column', 'width': 'stretch', 'items': [
                    {'type': 'TextBlock', 'text': '📊 Rader',
                     'size': 'Small', 'weight': 'Bolder', 'isSubtle': True},
                    {'type': 'TextBlock', 'text': str(payload['antall_rader']),
                     'size': 'ExtraLarge', 'weight': 'Bolder', 'spacing': 'None'}
                ]}
            ]},
            {'type': 'TextBlock', 'separator': True, 'spacing': 'Medium',
             'text': f"{payload['antall_feil']} feil oppdaget · {payload['pipeline_status']}",
             'size': 'Small', 'isSubtle': True}
        ] + cards + [
            {'type': 'ColumnSet', 'separator': True, 'spacing': 'Medium', 'columns': [
                {'type': 'Column', 'width': 'stretch', 'items': [
                    {'type': 'TextBlock',
                     'text': 'lh_av01_admin · quarantine.loading_errors',
                     'fontType': 'Monospace', 'size': 'Small', 'isSubtle': True}
                ]},
                {'type': 'Column', 'width': 'auto', 'items': [
                    {'type': 'TextBlock',
                     'text': '⛔ Pipeline stoppet' if payload['antall_kritiske'] > 0 else '⚠️ Advarsel',
                     'color': 'Attention' if payload['antall_kritiske'] > 0 else 'Warning',
                     'weight': 'Bolder', 'size': 'Small'}
                ]}
            ]}
        ]
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Utfør: preview + send + pipeline-signal
# =============================================================================

if errors:
    # Bygg Power Automate payload
    pa_payload = build_pa_payload(errors, ai_results)

    # ── Forhåndsvisning i notebook ────────────────────────────────────────────
    if DISPLAY_PREVIEW:
        preview_card = build_preview_card(pa_payload)
        card_json    = _json.dumps(preview_card, ensure_ascii=False)
        displayHTML(f"""
        <div style='max-width:600px;padding:8px'>
          <div style='font-size:11px;color:#888;margin-bottom:8px;
                      text-transform:uppercase;letter-spacing:0.05em'>
            Adaptive Card preview
          </div>
          <div id="ac-preview"></div>
        </div>
        <script src="https://unpkg.com/adaptivecards/dist/adaptivecards.min.js"></script>
        <script>
            var adaptiveCard = new AdaptiveCards.AdaptiveCard();
            adaptiveCard.hostConfig = new AdaptiveCards.HostConfig({{
                fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif",
                containerStyles: {{
                    default: {{
                        backgroundColor: "#ffffff",
                        foregroundColors: {{
                            default:   {{ default: "#1a1a1a", subtle: "#888888" }},
                            attention: {{ default: "#A32D2D", subtle: "#E24B4A" }},
                            warning:   {{ default: "#854F0B", subtle: "#BA7517" }},
                            accent:    {{ default: "#185FA5", subtle: "#378ADD" }}
                        }}
                    }}
                }},
                actions: {{
                    actionAlignment: "left",
                    actionsOrientation: "horizontal",
                    showCard: {{ actionMode: "inline" }}
                }}
            }});
            adaptiveCard.parse({card_json});
            var rendered = adaptiveCard.render();
            document.getElementById("ac-preview").appendChild(rendered);
        </script>
        """)
        print(f"Forhåndsvisning klar – klar til å sende {len(errors)} varsel(er) til Teams.")

    # ── Send til Teams via Power Automate ─────────────────────────────────────
    send_teams_alert(pa_payload, TEAMS_WEBHOOK_URL)

    # ── Pipeline-signal ────────────────────────────────────────────────────────
    if not TEST_MODE:
        if PIPELINE_ACTION == 'STOP':
            raise Exception(f"Pipeline stoppet – {len(errors)} feil varslet til Teams")
        else:
            print('  -> Advarselsvarsling sendt – pipeline fortsetter')
            notebookutils.notebook.exit('WARNING_NOTIFIED')
    else:
        print('  -> TEST_MODE: ingen pipeline-signal sendt')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
