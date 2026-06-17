# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-integration-tests-schema-drift
# 
# **Formål:** Integrasjonstester for schema drift-deteksjon. Verifiserer at heile kjeda fungerer mot ekte infrastruktur — at `log_drift()` faktisk skriv til `log.schema_drift` i SQL Fabric med korrekt innhald, og at `detect_schema_drift()` produserer rette rader end-to-end.
# 
# ---
# 
# **Avhengigheiter:** `nb-av01-generic-functions`, `nb-av01-schema-drift`
# 
# **Krev:** SQL-tilkobling til metadata-database, `log.schema_drift`-tabell oppretta
# 
# ---
# 
# > ⚠️ **Desse testane SKRIV til databasen.** Alle testrader får `run_id = -999` og vert rydda opp i cleanup-cella nedst. Køyr cleanup-cella manuelt viss testane avbryt før opprydding.
# 
# ---
# 
# ### Når skal du køyre desse testane?
# 
# - Etter første gongs oppsett av `log.schema_drift`-tabellen
# - Etter endringar i `log_drift()` eller `detect_schema_drift()`
# - Etter endringar i `log.schema_drift` DDL (nye kolonnar, typeendringar)
# - **Ikkje** som del av ordinær pipeline-køyring
# 
# ### Ikkje forveksle med unit-testane
# 
# | | Unit-testar | Integrasjonstester |
# |---|---|---|
# | Fil | `nb-av01-unit-tests` | denne notebooken |
# | SQL-skriving | nei (mock) | **ja** |
# | Køyring | ofte, automatisk | manuelt ved behov |
# | Testar | logikk i isolasjon | heile kjeda mot infra |


# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from datetime import datetime, timezone
import uuid

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

set_metadata_db_url(
    server   = variables.METADATA_SERVER,
    database = variables.METADATA_DB
)

# Alle testrader får denne run_id – gjer opprydding trivielt
INTEGRATION_TEST_RUN_ID = -999

# Unik suffix per køyring – sikrar at testane ikkje påverkar kvarandre
# sjølv om cleanup ikkje vart køyrt etter forrige køyring
TEST_RUN_SUFFIX = str(uuid.uuid4())[:8]

print("=" * 60)
print("  Integrasjonstester – Schema Drift")
print(f"  SQL:         {variables.METADATA_SERVER} / {variables.METADATA_DB}")
print(f"  Test run_id: {INTEGRATION_TEST_RUN_ID}")
print(f"  Run suffix:  {TEST_RUN_SUFFIX}")
print("=" * 60)
print()
print("⚠️  Desse testane SKRIV til log.schema_drift.")
print("   Opprydding skjer automatisk på slutten,")
print("   men ved feil: køyr cleanup-cella manuelt.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test-hjelpefunksjonar
# 
# Same mønster som `nb-av01-unit-tests`: `run_test()` fangar `AssertionError` og rapporterer PASSED / FAILED / ERROR.
# 
# `read_drift_log()` brukar same `.mssql()`-connector som resten av systemet — same mønster som `query_metadata_table()` og `les_delta_med_cdf()` i generic-functions.
# 
# Kvar test filtrerer på både `column_mapping_id` og `file_name` (som inneheld `TEST_RUN_SUFFIX`), slik at testane er idempotente — tidlegare køyringar utan cleanup påverkar ikkje resultatet.

# CELL ********************

def run_test(test_func):
    """Køyr ein testfunksjon og rapporter resultat."""
    test_name = test_func.__name__
    try:
        test_func()
        print(f"  PASSED: {test_name}")
        return True
    except AssertionError as e:
        print(f"  FAILED: {test_name} – {str(e)}")
        return False
    except Exception as e:
        print(f"  ERROR:  {test_name} – {type(e).__name__}: {str(e)}")
        return False


def read_drift_log(run_id=INTEGRATION_TEST_RUN_ID):
    """
    Les testrader frå log.schema_drift for gitt run_id.
    Brukar .mssql() med inline SQL – same mønster som les_delta_med_cdf()
    og query_metadata_table() i nb-av01-generic-functions.
    """
    return (
        spark.read
             .option("url", METADATA_DB_URL)
             .mssql(f"""(
                 SELECT *
                 FROM log.schema_drift
                 WHERE run_id = {run_id}
             ) AS q""")
    )


def test_file(label):
    """Genererer unik filnamn per test og køyring."""
    return f"integration_{label}_{TEST_RUN_SUFFIX}.json" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## log_drift → SQL Fabric
# 
# Testar at `log_drift()` skriv korrekte rader til `log.schema_drift`. Kvar test filtrerer på `file_name` med `TEST_RUN_SUFFIX` — idempotent sjølv utan cleanup mellom køyringar.

# CELL ********************

def test_integration_log_drift_single_record():
    """log_drift skriv éi rad med korrekte felt til log.schema_drift."""
    fname = test_file("single")
    test_record = [{
        "detected_at":      datetime.now(timezone.utc),
        "column_name":      "test_col",
        "drift_type":       "MISSING_COLUMN",
        "expected_value":   "string",
        "actual_value":     "ABSENT",
        "severity":         "HIGH",
        "suggested_action": "integrasjonstest – skal slettast",
    }]

    log_drift(
        spark             = spark,
        drift_records     = test_record,
        column_mapping_id = "INTEGRATION_TEST_SINGLE",
        source_path       = "Files/test/",
        file_name         = fname,
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    rows = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_SINGLE' AND file_name = '{fname}'"
    ).collect()

    assert len(rows) == 1,                        f"Forventa 1 rad, fekk {len(rows)}"
    r = rows[0]
    assert r["column_name"] == "test_col",        f"column_name feil: {r['column_name']}"
    assert r["drift_type"]  == "MISSING_COLUMN",  f"drift_type feil: {r['drift_type']}"
    assert r["severity"]    == "HIGH",            f"severity feil: {r['severity']}"
    assert r["error_code"]  == "E005",            f"error_code feil: {r['error_code']}"
    assert r["resolved"]    == False,             f"resolved skal vere False"


def test_integration_log_drift_multiple_records():
    """log_drift skriv fleire rader korrekt i eitt kall."""
    fname = test_file("multi")
    test_records = [
        {
            "detected_at":      datetime.now(timezone.utc),
            "column_name":      "col_a",
            "drift_type":       "NEW_COLUMN",
            "expected_value":   "NOT_IN_MAPPING",
            "actual_value":     "string",
            "severity":         "MEDIUM",
            "suggested_action": "integrasjonstest – skal slettast",
        },
        {
            "detected_at":      datetime.now(timezone.utc),
            "column_name":      "col_b",
            "drift_type":       "TYPE_MISMATCH",
            "expected_value":   "bigint",
            "actual_value":     "string",
            "severity":         "HIGH",
            "suggested_action": "integrasjonstest – skal slettast",
        },
    ]

    log_drift(
        spark             = spark,
        drift_records     = test_records,
        column_mapping_id = "INTEGRATION_TEST_MULTI",
        source_path       = "Files/test/",
        file_name         = fname,
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    rows = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_MULTI' AND file_name = '{fname}'"
    ).collect()

    assert len(rows) == 2,          f"Forventa 2 rader, fekk {len(rows)}"
    drift_types = {r["drift_type"] for r in rows}
    assert "NEW_COLUMN"    in drift_types, "Manglar NEW_COLUMN-rad"
    assert "TYPE_MISMATCH" in drift_types, "Manglar TYPE_MISMATCH-rad"


def test_integration_log_drift_error_code_mapping():
    """Alle drift-typar får riktig error_code i databasen."""
    fname = test_file("errorcodes")
    expected_codes = {
        "MISSING_COLUMN":  "E005",
        "POSSIBLE_RENAME": "E005",
        "TYPE_MISMATCH":   "E003",
        "NEW_COLUMN":      "E002",
    }

    test_records = [
        {
            "detected_at":      datetime.now(timezone.utc),
            "column_name":      f"col_{dt.lower()}",
            "drift_type":       dt,
            "expected_value":   "x",
            "actual_value":     "y",
            "severity":         "HIGH" if ec in ("E005", "E003") else "MEDIUM",
            "suggested_action": "integrasjonstest – skal slettast",
        }
        for dt, ec in expected_codes.items()
    ]

    log_drift(
        spark             = spark,
        drift_records     = test_records,
        column_mapping_id = "INTEGRATION_TEST_ERRORCODES",
        source_path       = "Files/test/",
        file_name         = fname,
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    rows = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_ERRORCODES' AND file_name = '{fname}'"
    ).collect()

    actual = {r["drift_type"]: r["error_code"] for r in rows}
    for drift_type, expected_code in expected_codes.items():
        assert actual.get(drift_type) == expected_code, \
            f"{drift_type}: forventa {expected_code}, fekk {actual.get(drift_type)}"


def test_integration_log_drift_resolved_default_false():
    """Nye rader skal ha resolved = False, ikkje NULL."""
    fname = test_file("resolved")
    test_record = [{
        "detected_at":      datetime.now(timezone.utc),
        "column_name":      "resolved_test_col",
        "drift_type":       "NEW_COLUMN",
        "expected_value":   "NOT_IN_MAPPING",
        "actual_value":     "string",
        "severity":         "MEDIUM",
        "suggested_action": "integrasjonstest – skal slettast",
    }]

    log_drift(
        spark             = spark,
        drift_records     = test_record,
        column_mapping_id = "INTEGRATION_TEST_RESOLVED",
        source_path       = "Files/test/",
        file_name         = fname,
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    rows = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_RESOLVED' AND file_name = '{fname}'"
    ).collect()

    assert len(rows) == 1,              f"Forventa 1 rad, fekk {len(rows)}"
    assert rows[0]["resolved"] is False, \
        f"resolved skal vere False, fekk: {rows[0]['resolved']}" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## detect_schema_drift → end-to-end
# 
# Testar heile kjeda: `detect_schema_drift()` køyrer drift-logikken, kallar `log_drift()` internt, og vi les rader tilbake frå SQL via `.mssql()` for å verifisere at korrekt innhald faktisk kom inn.

# CELL ********************

def test_integration_detect_missing_column_writes_to_sql():
    """detect_schema_drift skriv MISSING_COLUMN til SQL og returnerer riktig rapport."""
    fname = test_file("e2e_missing")
    mapping = [
        {"source": "video_id",   "target": "video_id",   "type": "string"},
        {"source": "statistics", "target": "statistics", "type": "string"},
    ]
    load_params = {"required_fields": "video_id,statistics"}
    df = spark.createDataFrame([Row(video_id="abc")])  # statistics manglar

    result = detect_schema_drift(
        spark             = spark,
        raw_df            = df,
        mapping           = mapping,
        load_params       = load_params,
        source_path       = "Files/test/",
        file_name         = fname,
        column_mapping_id = "INTEGRATION_TEST_E2E",
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    assert result["has_drift"],       "Skal rapportere drift"
    assert result["high_count"] >= 1, "Skal ha HIGH-avvik"
    assert not result["compatible"],  "Skal vere inkompatibel"

    rows = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_E2E' AND file_name = '{fname}'"
    ).collect()

    assert len(rows) >= 1, \
        f"Forventa minst 1 rad i log.schema_drift, fekk {len(rows)}"
    assert "MISSING_COLUMN" in {r["drift_type"] for r in rows}, \
        "Forventa MISSING_COLUMN i SQL"


def test_integration_detect_no_drift_writes_nothing():
    """detect_schema_drift skriv ingenting til SQL når skjema er OK."""
    fname = test_file("e2e_nodrift")
    mapping = [{"source": "video_id", "target": "video_id", "type": "string"}]
    load_params = {}
    df = spark.createDataFrame([Row(video_id="abc")])

    count_before = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_NO_DRIFT' AND file_name = '{fname}'"
    ).count()

    detect_schema_drift(
        spark             = spark,
        raw_df            = df,
        mapping           = mapping,
        load_params       = load_params,
        source_path       = "Files/test/",
        file_name         = fname,
        column_mapping_id = "INTEGRATION_TEST_NO_DRIFT",
        run_id            = INTEGRATION_TEST_RUN_ID,
    )

    count_after = read_drift_log().filter(
        f"column_mapping_id = 'INTEGRATION_TEST_NO_DRIFT' AND file_name = '{fname}'"
    ).count()

    assert count_after == count_before, \
        f"Ingen nye rader forventa, fekk {count_after - count_before} nye" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Køyr alle integrasjonstester
# 
# Køyr denne cella for å køyre alle testane i sekvens. Køyr **cleanup-cella nedst** etterpå.

# CELL ********************

def run_all_integration_tests():
    print("=" * 60)
    print("  Integrasjonstester – Schema Drift")
    print("=" * 60)

    test_categories = {
        "log_drift → SQL Fabric": [
            test_integration_log_drift_single_record,
            test_integration_log_drift_multiple_records,
            test_integration_log_drift_error_code_mapping,
            test_integration_log_drift_resolved_default_false,
        ],
        "detect_schema_drift → end-to-end": [
            test_integration_detect_missing_column_writes_to_sql,
            test_integration_detect_no_drift_writes_nothing,
        ],
    }

    total_passed = 0
    total_failed = 0

    for category, tests in test_categories.items():
        print(f"\n{category}:")
        for test_func in tests:
            if run_test(test_func):
                total_passed += 1
            else:
                total_failed += 1

    total = total_passed + total_failed
    print("\n" + "=" * 60)
    print(f"Resultat: {total_passed}/{total} bestått"
          + (f"  –  {total_failed} feilet" if total_failed else "  ✅ alle OK"))
    print("=" * 60)
    return total_failed == 0


all_passed = run_all_integration_tests()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Opprydding
# 
# > ⚠️ **Køyr alltid denne cella etter testane** — også viss testane feila.
# 
# Alle testrader har `run_id = -999`. Cella viser antal rader som vil bli sletta og skriv ut SQL-setninga du køyrer i SQL Fabric-editoren.

# CELL ********************

cleanup_sql = f"""
DELETE FROM log.schema_drift
WHERE run_id = {INTEGRATION_TEST_RUN_ID};
"""

try:
    count = (
        spark.read
             .option("url", METADATA_DB_URL)
             .mssql(f"""(
                 SELECT COUNT(*) AS n
                 FROM log.schema_drift
                 WHERE run_id = {INTEGRATION_TEST_RUN_ID}
             ) AS q""")
             .collect()[0]["n"]
    )
    print(f"Testrader i log.schema_drift med run_id = {INTEGRATION_TEST_RUN_ID}: {count}")
    print()
    print("── Køyr dette i SQL Fabric-editoren for å slette: ────────────")
    print(cleanup_sql)
    print("──────────────────────────────────────────────────────────────")
except Exception as e:
    print(f"Kunne ikkje lese rad-tal: {e}")
    print()
    print("── Køyr dette manuelt i SQL Fabric: ──────────────────────────")
    print(cleanup_sql)
    print("──────────────────────────────────────────────────────────────")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
