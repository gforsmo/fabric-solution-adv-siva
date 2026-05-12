# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Test – log_standard, get_watermark, update_watermark
# Kjør cellene i rekkefølge for å verifisere at SQL Fabric-connector fungerer.

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



variables = notebookutils.variableLibrary.getLibrary('vl-av01-variables')
set_metadata_db_url(server=variables.METADATA_SERVER, database=variables.METADATA_DB)
print('Setup OK')
print('METADATA_DB_URL:', METADATA_DB_URL)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Steg 2: Test log_standard ────────────────────────────────────────────────
# Forventer: rad skrives til log.pipeline_runs, ingen feil

rows = log_standard(
    spark            = spark,
    pipeline_name    = 'test_pipeline',
    notebook_name    = 'nb-test-watermark',
    status           = 'success',
    rows_processed   = 42,
    action_type      = 'ingestion',
    source_name      = 'brreg_enheter',
    instruction_detail = '/oppdateringer/enheter',
)
print('log_standard: OK')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Steg 3: Test get_watermark – før oppdatering ─────────────────────────────
# Forventer: rad finnes, watermark_dato=None, watermark_id=None

wm = get_watermark(spark, source_id=2, endpoint_path='/oppdateringer/enheter')
print('get_watermark resultat:', wm)
assert wm is not None, 'FEIL: Ingen watermark-rad funnet for source_id=2'
print('get_watermark: OK')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Steg 4: Test update_watermark med dato ───────────────────────────────────
# Simulerer hva download_to_files_brreg() gjør etter full load
# Forventer: watermark_dato satt, watermark_id=None

test_dato = '2026-05-04T10:00:00.000Z'

update_watermark(
    spark          = spark,
    source_id      = 2,
    endpoint_path  = '/oppdateringer/enheter',
    watermark_dato = test_dato,
)

wm = get_watermark(spark, source_id=2, endpoint_path='/oppdateringer/enheter')
print('Etter dato-oppdatering:', wm)
assert wm['watermark_dato'] == test_dato, f'FEIL: watermark_dato er {wm["watermark_dato"]}'
assert wm['watermark_id'] is None, f'FEIL: watermark_id skal være None, er {wm["watermark_id"]}'
print('update_watermark (dato): OK')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Steg 5: Test update_watermark med id ─────────────────────────────────────
# Simulerer hva ingest_brreg() gjør etter inkrementell kjøring
# Forventer: watermark_id satt, watermark_dato=None

test_id = 185432

update_watermark(
    spark         = spark,
    source_id     = 2,
    endpoint_path = '/oppdateringer/enheter',
    watermark_id  = test_id,
)

wm = get_watermark(spark, source_id=2, endpoint_path='/oppdateringer/enheter')
print('Etter id-oppdatering:', wm)
assert wm['watermark_id'] == test_id, f'FEIL: watermark_id er {wm["watermark_id"]}'
assert wm['watermark_dato'] is None, f'FEIL: watermark_dato skal være None, er {wm["watermark_dato"]}'
print('update_watermark (id): OK')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Steg 6: Reset watermark til NULL ────────────────────────────────────────
# Tilbakestill til utgangspunkt etter test

update_watermark(
    spark         = spark,
    source_id     = 2,
    endpoint_path = '/oppdateringer/enheter',
    watermark_dato = None,
    watermark_id   = None,
)

wm = get_watermark(spark, source_id=2, endpoint_path='/oppdateringer/enheter')
print('Etter reset:', wm)
assert wm['watermark_dato'] is None
assert wm['watermark_id'] is None
print('Reset: OK')
print()
print('=== Alle tester OK ===')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
