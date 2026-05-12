# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-init-sql-database
#  
# **Purpose**: Seed the metadata SQL database with initial configuration data.
# 
#  **Usage**: Run once per new workspace/environment to populate metadata tables.
#  
#  Uses the Spark - MSSQL Connector, [read more here](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector?tabs=pyspark%2Caccesstoken). 
# 
# **Dependencies**: Requires nb-av01-generic-functions (provides TimestampType, BooleanType, notebookutils)
# 
# **Tables Seeded**:
# - metadata.log_store, source_store, loading_store, transform_store, expectation_store, column_mappings
# - instructions.ingestion, loading, transformations, validations

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

# Additional imports (TimestampType, BooleanType available via generic-functions)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

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

print(f"METADATA_SERVER: '{variables.METADATA_SERVER}'")
print(f"METADATA_DB: '{variables.METADATA_DB}'")

print(f"METADATA_DB_URL: '{METADATA_DB_URL}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Metadata Store Definitions
#  Configuration tables that define available functions, sources, and mappings.

# CELL ********************

log_store_schema = StructType([
    StructField("log_id", IntegerType(), False),
    StructField("function_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

log_store_data = [
    (1, "log_standard", "Standard logging - records start, end, row counts, and status",
     '{"params": ["pipeline_name", "notebook_name", "status", "rows_processed"]}'),
    (2, "log_validation", "Validation logging - one row per expectation result with GX metadata",
     '{"params": ["validation_result"]}')
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_store_schema = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_type", StringType(), False),
    StructField("auth_method", StringType(), True),
    StructField("key_vault_url", StringType(), True),
    StructField("secret_name", StringType(), True),
    StructField("base_url", StringType(), True),
    StructField("handler_function", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

source_store_data = [
    (1, "youtube_api", "rest_api", "api_key",
     "https://av01-akv-restapis-keys.vault.azure.net/",
     "data-v3-api-key",
     "https://www.googleapis.com/youtube/v3",
     "ingest_youtube",
     "YouTube Data API v3 - Channel stats, videos, playlists",
     None, None),
    (2, "brreg_enheter", "rest_api", "none",
     None, None,
     "https://data.brreg.no/enhetsregisteret/api",
     "ingest_brreg",
     "BRREG Enhetsregisteret - Full load og inkrementell via oppdateringsid",
     None, None),
    (3, "siva_meldingslogg", "sharepoint_list", "oauth_spn",
     "https://av01-akv-restapis-keys.vault.azure.net/",
     "sharepoint-client-secret",
     "https://sivanorgeas.sharepoint.com/sites/Dataplattform",
     "ingest_sharepoint_list",
     "SharePoint meldingslogg - Hendelser og meldinger per bedrift",
     None, None),
    (4, "siva_regnskapbedrifter", "sharepoint_excel", "oauth_spn",
     "https://av01-akv-restapis-keys.vault.azure.net/",
     "sharepoint-client-secret",
     "https://sivanorgeas.sharepoint.com/sites/Dataplattform",
     "ingest_sharepoint_excel",
     "SharePoint Excel - Regnskapsdata per bedrift per år",
     None, None)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transform_store_schema = StructType([
    StructField("transform_id", IntegerType(), False),
    StructField("function_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

transform_store_data = [
    (1, "filter_nulls",
     "Remove rows where specified columns are null",
     '{"params": ["columns"]}'),
    (2, "dedupe_by_window",
     "Deduplicate using window function - keeps most recent by order column",
     '{"params": ["partition_cols", "order_col", "order_desc"]}'),
    (3, "rename_columns",
     "Rename columns according to mapping",
     '{"params": ["column_mapping"]}'),
    (4, "add_literal_columns",
     "Add columns with literal/static values",
     '{"params": ["columns"]}'),
    (5, "generate_surrogate_key",
     "Generate surrogate key using row_number over window, starting from max existing ID",
     '{"params": ["key_column_name", "order_by_col", "max_from_table"]}'),
    (6, "lookup_join",
     "Join to lookup/dimension table to get surrogate key or other columns",
     '{"params": ["lookup_table", "source_key", "lookup_key", "select_cols"]}'),
    (8, "generate_date_dimension",
     "Generate date dimension from year range, ignores source df",
     '{"params": ["start_year", "end_year"]}'),
    (9, "add_computed_columns",
     "Add columns with computed Spark SQL expressions",
     '{"params": ["columns"]}')
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Expectation Store: GX expectation types for validation (referenced by expectation_id)
expectation_store_schema = StructType([
    StructField("expectation_id", IntegerType(), False),
    StructField("expectation_name", StringType(), False),
    StructField("gx_method", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

expectation_store_data = [
    (1, "not_null", "ExpectColumnValuesToNotBeNull",
     "Validate that column contains no null values",
     '{"params": ["column"]}'),
    (2, "unique", "ExpectColumnValuesToBeUnique",
     "Validate that column contains only unique values",
     '{"params": ["column"]}'),
    (3, "value_in_set", "ExpectColumnValuesToBeInSet",
     "Validate that column values are within a defined set",
     '{"params": ["column", "value_set"]}'),
    (4, "values_increasing", "ExpectColumnValuesToBeIncreasing",
     "Validate that column values are strictly increasing",
     '{"params": ["column"]}'),
    (5, "compound_unique", "ExpectCompoundColumnsToBeUnique",
     "Validate that combination of columns is unique",
     '{"params": ["column_list"]}'),
    (6, "is_null", "ExpectColumnValuesToBeNull",
     "Validate that column contains only null values",
     '{"params": ["column"]}')
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_mappings_schema = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("column_order", IntegerType(), False),
    StructField("source_column", StringType(), False),
    StructField("target_column", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("description", StringType(), True)
])

column_mappings_data = [
    # ── YouTube Channels ──────────────────────────────────────────────────────
    ("youtube_channels", 1, "id",                          "channel_id",          "string",            "YouTube channel ID"),
    ("youtube_channels", 2, "snippet.title",               "channel_name",        "string",            "Channel display name"),
    ("youtube_channels", 3, "snippet.description",         "channel_description", "string",            "Channel description"),
    ("youtube_channels", 4, "statistics.viewCount",        "view_count",          "int",               "Total channel views"),
    ("youtube_channels", 5, "statistics.subscriberCount",  "subscriber_count",    "int",               "Subscriber count"),
    ("youtube_channels", 6, "statistics.videoCount",       "video_count",         "int",               "Number of videos"),
    ("youtube_channels", 7, "_loading_ts",                 "loading_TS",          "current_timestamp", "Load timestamp"),
    # ── YouTube Playlist Items ────────────────────────────────────────────────
    ("youtube_playlist_items", 1, "snippet.channelId",                   "channel_id",       "string",            "Channel ID"),
    ("youtube_playlist_items", 2, "snippet.resourceId.videoId",          "video_id",         "string",            "Video ID"),
    ("youtube_playlist_items", 3, "snippet.title",                       "video_title",      "string",            "Video title"),
    ("youtube_playlist_items", 4, "snippet.description",                 "video_description","string",            "Video description"),
    ("youtube_playlist_items", 5, "snippet.thumbnails.high.url",         "thumbnail_url",    "string",            "Thumbnail URL"),
    ("youtube_playlist_items", 6, "snippet.publishedAt",                 "video_publish_TS", "timestamp",         "Video publish timestamp"),
    ("youtube_playlist_items", 7, "_loading_ts",                         "loading_TS",       "current_timestamp", "Load timestamp"),
    # ── YouTube Videos ────────────────────────────────────────────────────────
    ("youtube_videos", 1, "id",                       "video_id",           "string",            "Video ID"),
    ("youtube_videos", 2, "statistics.viewCount",     "video_view_count",   "int",               "Video view count"),
    ("youtube_videos", 3, "statistics.likeCount",     "video_like_count",   "int",               "Video like count"),
    ("youtube_videos", 4, "statistics.commentCount",  "video_comment_count","int",               "Video comment count"),
    ("youtube_videos", 5, "_loading_ts",              "loading_TS",         "current_timestamp", "Load timestamp"),
    # ── BRREG Enheter ─────────────────────────────────────────────────────────
    ("brreg_enheter",  1, "organisasjonsnummer",                       "organisasjonsnummer",                       "string",            "Organisasjonsnummer (nøkkel)"),
    ("brreg_enheter",  2, "navn",                                       "navn",                                       "string",            "Foretaksnavn"),
    ("brreg_enheter",  3, "organisasjonsform.kode",                     "organisasjonsform_kode",                     "string",            "Organisasjonsform kode"),
    ("brreg_enheter",  4, "organisasjonsform.beskrivelse",              "organisasjonsform_beskrivelse",              "string",            "Organisasjonsform beskrivelse"),
    ("brreg_enheter",  5, "institusjonellSektorkode.kode",              "institusjonellSektorkode_kode",              "string",            "Institusjonell sektorkode"),
    ("brreg_enheter",  6, "institusjonellSektorkode.beskrivelse",       "institusjonellSektorkode_beskrivelse",       "string",            "Institusjonell sektorkode beskrivelse"),
    ("brreg_enheter",  7, "registreringsdatoEnhetsregisteret",          "registreringsdatoEnhetsregisteret",          "date",              "Registreringsdato"),
    ("brreg_enheter",  8, "stiftelsesdato",                             "stiftelsesdato",                             "date",              "Stiftelsesdato"),
    ("brreg_enheter",  9, "konkursdato",                                "konkursdato",                                "date",              "Konkursdato"),
    ("brreg_enheter", 10, "underAvviklingDato",                         "underAvviklingDato",                         "date",              "Under avvikling dato"),
    ("brreg_enheter", 11, "sisteInnsendteAarsregnskap",                 "sisteInnsendteAarsregnskap",                 "int",               "Siste innsendte årsregnskap"),
    ("brreg_enheter", 12, "antallAnsatte",                              "antallAnsatte",                              "int",               "Antall ansatte"),
    ("brreg_enheter", 13, "harRegistrertAntallAnsatte",                 "harRegistrertAntallAnsatte",                 "boolean",           "Har registrert antall ansatte"),
    ("brreg_enheter", 14, "registrertIMvaregisteret",                   "registrertIMvaregisteret",                   "boolean",           "Registrert i MVA"),
    ("brreg_enheter", 15, "registrertIForetaksregisteret",              "registrertIForetaksregisteret",              "boolean",           "Registrert i Foretaksregisteret"),
    ("brreg_enheter", 16, "registrertIFrivillighetsregisteret",         "registrertIFrivillighetsregisteret",         "boolean",           "Registrert i Frivillighetsregisteret"),
    ("brreg_enheter", 17, "registrertIPartiregisteret",                 "registrertIPartiregisteret",                 "boolean",           "Registrert i Partiregisteret"),
    ("brreg_enheter", 18, "konkurs",                                    "konkurs",                                    "boolean",           "Er under konkurs"),
    ("brreg_enheter", 19, "underAvvikling",                             "underAvvikling",                             "boolean",           "Er under avvikling"),
    ("brreg_enheter", 20, "underTvangsavviklingEllerTvangsopplosning",  "underTvangsavviklingEllerTvangsopplosning",  "boolean",           "Under tvangsavvikling"),
    ("brreg_enheter", 21, "erIKonsern",                                 "erIKonsern",                                 "boolean",           "Er i konsern"),
    ("brreg_enheter", 22, "overordnetEnhet",                            "overordnetEnhet",                            "string",            "Overordnet enhet"),
    ("brreg_enheter", 23, "hjemmeside",                                 "hjemmeside",                                 "string",            "Hjemmeside"),
    ("brreg_enheter", 24, "epostadresse",                               "epostadresse",                               "string",            "E-postadresse"),
    ("brreg_enheter", 25, "telefon",                                    "telefon",                                    "string",            "Telefon"),
    ("brreg_enheter", 26, "mobil",                                      "mobil",                                      "string",            "Mobil"),
    ("brreg_enheter", 27, "maalform",                                   "maalform",                                   "string",            "Målform"),
    ("brreg_enheter", 28, "naeringskode1.kode",                         "naeringskode1_kode",                         "string",            "Næringskode 1"),
    ("brreg_enheter", 29, "naeringskode1.beskrivelse",                  "naeringskode1_beskrivelse",                  "string",            "Næringskode 1 beskrivelse"),
    ("brreg_enheter", 30, "naeringskode2.kode",                         "naeringskode2_kode",                         "string",            "Næringskode 2"),
    ("brreg_enheter", 31, "naeringskode2.beskrivelse",                  "naeringskode2_beskrivelse",                  "string",            "Næringskode 2 beskrivelse"),
    ("brreg_enheter", 32, "naeringskode3.kode",                         "naeringskode3_kode",                         "string",            "Næringskode 3"),
    ("brreg_enheter", 33, "naeringskode3.beskrivelse",                  "naeringskode3_beskrivelse",                  "string",            "Næringskode 3 beskrivelse"),
    ("brreg_enheter", 34, "forretningsadresse.kommune",                 "forretningsadresse_kommune",                 "string",            "Kommune"),
    ("brreg_enheter", 35, "forretningsadresse.kommunenummer",           "forretningsadresse_kommunenummer",           "string",            "Kommunenummer"),
    ("brreg_enheter", 36, "forretningsadresse.postnummer",              "forretningsadresse_postnummer",              "string",            "Postnummer"),
    ("brreg_enheter", 37, "forretningsadresse.poststed",                "forretningsadresse_poststed",                "string",            "Poststed"),
    ("brreg_enheter", 38, "forretningsadresse.landkode",                "forretningsadresse_landkode",                "string",            "Landkode"),
    ("brreg_enheter", 39, "kapital.belop",                              "kapital_belop",                              "double",            "Aksjekapital beløp"),
    ("brreg_enheter", 40, "kapital.valuta",                             "kapital_valuta",                             "string",            "Aksjekapital valuta"),
    ("brreg_enheter", 41, "kapital.antallAksjer",                       "kapital_antallAksjer",                       "bigint",            "Antall aksjer"),
    ("brreg_enheter", 42, "underlagtLovgivningLandKode",                "underlagtLovgivningLandKode",                "string",            "Underlagt lovgivning landkode"),
    ("brreg_enheter", 43, "underlagtLovgivningLand",                    "underlagtLovgivningLand",                    "string",            "Underlagt lovgivning land"),
    ("brreg_enheter", 44, "lastet_tidspunkt",                           "lastet_tidspunkt",                           "timestamp",         "Lastet tidspunkt fra BRREG"),
    ("brreg_enheter", 45, "_loading_ts",                                "_loading_ts",                                "current_timestamp", "Pipeline lastet tidspunkt"),
    # ── SharePoint Meldingslogg ───────────────────────────────────────────────
    ("sp_meldingslogg",  1, "sp_id",          "sp_id",          "string",            "SharePoint item ID (nøkkel)"),
    ("sp_meldingslogg",  2, "orgnr",           "orgnr",          "string",            "Organisasjonsnummer"),
    ("sp_meldingslogg",  3, "selskapsnavn",    "selskapsnavn",   "string",            "Selskapsnavn"),
    ("sp_meldingslogg",  4, "hendelsesdato",   "hendelsesdato",  "date",              "Dato for hendelsen"),
    ("sp_meldingslogg",  5, "meldingstype",    "meldingstype",   "string",            "Type melding"),
    ("sp_meldingslogg",  6, "meldingsinnhold", "meldingsinnhold","string",            "Innhold i meldingen"),
    ("sp_meldingslogg",  7, "url",             "url",            "string",            "URL/lenke"),
    ("sp_meldingslogg",  8, "created_ts",      "created_ts",     "timestamp",         "Opprettet tidspunkt"),
    ("sp_meldingslogg",  9, "modified_ts",     "modified_ts",    "timestamp",         "Sist endret tidspunkt"),
    ("sp_meldingslogg", 10, "_loading_ts",     "_loading_ts",    "current_timestamp", "Pipeline lastet tidspunkt"),
    # ── SharePoint Regnskapbedrifter ──────────────────────────────────────────
    ("sp_regnskapbedrifter",  1, "orgnr",                        "orgnr",                        "string",            "Organisasjonsnummer (nøkkel)"),
    ("sp_regnskapbedrifter",  2, "aarstall",                      "aarstall",                      "int",               "Regnskapsår (nøkkel)"),
    ("sp_regnskapbedrifter",  3, "sum_salgsinntekter",            "sum_salgsinntekter",            "bigint",            "Sum salgsinntekter"),
    ("sp_regnskapbedrifter",  4, "sum_driftsinntekter",           "sum_driftsinntekter",           "bigint",            "Sum driftsinntekter"),
    ("sp_regnskapbedrifter",  5, "andre_driftsinntekter",         "andre_driftsinntekter",         "bigint",            "Andre driftsinntekter"),
    ("sp_regnskapbedrifter",  6, "sum_finansinntekter",           "sum_finansinntekter",           "bigint",            "Sum finansinntekter"),
    ("sp_regnskapbedrifter",  7, "finanskostnader",               "finanskostnader",               "bigint",            "Finanskostnader"),
    ("sp_regnskapbedrifter",  8, "vareforbruk",                   "vareforbruk",                   "bigint",            "Vareforbruk"),
    ("sp_regnskapbedrifter",  9, "loennskostnader",               "loennskostnader",               "bigint",            "Lønnskostnader"),
    ("sp_regnskapbedrifter", 10, "herav_kun_loenn",               "herav_kun_loenn",               "bigint",            "Herav kun lønn"),
    ("sp_regnskapbedrifter", 11, "avskr_varige_driftsmidler",     "avskr_varige_driftsmidler",     "bigint",            "Avskrivninger varige driftsmidler"),
    ("sp_regnskapbedrifter", 12, "andre_driftskostnader",         "andre_driftskostnader",         "bigint",            "Andre driftskostnader"),
    ("sp_regnskapbedrifter", 13, "sum_driftskostnader",           "sum_driftskostnader",           "bigint",            "Sum driftskostnader"),
    ("sp_regnskapbedrifter", 14, "driftsresultat",                "driftsresultat",                "bigint",            "Driftsresultat"),
    ("sp_regnskapbedrifter", 15, "ordinaert_resultat_foer_skatt", "ordinaert_resultat_foer_skatt", "bigint",            "Ordinært resultat før skatt"),
    ("sp_regnskapbedrifter", 16, "ekstraordinaere_poster",        "ekstraordinaere_poster",        "bigint",            "Ekstraordinære poster"),
    ("sp_regnskapbedrifter", 17, "aarsresultat",                  "aarsresultat",                  "bigint",            "Årsresultat"),
    ("sp_regnskapbedrifter", 18, "sum_varelager",                 "sum_varelager",                 "bigint",            "Sum varelager"),
    ("sp_regnskapbedrifter", 19, "sum_eiendeler",                 "sum_eiendeler",                 "bigint",            "Sum eiendeler"),
    ("sp_regnskapbedrifter", 20, "sum_omloepsmidler",             "sum_omloepsmidler",             "bigint",            "Sum omløpsmidler"),
    ("sp_regnskapbedrifter", 21, "kundefordringer",               "kundefordringer",               "bigint",            "Kundefordringer"),
    ("sp_regnskapbedrifter", 22, "kasse_bank_post",               "kasse_bank_post",               "bigint",            "Kasse/bank/post"),
    ("sp_regnskapbedrifter", 23, "andre_finansielle_instr",       "andre_finansielle_instr",       "bigint",            "Andre finansielle instrumenter"),
    ("sp_regnskapbedrifter", 24, "sum_investeringer",             "sum_investeringer",             "bigint",            "Sum investeringer"),
    ("sp_regnskapbedrifter", 25, "goodwill",                      "goodwill",                      "bigint",            "Goodwill"),
    ("sp_regnskapbedrifter", 26, "utsatt_skattefordel",           "utsatt_skattefordel",           "bigint",            "Utsatt skattefordel"),
    ("sp_regnskapbedrifter", 27, "forskning_og_utvikling",        "forskning_og_utvikling",        "bigint",            "Forskning og utvikling"),
    ("sp_regnskapbedrifter", 28, "sum_egenkapital",               "sum_egenkapital",               "bigint",            "Sum egenkapital"),
    ("sp_regnskapbedrifter", 29, "aksje_selskapskapital",         "aksje_selskapskapital",         "bigint",            "Aksje/selskapskapital"),
    ("sp_regnskapbedrifter", 30, "utbytte",                       "utbytte",                       "bigint",            "Utbytte"),
    ("sp_regnskapbedrifter", 31, "avsatt_utbytte",                "avsatt_utbytte",                "bigint",            "Avsatt utbytte"),
    ("sp_regnskapbedrifter", 32, "ekstraordinaert_utbytte",       "ekstraordinaert_utbytte",       "bigint",            "Ekstraordinært utbytte"),
    ("sp_regnskapbedrifter", 33, "overkursfond",                  "overkursfond",                  "bigint",            "Overkursfond"),
    ("sp_regnskapbedrifter", 34, "sum_innskutt_egenkapital",      "sum_innskutt_egenkapital",      "bigint",            "Sum innskutt egenkapital"),
    ("sp_regnskapbedrifter", 35, "sum_opptjent_kapital",          "sum_opptjent_kapital",          "bigint",            "Sum opptjent kapital"),
    ("sp_regnskapbedrifter", 36, "sum_gjeld",                     "sum_gjeld",                     "bigint",            "Sum gjeld"),
    ("sp_regnskapbedrifter", 37, "sum_kortsiktig_gjeld",          "sum_kortsiktig_gjeld",          "bigint",            "Sum kortsiktig gjeld"),
    ("sp_regnskapbedrifter", 38, "leverandoergjeld",              "leverandoergjeld",              "bigint",            "Leverandørgjeld"),
    ("sp_regnskapbedrifter", 39, "lederloenn",                    "lederloenn",                    "bigint",            "Lederlønn"),
    ("sp_regnskapbedrifter", 40, "pensjonskostnader",             "pensjonskostnader",             "bigint",            "Pensjonskostnader"),
    ("sp_regnskapbedrifter", 41, "husleiekostnader",              "husleiekostnader",              "bigint",            "Husleiekostnader"),
    ("sp_regnskapbedrifter", 42, "beholdningsendringer",          "beholdningsendringer",          "bigint",            "Beholdningsendringer"),
    ("sp_regnskapbedrifter", 43, "_loading_ts",                   "_loading_ts",                   "current_timestamp", "Pipeline lastet tidspunkt")
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instruction Table Definitions
# Runtime instructions that control pipeline behavior.

# CELL ********************

ingestion_schema = StructType([
    StructField("ingestion_id", IntegerType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("endpoint_path", StringType(), True),
    StructField("landing_path", StringType(), False),
    StructField("request_params", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

ingestion_data = [
    # YouTube
    (1, 1, "/channels", "youtube_data_v3/channels/",
     '{"part": "snippet,statistics,contentDetails", "id": "UCrvoIYkzS-RvCEb0x7wfmwQ"}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None),
    (2, 1, "/playlistItems", "youtube_data_v3/playlistItems/",
     '{"part": "snippet", "maxResults": 50, "playlistId": "UUrvoIYkzS-RvCEb0x7wfmwQ"}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None),
    (3, 1, "/videos", "youtube_data_v3/videos/",
     '{"part": "statistics", "maxResults": 50}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None),
    # BRREG Enheter - inkrementell via oppdateringsid
    (4, 2, "/oppdateringer/enheter", "brreg/enheter/",
     '{"run_mode": "auto"}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None),
    # SharePoint Meldingslogg
    (5, 3, "data-meldingslogg", "sharepoint/meldingslogg/",
     '{}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None),
    # SharePoint Regnskapbedrifter Excel
    (6, 4, "Regnskapbedrifter.xlsx", "sharepoint/regnskapbedrifter/",
     '{"sheet_name": 0, "header_row": 0, "strip_year_suffix": true}',
     True, 1, "data_pipeline", "nb-av01-0-ingest-api", None, None)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loading_schema = StructType([
    StructField("loading_instr_id", IntegerType(), False),
    StructField("loading_id", IntegerType(), False),
    StructField("source_path", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("target_layer", StringType(), False),
    StructField("key_columns", StringType(), False),
    StructField("load_params", StringType(), True),
    StructField("merge_condition", StringType(), True),
    StructField("merge_type", StringType(), True),
    StructField("merge_columns", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

loading_data = [
    # YouTube
    (1, 1, "Files/youtube_data_v3/channels/", "raw", "youtube/channel", "bronze",
     '["channel_id"]',
     '{"column_mapping_id": "youtube_channels"}',
     "target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None),
    (2, 1, "Files/youtube_data_v3/playlistItems/", "raw", "youtube/playlist_items", "bronze",
     '["video_id"]',
     '{"column_mapping_id": "youtube_playlist_items"}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None),
    (3, 1, "Files/youtube_data_v3/videos/", "raw", "youtube/videos", "bronze",
     '["video_id"]',
     '{"column_mapping_id": "youtube_videos"}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None),
    # BRREG Enheter
    (4, 2, "Files/brreg/enheter/", "raw", "brreg/enheter", "bronze",
     '["organisasjonsnummer"]',
     '{"column_mapping_id": "brreg_enheter", "required_fields": "organisasjonsnummer,navn,organisasjonsform_kode", "not_null_fields": "organisasjonsnummer,navn,organisasjonsform_kode", "orgnr_column": "organisasjonsnummer"}',
     "target.organisasjonsnummer = source.organisasjonsnummer",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None),
    # SharePoint Meldingslogg
    (5, 3, "Files/sharepoint/meldingslogg/", "raw", "sharepoint/meldingslogg", "bronze",
     '["sp_id"]',
     '{"column_mapping_id": "sp_meldingslogg", "required_fields": "sp_id,orgnr,selskapsnavn,hendelsesdato,meldingstype", "not_null_fields": "sp_id,orgnr,selskapsnavn,hendelsesdato,meldingstype", "orgnr_column": "orgnr"}',
     "target.sp_id = source.sp_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None),
    # SharePoint Regnskapbedrifter
    (6, 4, "Files/sharepoint/regnskapbedrifter/", "raw", "sharepoint/regnskapbedrifter", "bronze",
     '["orgnr", "aarstall"]',
     '{"column_mapping_id": "sp_regnskapbedrifter", "required_fields": "orgnr,aarstall", "not_null_fields": "orgnr,aarstall", "orgnr_column": "orgnr"}',
     "target.orgnr = source.orgnr AND target.aarstall = source.aarstall",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-1-load", None, None)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformations_schema = StructType([
    StructField("transform_instr_id", IntegerType(), False),
    StructField("source_table", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("dest_table", StringType(), False),
    StructField("dest_layer", StringType(), False),
    StructField("transform_pipeline", StringType(), False),
    StructField("transform_params", StringType(), True),
    StructField("merge_condition", StringType(), True),
    StructField("merge_type", StringType(), True),
    StructField("merge_columns", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

transformations_data = [
    # ── Bronze → Silver: YouTube ──────────────────────────────────────────────
    (1, "youtube/channel", "bronze", "youtube/channel_stats", "silver",
     "[1, 2]",
     '{"1": {"columns": ["channel_id"]}, "2": {"partition_cols": ["channel_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
     "target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    (2, "youtube/playlist_items", "bronze", "youtube/videos", "silver",
     "[1, 2]",
     '{"1": {"columns": ["video_id", "video_title"]}, "2": {"partition_cols": ["video_id"], "order_col": "loading_TS", "order_desc": true}}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    (3, "youtube/videos", "bronze", "youtube/video_statistics", "silver",
     "[1, 2]",
     '{"1": {"columns": ["video_id"]}, "2": {"partition_cols": ["video_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    # ── Silver → Gold: YouTube ────────────────────────────────────────────────
    (10, "youtube/channel_stats", "silver", "marketing/channels", "gold",
     "[4, 3]",
     '{"4": {"columns": {"channel_surrogate_id": 1, "channel_platform": "youtube"}}, "3": {"column_mapping": {"channel_name": "channel_account_name", "channel_description": "channel_account_description", "subscriber_count": "channel_total_subscribers", "video_count": "channel_total_assets", "view_count": "channel_total_views", "loading_TS": "modified_TS"}}}',
     "target.channel_surrogate_id = source.channel_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    (11, "youtube/videos", "silver", "marketing/assets", "gold",
     "[4, 3, 5]",
     '{"4": {"columns": {"channel_surrogate_id": 1}}, "3": {"column_mapping": {"video_id": "asset_natural_id", "video_title": "asset_title", "video_description": "asset_text", "video_publish_TS": "asset_publish_date", "loading_TS": "modified_TS"}}, "5": {"key_column_name": "asset_surrogate_id", "order_by_col": "asset_publish_date", "natural_key": "asset_natural_id", "max_from_table": "marketing/assets"}}',
     "target.asset_natural_id = source.asset_natural_id",
     "specific_columns",
     '{"update": ["asset_title", "asset_text", "asset_publish_date", "modified_TS"], "insert": ["asset_surrogate_id", "asset_natural_id", "channel_surrogate_id", "asset_title", "asset_text", "asset_publish_date", "modified_TS"]}',
     True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    (12, "youtube/video_statistics", "silver", "marketing/asset_stats", "gold",
     "[6, 3, 4]",
     '{"6": {"lookup_table": "marketing/assets", "source_key": "video_id", "lookup_key": "asset_natural_id", "select_cols": ["asset_surrogate_id"]}, "3": {"column_mapping": {"video_view_count": "asset_total_views", "video_like_count": "asset_total_likes", "video_comment_count": "asset_total_comments", "loading_TS": "modified_TS"}}, "4": {"columns": {"asset_total_impressions": None}}}',
     "target.asset_surrogate_id = source.asset_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)",
     "specific_columns",
     '{"update": ["asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"], "insert": ["asset_surrogate_id", "asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"]}',
     True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    # ── Gold: dim_dato (synthetic) ────────────────────────────────────────────
    (19, "_synthetic", "none", "siva/dim_dato", "gold",
     "[8]",
     '{"8": {"start_year": 2015, "end_year": 2030}}',
     "target.dato = source.dato",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    # ── Bronze → Silver: BRREG og SharePoint ─────────────────────────────────
    (20, "brreg/enheter", "bronze", "brreg/enheter", "silver",
     "[2, 3, 4]",
     '{"2": {"partition_cols": ["organisasjonsnummer"], "order_col": "_loading_ts", "order_desc": true}, "3": {"column_mapping": {"registreringsdatoEnhetsregisteret": "registreringsdato"}}, "4": {"columns": {"_kilde": "brreg"}}}',
     "target.organisasjonsnummer = source.organisasjonsnummer",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    (21, "sharepoint/meldingslogg", "bronze", "sharepoint/meldingslogg", "silver",
     "[2, 4]",
     '{"2": {"partition_cols": ["sp_id"], "order_col": "modified_ts", "order_desc": true}, "4": {"columns": {"_kilde": "sharepoint"}}}',
     "target.sp_id = source.sp_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    (22, "sharepoint/regnskapbedrifter", "bronze", "sharepoint/regnskapbedrifter", "silver",
     "[2, 4]",
     '{"2": {"partition_cols": ["orgnr", "aarstall"], "order_col": "_loading_ts", "order_desc": true}, "4": {"columns": {"_kilde": "sharepoint"}}}',
     "target.orgnr = source.orgnr AND target.aarstall = source.aarstall",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-2-clean", None, None),
    # ── Silver → Gold: BRREG og SharePoint ───────────────────────────────────
    (23, "brreg/enheter", "silver", "siva/dim_bedrift", "gold",
     "[5, 4]",
     '{"5": {"key_column_name": "bedrift_surrogate_id", "order_by_col": "organisasjonsnummer", "natural_key": "organisasjonsnummer", "max_from_table": "siva/dim_bedrift"}, "4": {"columns": {"er_aktiv": True}}}',
     "target.organisasjonsnummer = source.organisasjonsnummer",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    (24, "sharepoint/meldingslogg", "silver", "siva/fact_melding", "gold",
     "[5, 6, 6]",
     '{"5": {"key_column_name": "melding_surrogate_id", "order_by_col": "sp_id", "natural_key": "sp_id", "max_from_table": "siva/fact_melding"}, "6": [{"lookup_table": "siva/dim_bedrift", "source_key": "orgnr", "lookup_key": "organisasjonsnummer", "select_cols": ["bedrift_surrogate_id"]}, {"lookup_table": "siva/dim_dato", "source_key": "hendelsesdato", "lookup_key": "dato", "select_cols": ["dato_surrogate_id"]}]}',
     "target.sp_id = source.sp_id",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-3-model", None, None),
    (25, "sharepoint/regnskapbedrifter", "silver", "siva/fact_regnskap", "gold",
     "[5, 6, 9]",
     '{"5": {"key_column_name": "regnskap_surrogate_id", "order_by_col": "aarstall", "natural_key": ["orgnr", "aarstall"], "max_from_table": "siva/fact_regnskap"}, "6": {"lookup_table": "siva/dim_bedrift", "source_key": "orgnr", "lookup_key": "organisasjonsnummer", "select_cols": ["bedrift_surrogate_id"]}, "9": {"columns": {"regnskapsaar_dato_id": "cast(concat(cast(aarstall as string), 1231) as int)"}}}',
     "target.orgnr = source.orgnr AND target.aarstall = source.aarstall",
     "update_all", None, True, 1, "data_pipeline", "nb-av01-3-model", None, None)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validations_schema = StructType([
    StructField("validation_instr_id", IntegerType(), False),
    StructField("target_table", StringType(), False),
    StructField("target_layer", StringType(), False),
    StructField("expectation_id", IntegerType(), False),
    StructField("column_name", StringType(), True),
    StructField("validation_params", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

validations_data = [
    # ── YouTube Gold validasjoner ─────────────────────────────────────────────
    (1, "marketing/channels", "gold", 3, "channel_surrogate_id", '{"value_set": [1]}', "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (2, "marketing/channels", "gold", 4, "channel_total_views",  None,                 "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (3, "marketing/assets",   "gold", 2, "asset_surrogate_id",   None,                 "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (4, "marketing/assets",   "gold", 1, "asset_natural_id",     None,                 "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (5, "marketing/assets",   "gold", 1, "asset_publish_date",   None,                 "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (6, "marketing/assets",   "gold", 5, None, '{"column_list": ["asset_title", "asset_surrogate_id"]}', "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (7, "marketing/asset_stats", "gold", 6, "asset_total_impressions", None,           "error", True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    # ── SIVA Gold validasjoner ────────────────────────────────────────────────
    (8,  "siva/dim_bedrift",  "gold", 1, "bedrift_surrogate_id",  None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (9,  "siva/dim_bedrift",  "gold", 1, "organisasjonsnummer",   None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (10, "siva/dim_bedrift",  "gold", 2, "bedrift_surrogate_id",  None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (11, "siva/dim_dato",     "gold", 1, "dato_surrogate_id",     None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (12, "siva/dim_dato",     "gold", 2, "dato_surrogate_id",     None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (13, "siva/fact_melding", "gold", 1, "melding_surrogate_id",  None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (14, "siva/fact_melding", "gold", 1, "sp_id",                 None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (15, "siva/fact_melding", "gold", 2, "melding_surrogate_id",  None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (16, "siva/fact_regnskap","gold", 1, "regnskap_surrogate_id", None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (17, "siva/fact_regnskap","gold", 1, "orgnr",                 None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None),
    (18, "siva/fact_regnskap","gold", 2, "regnskap_surrogate_id", None, "error",   True, 2, "data_pipeline", "nb-av01-4-validate", None, None)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seed Execution

# CELL ********************

def write_seed_table(table_name: str, schema: StructType, data: list) -> int:
    """
    Write seed data to a SQL metadata table.

    Args:
        table_name: Fully qualified table name (schema.table)
        schema: PySpark StructType defining the table schema
        data: List of tuples containing row data

    Returns:
        Number of rows written
    """
    if not data:
        print(f"  Skipping {table_name} - no data to write")
        return 0

    df = spark.createDataFrame(data, schema)
    df.write.mode("append").option("url", METADATA_DB_URL).mssql(table_name)
    print(f"  Wrote {len(data)} rows to {table_name}")
    return len(data)

'''
def table_has_data(table_name: str) -> bool:
    """Check if a table already has data."""
    df = spark.read.option("url", METADATA_DB_URL).mssql(table_name)
    return df.count() > 0
'''
def table_has_data(table_name: str) -> bool:
    try:
        df = spark.read.option("url", METADATA_DB_URL).mssql(table_name)
        return df.count() > 0
    except Exception:
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.option("url", METADATA_DB_URL).mssql("INFORMATION_SCHEMA.TABLES").show(100, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if table_has_data("metadata.log_store"):
    print("Seed data already exists - skipping seeding")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if data already exists (use metadata.log_store as indicator)
if table_has_data("metadata.log_store"):
    print("Seed data already exists - skipping seeding")
else:
    # Seed instruction tables
    instr_rows = 0
    instr_rows += write_seed_table("instructions.ingestion", ingestion_schema, ingestion_data)
    instr_rows += write_seed_table("instructions.loading", loading_schema, loading_data)
    instr_rows += write_seed_table("instructions.transformations", transformations_schema, transformations_data)
    instr_rows += write_seed_table("instructions.validations", validations_schema, validations_data)

    # Seed metadata store tables
    total_rows = 0
    total_rows += write_seed_table("metadata.log_store", log_store_schema, log_store_data)
    total_rows += write_seed_table("metadata.source_store", source_store_schema, source_store_data)
    total_rows += write_seed_table("metadata.loading_store", loading_store_schema, loading_store_data)
    total_rows += write_seed_table("metadata.transform_store", transform_store_schema, transform_store_data)
    total_rows += write_seed_table("metadata.expectation_store", expectation_store_schema, expectation_store_data)
    total_rows += write_seed_table("metadata.column_mappings", column_mappings_schema, column_mappings_data)

    print(f"Seeded {instr_rows} instruction rows and {total_rows} metadata rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sjekk om tabellene eksisterer
tables_to_check = [
    "metadata.log_store",
    "metadata.source_store", 
    "instructions.ingestion"
]

for table in tables_to_check:
    try:
        df = spark.read.option("url", METADATA_DB_URL).mssql(table)
        count = df.count()
        print(f"✓ {table}: {count} rader")
    except Exception as e:
        print(f"✗ {table}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
