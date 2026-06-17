# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-lhcreate-all
# **Purpose**: Create all lakehouse schemas and tables, then configure infrastructure.
# 
# **When to run**:
# - New deployment (dev / test / prod)
# - New tables or schemas added
# - Infrastructure configuration changes
# 
# **Sections**:
# 1. Create lakehouse schemas and tables (Bronze / Silver / Gold)
# 2. Activate Delta Change Data Feed (CDF)
# 3. Upgrade Delta protocol (ancient datetime support)
# 4. Parquet datetime config
# 5. Status summary

# MARKDOWN ********************

# ## Setup

# CELL ********************

import notebookutils

variables         = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
lh_workspace_name = variables.LH_WORKSPACE_NAME

CATALOG = lh_workspace_name
BRONZE  = variables.BRONZE_LH_NAME
SILVER  = variables.SILVER_LH_NAME
GOLD    = variables.GOLD_LH_NAME
ADMIN    = variables.ADMIN_LH_NAME

print(f"Workspace : {lh_workspace_name}")
print(f"Bronze    : {BRONZE}")
print(f"Silver    : {SILVER}")
print(f"Gold      : {GOLD}")
print(f"Admin     : {ADMIN}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 – Lakehouse Schema Definitions

# CELL ********************

YOUTUBE_METADATA = {
    'bronze': {
        'name_variable': 'BRONZE_LH_NAME',
        'schemas': ['youtube'],
        'tables': {
            'youtube.channel': '''
                channel_id          STRING,
                channel_name        STRING,
                channel_description STRING,
                view_count          INT,
                subscriber_count    INT,
                video_count         INT,
                loading_TS          TIMESTAMP''',
            'youtube.playlist_items': '''
                channel_id          STRING,
                video_id            STRING,
                video_title         STRING,
                video_description   STRING,
                thumbnail_url       STRING,
                video_publish_TS    TIMESTAMP,
                loading_TS          TIMESTAMP''',
            'youtube.videos': '''
                video_id            STRING,
                video_view_count    INT,
                video_like_count    INT,
                video_comment_count INT,
                loading_TS          TIMESTAMP''',
        }
    },
    'silver': {
        'name_variable': 'SILVER_LH_NAME',
        'schemas': ['youtube'],
        'tables': {
            'youtube.channel_stats': '''
                channel_id          STRING,
                channel_name        STRING,
                channel_description STRING,
                view_count          INT,
                subscriber_count    INT,
                video_count         INT,
                loading_TS          TIMESTAMP''',
            'youtube.videos': '''
                channel_id          STRING,
                video_id            STRING,
                video_title         STRING,
                video_description   STRING,
                thumbnail_url       STRING,
                video_publish_TS    TIMESTAMP,
                loading_TS          TIMESTAMP''',
            'youtube.video_statistics': '''
                video_id            STRING,
                video_view_count    INT,
                video_like_count    INT,
                video_comment_count INT,
                loading_TS          TIMESTAMP''',
        }
    },
    "gold": {
        "name_variable": "GOLD_LH_NAME",
        "schemas": ["marketing"],
        "tables": {
            "marketing.channels": """
                channel_surrogate_id INT, 
                channel_platform STRING,
                channel_account_name STRING,
                channel_account_description STRING,
                channel_total_subscribers INT,
                channel_total_assets INT,
                channel_total_views INT,
                modified_TS TIMESTAMP""",
            "marketing.assets": """
                asset_surrogate_id INT,
                asset_natural_id STRING,
                channel_surrogate_id INT,
                asset_title STRING,
                asset_text STRING, 
                asset_publish_date TIMESTAMP,
                modified_TS TIMESTAMP""",
            "marketing.asset_stats": """
                asset_surrogate_id INT, 
                asset_total_impressions INT,
                asset_total_views INT, 
                asset_total_likes INT,
                asset_total_comments INT,
                modified_TS TIMESTAMP""",
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SHAREPOINT_METADATA = {
    'bronze': {
        'name_variable': 'BRONZE_LH_NAME',
        'schemas': ['sharepoint'],
        'tables': {
            'sharepoint.meldingslogg': '''
                sp_id               STRING,
                orgnr               STRING,
                selskapsnavn        STRING,
                hendelsesdato       DATE,
                meldingstype        STRING,
                meldingsinnhold     STRING,
                referanse           STRING,
                url                 STRING,
                created_ts          TIMESTAMP,
                modified_ts         TIMESTAMP,
                _loading_ts         TIMESTAMP''',
        }
    },
    'silver': {
        'name_variable': 'SILVER_LH_NAME',
        'schemas': ['sharepoint'],
        'tables': {
            'sharepoint.meldingslogg': '''
                sp_id               STRING      NOT NULL,
                orgnr               STRING,
                selskapsnavn        STRING,
                hendelsesdato       DATE,
                meldingstype        STRING,
                meldingsinnhold     STRING,
                referanse           STRING,
                url                 STRING,
                created_ts          TIMESTAMP,
                modified_ts         TIMESTAMP,
                _loading_ts         TIMESTAMP,
                _kilde              STRING''',
        }
    },
    'gold': {
        'name_variable': 'GOLD_LH_NAME',
        'schemas': ['siva'],
        'tables': {
            'siva.fact_melding': '''
                melding_surrogate_id    BIGINT,
                sp_id                   STRING      NOT NULL,
                bedrift_surrogate_id    BIGINT,
                orgnr                   STRING,
                selskapsnavn            STRING,
                hendelsesdato           DATE,
                meldingstype            STRING,
                meldingsinnhold         STRING,
                referanse               STRING,
                url                     STRING,
                created_ts              TIMESTAMP,
                modified_ts             TIMESTAMP,
                _loading_ts             TIMESTAMP,
                dato_surrogate_id       INT''',
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SHAREPOINT_EXCEL_METADATA = {
    'bronze': {
        'name_variable': 'BRONZE_LH_NAME',
        'schemas': ['sharepoint'],
        'tables': {
            'sharepoint.regnskapbedrifter': '''
                aarstall                            INT,
                orgnr                               STRING,
                sum_salgsinntekter                  BIGINT,
                sum_driftsinntekter                 BIGINT,
                andre_driftsinntekter               BIGINT,
                sum_finansinntekter                 BIGINT,
                finanskostnader                     BIGINT,
                vareforbruk                         BIGINT,
                loennskostnader                     BIGINT,
                herav_kun_loenn                     BIGINT,
                avskr_varige_driftsmidler           BIGINT,
                andre_driftskostnader               BIGINT,
                sum_driftskostnader                 BIGINT,
                driftsresultat                      BIGINT,
                ordinaert_resultat_foer_skatt       BIGINT,
                ekstraordinaere_poster              BIGINT,
                aarsresultat                        BIGINT,
                sum_varelager                       BIGINT,
                sum_eiendeler                       BIGINT,
                sum_omloepsmidler                   BIGINT,
                kundefordringer                     BIGINT,
                kasse_bank_post                     BIGINT,
                andre_finansielle_instr             BIGINT,
                sum_investeringer                   BIGINT,
                goodwill                            BIGINT,
                utsatt_skattefordel                 BIGINT,
                forskning_og_utvikling              BIGINT,
                sum_egenkapital                     BIGINT,
                aksje_selskapskapital               BIGINT,
                utbytte                             BIGINT,
                avsatt_utbytte                      BIGINT,
                ekstraordinaert_utbytte             BIGINT,
                overkursfond                        BIGINT,
                sum_innskutt_egenkapital            BIGINT,
                sum_opptjent_kapital                BIGINT,
                sum_gjeld                           BIGINT,
                sum_kortsiktig_gjeld                BIGINT,
                leverandoergjeld                    BIGINT,
                lederloenn                          BIGINT,
                pensjonskostnader                   BIGINT,
                husleiekostnader                    BIGINT,
                beholdningsendringer                BIGINT,
                _loading_ts                         TIMESTAMP''',
        }
    },
    'silver': {
        'name_variable': 'SILVER_LH_NAME',
        'schemas': ['sharepoint'],
        'tables': {
            'sharepoint.regnskapbedrifter': '''
                orgnr                               STRING      NOT NULL,
                aarstall                            INT         NOT NULL,
                sum_salgsinntekter                  BIGINT,
                sum_driftsinntekter                 BIGINT,
                driftsresultat                      BIGINT,
                ordinaert_resultat_foer_skatt       BIGINT,
                aarsresultat                        BIGINT,
                sum_eiendeler                       BIGINT,
                sum_egenkapital                     BIGINT,
                sum_gjeld                           BIGINT,
                _loading_ts                         TIMESTAMP,
                _kilde                              STRING''',
        }
    },
    'gold': {
        'name_variable': 'GOLD_LH_NAME',
        'schemas': ['siva'],
        'tables': {
            'siva.fact_regnskap': '''
                regnskap_surrogate_id               BIGINT,
                bedrift_surrogate_id                BIGINT,
                orgnr                               STRING      NOT NULL,
                aarstall                            INT         NOT NULL,
                sum_salgsinntekter                  BIGINT,
                sum_driftsinntekter                 BIGINT,
                driftsresultat                      BIGINT,
                ordinaert_resultat_foer_skatt       BIGINT,
                aarsresultat                        BIGINT,
                sum_eiendeler                       BIGINT,
                sum_egenkapital                     BIGINT,
                sum_gjeld                           BIGINT,
                kasse_bank_post                     BIGINT,
                kundefordringer                     BIGINT,
                lederloenn                          BIGINT,
                aksje_selskapskapital               BIGINT,
                _loading_ts                         TIMESTAMP,
                regnskapsaar_dato_id                INT''',
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRREG_LAKEHOUSE_METADATA = {
    'bronze': {
        'name_variable': 'BRONZE_LH_NAME',
        'schemas': ['brreg'],
        'tables': {
            'brreg.enheter': '''
                organisasjonsnummer                         STRING,
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                institusjonellSektorkode_kode               STRING,
                institusjonellSektorkode_beskrivelse        STRING,
                registreringsdatoEnhetsregisteret           DATE,
                stiftelsesdato                              DATE,
                konkursdato                                 DATE,
                underAvviklingDato                          DATE,
                sisteInnsendteAarsregnskap                  INT,
                antallAnsatte                               INT,
                harRegistrertAntallAnsatte                  BOOLEAN,
                registrertIMvaregisteret                    BOOLEAN,
                registrertIForetaksregisteret               BOOLEAN,
                registrertIFrivillighetsregisteret          BOOLEAN,
                registrertIPartiregisteret                  BOOLEAN,
                konkurs                                     BOOLEAN,
                underAvvikling                              BOOLEAN,
                underTvangsavviklingEllerTvangsopplosning   BOOLEAN,
                erIKonsern                                  BOOLEAN,
                overordnetEnhet                             STRING,
                hjemmeside                                  STRING,
                epostadresse                                STRING,
                telefon                                     STRING,
                mobil                                       STRING,
                maalform                                    STRING,
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                naeringskode3_kode                          STRING,
                naeringskode3_beskrivelse                   STRING,
                forretningsadresse_kommune                  STRING,
                forretningsadresse_kommunenummer            STRING,
                forretningsadresse_postnummer               STRING,
                forretningsadresse_poststed                 STRING,
                forretningsadresse_landkode                 STRING,
                kapital_belop                               DOUBLE,
                kapital_valuta                              STRING,
                kapital_antallAksjer                        BIGINT,
                lastet_tidspunkt                            TIMESTAMP,
                _loading_ts                                 TIMESTAMP''',
        }
    },
    'silver': {
        'name_variable': 'SILVER_LH_NAME',
        'schemas': ['brreg'],
        'tables': {
            'brreg.enheter': '''
                organisasjonsnummer                         STRING      NOT NULL,
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                naeringskode3_kode                          STRING,
                naeringskode3_beskrivelse                   STRING,
                antallAnsatte                               INT,
                registreringsdato                           DATE,
                stiftelsesdato                              DATE,
                konkurs                                     BOOLEAN,
                underAvvikling                              BOOLEAN,
                erIKonsern                                  BOOLEAN,
                forretningsadresse_kommunenummer            STRING,
                forretningsadresse_kommune                  STRING,
                forretningsadresse_postnummer               STRING,
                forretningsadresse_poststed                 STRING,
                forretningsadresse_landkode                 STRING,
                kapital_belop                               DOUBLE,
                lastet_tidspunkt                            TIMESTAMP,
                _loading_ts                                 TIMESTAMP,
                _kilde                                      STRING''',
        }
    },
    'gold': {
        'name_variable': 'GOLD_LH_NAME',
        'schemas': ['siva'],
        'tables': {
            'siva.dim_bedrift': '''
                bedrift_surrogate_id                        BIGINT,
                organisasjonsnummer                         STRING      NOT NULL,
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                naeringskode3_kode                          STRING,
                naeringskode3_beskrivelse                   STRING,
                antallAnsatte                               INT,
                registreringsdato                           DATE,
                stiftelsesdato                              DATE,
                konkurs                                     BOOLEAN,
                underAvvikling                              BOOLEAN,
                erIKonsern                                  BOOLEAN,
                forretningsadresse_kommunenummer            STRING,
                forretningsadresse_kommune                  STRING,
                forretningsadresse_postnummer               STRING,
                forretningsadresse_poststed                 STRING,
                forretningsadresse_landkode                 STRING,
                kapital_belop                               DOUBLE,
                er_aktiv                                    BOOLEAN,
                _loading_ts                                 TIMESTAMP''',
            'siva.dim_dato': '''
                dato_surrogate_id   INT,
                dato                DATE        NOT NULL,
                aar                 INT,
                kvartal             INT,
                maaned              INT,
                maaned_navn         STRING,
                uke                 INT,
                dag                 INT,
                ukedag_nr           INT,
                ukedag_navn         STRING,
                er_helg             BOOLEAN,
                aar_kvartal         STRING,
                aar_maaned          STRING''',
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REFRESH_METADATA = {
    'gold': {
        'name_variable': 'GOLD_LH_NAME',
        'schemas': ['siva'],
        'tables': {
            'siva.refresh_metadata': '''
                dataset_name    STRING,
                refresh_type    STRING,
                refresh_mode    STRING,
                status          STRING,
                started_at      STRING,
                completed_at    STRING,
                request_id      STRING,
                pipeline_name   STRING,
                workspace_name  STRING''',
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ADMIN_METADATA = {
    'admin': {
        'name_variable': 'ADMIN_LH_NAME',
        'schemas': ['metadata'],
        'tables': {
            'metadata.watermark_store': '''
                source_id       INT         NOT NULL,
                endpoint_path   STRING      NOT NULL,
                watermark_date  STRING,
                watermark_id    INT,
                updated_at      TIMESTAMP''',

            'metadata.maintenance_settings': '''
                setting_id              STRING  NOT NULL,
                catalog_name            STRING  NOT NULL,
                lakehouse_name          STRING  NOT NULL,
                schema_name             STRING  NOT NULL,
                table_name              STRING  NOT NULL,
                optimize_enabled        BOOLEAN NOT NULL,
                vacuum_enabled          BOOLEAN NOT NULL,
                vacuum_retention_hours  INT     NOT NULL,
                optimize_interval_hours INT     NOT NULL,
                vacuum_interval_hours   INT     NOT NULL,
                last_optimize_at        TIMESTAMP,
                last_vacuum_at          TIMESTAMP,
                is_active               BOOLEAN NOT NULL'''
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
token        = notebookutils.credentials.getToken("storage")
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]

resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses",
    headers={"Authorization": f"Bearer {token}"}
)
print(resp.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 – Create Lakehouse Objects

# CELL ********************

def merge_lakehouse_metadata(*metadata_dicts):
    result = {}
    for metadata in metadata_dicts:
        for layer, config in metadata.items():
            if layer not in result:
                result[layer] = {"name_variable": config["name_variable"], "schemas": [], "tables": {}}
            for schema in config.get("schemas", []):
                if schema not in result[layer]["schemas"]:
                    result[layer]["schemas"].append(schema)
            result[layer]["tables"].update(config.get("tables", {}))
    return result


def create_lakehouse_objects_old(layer_name, lakehouse_config):
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    print(f"Creating objects for {layer_name} layer ({lh_name})...")
    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
        print(f"  Schema: {schema_name}")
    for table, ddl in lakehouse_config["tables"].items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.{table} ({ddl});")
        print(f"  Table : {table}")

def create_lakehouse_objects(layer_name, lakehouse_config):
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    print(f"Creating objects for {layer_name} layer ({lh_name})...")

    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
        print(f"  Schema: {schema_name}")

    for table, ddl in lakehouse_config["tables"].items():
        full_table = f"`{lh_workspace_name}`.`{lh_name}`.{table}"

        # Bruk DESCRIBE for å sjekke eksisterende kolonner
        # Feiler med AnalysisException hvis tabellen ikke finnes
        try:
            eksisterende = {
                row["col_name"].lower()
                for row in spark.sql(f"DESCRIBE TABLE {full_table}").collect()
                if not row["col_name"].startswith("#")
            }
            # Tabell eksisterer – sjekk nye kolonner
            nye_kolonner = [
                col_def.strip()
                for col_def in ddl.split(",")
                if col_def.strip()
                and col_def.strip().split()[0].strip("`").lower() not in eksisterende
            ]
            if nye_kolonner:
                for col_def in nye_kolonner:
                    spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col_def}")
                    print(f"  Table : {table}  [ny kolonne: {col_def.split()[0]}]")
            else:
                print(f"  Table : {table}  [ingen endringer]")

        except Exception:
            # Tabell finnes ikke – opprett den
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} ({ddl});")
            print(f"  Table : {table}  [opprettet]")

LAKEHOUSE_METADATA = merge_lakehouse_metadata(
    YOUTUBE_METADATA,
    SHAREPOINT_METADATA,
    SHAREPOINT_EXCEL_METADATA,
    BRREG_LAKEHOUSE_METADATA,
    REFRESH_METADATA,
    ADMIN_METADATA 
)

for layer_name, config in SHAREPOINT_METADATA.items():
    create_lakehouse_objects(layer_name, config)

print("\n=== Lakehouse creation complete ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 – Activate Delta Change Data Feed

# CELL ********************

# Only tables with incremental load and high row counts need CDF
CDF_TABLES = [
    f"`{CATALOG}`.`{BRONZE}`.brreg.enheter",
    f"`{CATALOG}`.`{BRONZE}`.sharepoint.meldingslogg",
    f"`{CATALOG}`.`{SILVER}`.brreg.enheter",
    f"`{CATALOG}`.`{SILVER}`.sharepoint.meldingslogg",
]

print("=== Activating CDF ===")
for table in CDF_TABLES:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"  CDF activated: {table}")

print("\n=== Verifying CDF ===")
for table in CDF_TABLES:
    props = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    cdf   = next((r["value"] for r in props if r["key"] == "delta.enableChangeDataFeed"), "false")
    print(f"  [{'OK' if cdf == 'true' else 'FAILED'}] {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 – Delta Protocol Upgrade (ancient datetime support)

# CELL ********************

# Required for BRREG data with dates before 1582 (e.g. stiftelsesdato)
PROTOCOL_TABLES = [
    f"`{CATALOG}`.`{BRONZE}`.brreg.enheter",
]

print("=== Upgrading Delta protocol ===")
for table in PROTOCOL_TABLES:
    spark.sql(f"""
        ALTER TABLE {table}
        SET TBLPROPERTIES (
            'delta.minReaderVersion'   = '2',
            'delta.minWriterVersion'   = '5',
            'delta.columnMapping.mode' = 'name'
        )
    """)
    print(f"  Protocol upgraded: {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 – Parquet Datetime Config

# CELL ********************

# Handle dates before 1582-10-15 in BRREG data
print("=== Setting Parquet datetime config ===")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead",  "CORRECTED")
print(f"  Write : {spark.conf.get('spark.sql.parquet.datetimeRebaseModeInWrite')}")
print(f"  Read  : {spark.conf.get('spark.sql.parquet.datetimeRebaseModeInRead')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 – Status Summary

# CELL ********************

print("=== Setup complete ===")

alle_tabeller = list(set(CDF_TABLES + PROTOCOL_TABLES))
for table in sorted(alle_tabeller):
    try:
        props       = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        cdf         = props.get("delta.enableChangeDataFeed", "false")
        col_mapping = props.get("delta.columnMapping.mode",   "none")
        min_reader  = props.get("delta.minReaderVersion",     "1")
        min_writer  = props.get("delta.minWriterVersion",     "2")
        print(f"\n  {table}")
        print(f"    CDF            : {cdf}")
        print(f"    Column mapping : {col_mapping}")
        print(f"    Protocol       : reader={min_reader} writer={min_writer}")
    except Exception as e:
        print(f"  WARNING: {table}: {e}")

print("\n=== Done ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Watermarks – éin rad per kjelde
# source_id må matche instructions.ingestion
watermark_data = [
    (2, "/oppdateringer/enheter",  None, None, None),
    (3, "data-meldingslogg",       None, None, None),
    (4, "Regnskapbedrifter.xlsx",  None, None, None),
]

# Bytt TIMESTAMP → STRING og BIGINT → INT
schema = "source_id INT, endpoint_path STRING, watermark_date STRING, watermark_id INT, updated_at TIMESTAMP"

spark.createDataFrame(watermark_data, schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"`{CATALOG}`.`{ADMIN}`.metadata.watermark_store")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 – Fill default metadata

# CELL ********************

maintenance_data = [
    ("bronze_youtube_videos",               "av01-dev-datastores", "lh_av01_bronze", "youtube",    "videos",             True, True, 168, 24, 168, None, None, True),
    ("bronze_youtube_channel",              "av01-dev-datastores", "lh_av01_bronze", "youtube",    "channel",            True, True, 168, 24, 168, None, None, True),
    ("bronze_youtube_playlist_items",       "av01-dev-datastores", "lh_av01_bronze", "youtube",    "playlist_items",     True, True, 168, 24, 168, None, None, True),
    ("bronze_brreg_enheter",                "av01-dev-datastores", "lh_av01_bronze", "brreg",      "enheter",            True, True, 168, 24, 168, None, None, True),
    ("bronze_sharepoint_meldingslogg",      "av01-dev-datastores", "lh_av01_bronze", "sharepoint", "meldingslogg",       True, True, 168, 24, 168, None, None, True),
    ("bronze_sharepoint_regnskapbedrifter", "av01-dev-datastores", "lh_av01_bronze", "sharepoint", "regnskapbedrifter",  True, True, 168, 24, 168, None, None, True),
    ("silver_brreg_enheter",                "av01-dev-datastores", "lh_av01_silver", "brreg",      "enheter",            True, True, 168, 24, 168, None, None, True),
    ("silver_sharepoint_meldingslogg",      "av01-dev-datastores", "lh_av01_silver", "sharepoint", "meldingslogg",       True, True, 168, 24, 168, None, None, True),
    ("silver_sharepoint_regnskapbedrifter", "av01-dev-datastores", "lh_av01_silver", "sharepoint", "regnskapbedrifter",  True, True, 168, 24, 168, None, None, True),
    ("silver_youtube_channel_stats",        "av01-dev-datastores", "lh_av01_silver", "youtube",    "channel_stats",      True, True, 168, 24, 168, None, None, True),
    ("silver_youtube_videos",               "av01-dev-datastores", "lh_av01_silver", "youtube",    "videos",             True, True, 168, 24, 168, None, None, True),
    ("silver_youtube_video_statistics",     "av01-dev-datastores", "lh_av01_silver", "youtube",    "video_statistics",   True, True, 168, 24, 168, None, None, True),
    ("gold_siva_dim_bedrift",               "av01-dev-datastores", "lh_av01_gold",   "siva",       "dim_bedrift",        True, True, 168, 24, 168, None, None, True),
    ("gold_siva_dim_dato",                  "av01-dev-datastores", "lh_av01_gold",   "siva",       "dim_dato",           True, True, 168, 24, 168, None, None, True),
    ("gold_siva_fact_melding",              "av01-dev-datastores", "lh_av01_gold",   "siva",       "fact_melding",       True, True, 168, 24, 168, None, None, True),
    ("gold_siva_fact_regnskap",             "av01-dev-datastores", "lh_av01_gold",   "siva",       "fact_regnskap",      True, True, 168, 24, 168, None, None, True),
    ("gold_siva_refresh_metadata",          "av01-dev-datastores", "lh_av01_gold",   "siva",       "refresh_metadata",   True, True, 168, 24, 168, None, None, True),
]

schema = """
    setting_id              STRING,
    catalog_name            STRING,
    lakehouse_name          STRING,
    schema_name             STRING,
    table_name              STRING,
    optimize_enabled        BOOLEAN,
    vacuum_enabled          BOOLEAN,
    vacuum_retention_hours  INT,
    optimize_interval_hours INT,
    vacuum_interval_hours   INT,
    last_optimize_at        TIMESTAMP,
    last_vacuum_at          TIMESTAMP,
    is_active               BOOLEAN
"""

spark.createDataFrame(maintenance_data, schema) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"`{CATALOG}`.`{ADMIN}`.metadata.maintenance_settings")

print(f"  -> maintenance_settings populated: {len(maintenance_data)} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
