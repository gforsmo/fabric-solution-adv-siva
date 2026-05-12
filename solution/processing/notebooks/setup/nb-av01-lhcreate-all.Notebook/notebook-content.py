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
#  
#  **Purpose**: Create all lakehouse schemas and tables for Bronze, Silver, and Gold layers.
#  
#  **Usage**: Run via nb-av01-new-workspace-setup or directly for lakehouse initialization.
#  
#  **Naming**: Uses Spark-SQL four-part naming: `workspace`.`lakehouse`.`schema`.`table`

# MARKDOWN ********************

# ## Setup

# CELL ********************

import notebookutils

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
lh_workspace_name = variables.LH_WORKSPACE_NAME

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


print(lh_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lakehouse Schema Definitions
# Table definitions for each layer (Bronze, Silver, Gold).

# CELL ********************

YOUTUBE_METADATA = {
    "bronze": {
        "name_variable": "BRONZE_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube.channel": """
                channel_id STRING, 
                channel_name STRING, 
                channel_description STRING, 
                view_count INT, 
                subscriber_count INT, 
                video_count INT, 
                loading_TS TIMESTAMP""",
            "youtube.playlist_items": """
                channel_id STRING, 
                video_id STRING, 
                video_title STRING, 
                video_description STRING,
                thumbnail_url STRING,
                video_publish_TS TIMESTAMP,
                loading_TS TIMESTAMP""",
            "youtube.videos": """
                video_id STRING, 
                video_view_count INT, 
                video_like_count INT, 
                video_comment_count INT,
                loading_TS TIMESTAMP""",
        }
    },
    "silver": {
        "name_variable": "SILVER_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube.channel_stats": """
                channel_id STRING, 
                channel_name STRING, 
                channel_description STRING, 
                view_count INT, 
                subscriber_count INT, 
                video_count INT, 
                loading_TS TIMESTAMP""",
            "youtube.videos": """
                channel_id STRING, 
                video_id STRING, 
                video_title STRING, 
                video_description STRING,
                thumbnail_url STRING,
                video_publish_TS TIMESTAMP,
                loading_TS TIMESTAMP""",
            "youtube.video_statistics": """
                video_id STRING, 
                video_view_count INT, 
                video_like_count INT, 
                video_comment_count INT,
                loading_TS TIMESTAMP""",
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
    "bronze": {
        "name_variable": "BRONZE_LH_NAME",
        "schemas": ["sharepoint"],
        "tables": {
            # Kolonner og typer matcher sp_meldingslogg kolonne-mapping eksakt
            "sharepoint.meldingslogg": """
                sp_id               STRING,
                orgnr               STRING,
                selskapsnavn        STRING,
                hendelsesdato       DATE,
                meldingstype        STRING,
                meldingsinnhold     STRING,
                url                 STRING,
                created_ts          TIMESTAMP,
                modified_ts         TIMESTAMP,
                _loading_ts         TIMESTAMP""",
        }
    },
    "silver": {
        "name_variable": "SILVER_LH_NAME",
        "schemas": ["sharepoint"],
        "tables": {
            "sharepoint.meldingslogg": """
                sp_id               STRING      NOT NULL,
                orgnr               STRING,
                orgnr_status        STRING,
                selskapsnavn        STRING,
                hendelsesdato       DATE,
                meldingstype        STRING,
                meldingsinnhold     STRING,
                url                 STRING,
                created_ts          TIMESTAMP,
                modified_ts         TIMESTAMP,
                _loading_ts         TIMESTAMP,
                _kilde              STRING""",
        }
    },
    "gold": {
        "name_variable": "GOLD_LH_NAME",
        "schemas": ["siva"],
        "tables": {
            "siva.meldingslogg": """
                meldingslogg_surrogate_id   BIGINT,
                sp_id                       STRING      NOT NULL,
                orgnr                       STRING,
                bedrift_navn                STRING,
                orgnr_status                STRING,
                hendelsesdato               DATE,
                meldingstype                STRING,
                meldingsinnhold             STRING,
                url                         STRING,
                created_ts                  TIMESTAMP,
                modified_ts                 TIMESTAMP,
                _loading_ts                 TIMESTAMP""",
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
    "bronze": {
        "name_variable": "BRONZE_LH_NAME",
        "schemas": ["sharepoint"],
        "tables": {
            "sharepoint.regnskapbedrifter": """
                aarstall                            INT,
                orgnr                               STRING,
                sum_salgsinntekter                  DOUBLE,
                sum_driftsinntekter                 DOUBLE,
                andre_driftsinntekter               DOUBLE,
                sum_finansinntekter                 DOUBLE,
                finanskostnader                     DOUBLE,
                vareforbruk                         DOUBLE,
                loennskostnader                     DOUBLE,
                herav_kun_loenn                     DOUBLE,
                avskr_varige_driftsmidler           DOUBLE,
                andre_driftskostnader               DOUBLE,
                sum_driftskostnader                 DOUBLE,
                driftsresultat                      DOUBLE,
                ordinaert_resultat_foer_skatt       DOUBLE,
                ekstraordinaere_poster              DOUBLE,
                aarsresultat                        DOUBLE,
                sum_varelager                       DOUBLE,
                sum_eiendeler                       DOUBLE,
                sum_omloepsmidler                   DOUBLE,
                kundefordringer                     DOUBLE,
                kasse_bank_post                     DOUBLE,
                andre_finansielle_instr             DOUBLE,
                sum_investeringer                   DOUBLE,
                goodwill                            DOUBLE,
                utsatt_skattefordel                 DOUBLE,
                forskning_og_utvikling              DOUBLE,
                sum_egenkapital                     DOUBLE,
                aksje_selskapskapital               DOUBLE,
                utbytte                             DOUBLE,
                avsatt_utbytte                      DOUBLE,
                ekstraordinaert_utbytte             DOUBLE,
                overkursfond                        DOUBLE,
                sum_innskutt_egenkapital            DOUBLE,
                sum_opptjent_kapital                DOUBLE,
                sum_gjeld                           DOUBLE,
                sum_kortsiktig_gjeld                DOUBLE,
                leverandoergjeld                    DOUBLE,
                lederloenn                          DOUBLE,
                pensjonskostnader                   DOUBLE,
                husleiekostnader                    DOUBLE,
                beholdningsendringer                DOUBLE,
                _loading_ts                         TIMESTAMP""",
        }
    },
    "silver": {
        "name_variable": "SILVER_LH_NAME",
        "schemas": ["sharepoint"],
        "tables": {
            "sharepoint.regnskapbedrifter": """
                orgnr                               STRING      NOT NULL,
                aarstall                            INT         NOT NULL,
                orgnr_status                        STRING,
                har_finansielle_data                BOOLEAN,

                sum_salgsinntekter                  DECIMAL(18,0),
                aksje_selskapskapital               DECIMAL(18,0),
                lederloenn                          DECIMAL(18,0),
                finanskostnader                     DECIMAL(18,0),
                andre_driftsinntekter               DECIMAL(18,0),
                sum_finansinntekter                 DECIMAL(18,0),
                driftsresultat                      DECIMAL(18,0),
                ordinaert_resultat_foer_skatt       DECIMAL(18,0),
                ekstraordinaere_poster              DECIMAL(18,0),
                aarsresultat                        DECIMAL(18,0),
                vareforbruk                         DECIMAL(18,0),
                loennskostnader                     DECIMAL(18,0),
                herav_kun_loenn                     DECIMAL(18,0),
                avskr_varige_driftsmidler           DECIMAL(18,0),
                andre_driftskostnader               DECIMAL(18,0),
                sum_driftskostnader                 DECIMAL(18,0),
                sum_driftsinntekter                 DECIMAL(18,0),
                sum_varelager                       DECIMAL(18,0),
                sum_eiendeler                       DECIMAL(18,0),
                sum_egenkapital                     DECIMAL(18,0),
                utbytte                             DECIMAL(18,0),
                avsatt_utbytte                      DECIMAL(18,0),
                ekstraordinaert_utbytte             DECIMAL(18,0),
                overkursfond                        DECIMAL(18,0),
                forskning_og_utvikling              DECIMAL(18,0),
                sum_gjeld                           DECIMAL(18,0),
                kundefordringer                     DECIMAL(18,0),
                andre_finansielle_instr             DECIMAL(18,0),
                sum_investeringer                   DECIMAL(18,0),
                kasse_bank_post                     DECIMAL(18,0),
                beholdningsendringer                DECIMAL(18,0),
                sum_kortsiktig_gjeld                DECIMAL(18,0),
                sum_omloepsmidler                   DECIMAL(18,0),
                pensjonskostnader                   DECIMAL(18,0),
                husleiekostnader                    DECIMAL(18,0),
                leverandoergjeld                    DECIMAL(18,0),
                sum_innskutt_egenkapital            DECIMAL(18,0),
                sum_opptjent_kapital                DECIMAL(18,0),
                goodwill                            DECIMAL(18,0),
                utsatt_skattefordel                 DECIMAL(18,0),

                _loading_ts                         TIMESTAMP,
                _kilde                              STRING""",
        }
    },
    "gold": {
        "name_variable": "GOLD_LH_NAME",
        "schemas": ["siva"],
        "tables": {
            "siva.fact_finansiell": """
                finansiell_surrogate_id             BIGINT,
                orgnr                               STRING      NOT NULL,
                aarstall                            INT         NOT NULL,
                bedrift_navn                        STRING,
                orgnr_status                        STRING,
                har_finansielle_data                BOOLEAN,

                sum_salgsinntekter                  DECIMAL(18,0),
                sum_driftsinntekter                 DECIMAL(18,0),
                driftsresultat                      DECIMAL(18,0),
                aarsresultat                        DECIMAL(18,0),
                sum_egenkapital                     DECIMAL(18,0),
                sum_gjeld                           DECIMAL(18,0),
                sum_eiendeler                       DECIMAL(18,0),
                loennskostnader                     DECIMAL(18,0),
                sum_driftskostnader                 DECIMAL(18,0),
                kasse_bank_post                     DECIMAL(18,0),

                _sist_hentet                        TIMESTAMP""",
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
    "bronze": {
        "name_variable": "BRONZE_LH_NAME",
        "schemas": ["brreg"],
        "tables": {
            "brreg.enheter": """
                -- Nøkkel
                organisasjonsnummer                         STRING,

                -- Navn og form
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                institusjonellSektorkode_kode               STRING,
                institusjonellSektorkode_beskrivelse        STRING,

                -- Datoer
                registreringsdatoEnhetsregisteret           DATE,
                stiftelsesdato                              DATE,
                konkursdato                                 DATE,
                underAvviklingDato                          DATE,
                sisteInnsendteAarsregnskap                  INT,

                -- Ansatte
                antallAnsatte                               INT,
                harRegistrertAntallAnsatte                  BOOLEAN,

                -- Registreringsstatus
                registrertIMvaregisteret                    BOOLEAN,
                registrertIForetaksregisteret               BOOLEAN,
                registrertIFrivillighetsregisteret          BOOLEAN,
                registrertIPartiregisteret                  BOOLEAN,
                konkurs                                     BOOLEAN,
                underAvvikling                              BOOLEAN,
                underTvangsavviklingEllerTvangsopplosning   BOOLEAN,
                erIKonsern                                  BOOLEAN,
                overordnetEnhet                             STRING,

                -- Kontakt
                hjemmeside                                  STRING,
                epostadresse                                STRING,
                telefon                                     STRING,
                mobil                                       STRING,
                maalform                                    STRING,

                -- Næringskoder (string – inneholder punktum, f.eks. "43.210")
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                naeringskode3_kode                          STRING,
                naeringskode3_beskrivelse                   STRING,

                -- Adresse (string – postnummer/kommunenummer kan ha ledende nuller)
                forretningsadresse_kommune                  STRING,
                forretningsadresse_kommunenummer            STRING,
                forretningsadresse_postnummer               STRING,
                forretningsadresse_poststed                 STRING,
                forretningsadresse_landkode                 STRING,

                -- Kapital
                kapital_belop                               DOUBLE,
                kapital_valuta                              STRING,
                kapital_antallAksjer                        BIGINT,

                -- Lovgivning
                underlagtLovgivningLandKode                 STRING,
                underlagtLovgivningLand                     STRING,

                -- Metadata
                lastet_tidspunkt                            TIMESTAMP,
                _loading_ts                                 TIMESTAMP""",
        }
    },
    "silver": {
        "name_variable": "SILVER_LH_NAME",
        "schemas": ["brreg"],
        "tables": {
            "brreg.enheter": """
                -- Nøkkel
                organisasjonsnummer                         STRING      NOT NULL,

                -- Navn og form
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                institusjonellSektorkode_kode               STRING,
                institusjonellSektorkode_beskrivelse        STRING,

                -- Datoer
                registreringsdato                           DATE,
                stiftelsesdato                              DATE,
                konkursdato                                 DATE,
                underAvviklingDato                          DATE,
                sisteInnsendteAarsregnskap                  INT,

                -- Ansatte og status
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

                -- Kontakt
                hjemmeside                                  STRING,
                epostadresse                                STRING,
                telefon                                     STRING,
                mobil                                       STRING,
                maalform                                    STRING,

                -- Næringskoder
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                naeringskode3_kode                          STRING,
                naeringskode3_beskrivelse                   STRING,

                -- Adresse
                forretningsadresse_kommune                  STRING,
                forretningsadresse_kommunenummer            STRING,
                forretningsadresse_postnummer               STRING,
                forretningsadresse_poststed                 STRING,
                forretningsadresse_landkode                 STRING,

                -- Kapital
                kapital_belop                               DECIMAL(18,2),
                kapital_valuta                              STRING,
                kapital_antallAksjer                        BIGINT,

                -- Lovgivning
                underlagtLovgivningLandKode                 STRING,
                underlagtLovgivningLand                     STRING,

                -- Metadata
                lastet_tidspunkt                            TIMESTAMP,
                _loading_ts                                 TIMESTAMP,
                _kilde                                      STRING""",
        }
    },
    "gold": {
        "name_variable": "GOLD_LH_NAME",
        "schemas": ["siva"],
        "tables": {
            "siva.dim_bedrift": """
                bedrift_surrogate_id                        BIGINT,
                organisasjonsnummer                         STRING      NOT NULL,
                navn                                        STRING,
                organisasjonsform_kode                      STRING,
                organisasjonsform_beskrivelse               STRING,
                naeringskode1_kode                          STRING,
                naeringskode1_beskrivelse                   STRING,
                naeringskode2_kode                          STRING,
                naeringskode2_beskrivelse                   STRING,
                antallAnsatte                               INT,
                registreringsdato                           DATE,
                stiftelsesdato                              DATE,
                konkurs                                     BOOLEAN,
                underAvvikling                              BOOLEAN,
                kommunenummer                               STRING,
                kommune                                     STRING,
                postnummer                                  STRING,
                poststed                                    STRING,
                landkode                                    STRING,
                er_aktiv                                    BOOLEAN,
                _sist_hentet                                TIMESTAMP""",
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Objects

# CELL ********************

def merge_lakehouse_metadata(*metadata_dicts: dict) -> dict:
    """
    Merger flere LAKEHOUSE_METADATA-dicts.
    Schemas og tables slås sammen per lag (bronze/silver/gold).
    Duplikate tabellnavn vil overskrive hverandre – siste vinner.
    """
    result = {}

    for metadata in metadata_dicts:
        for layer, config in metadata.items():
            if layer not in result:
                result[layer] = {
                    "name_variable": config["name_variable"],
                    "schemas": [],
                    "tables": {}
                }

            # Merge schemas (unike verdier)
            for schema in config.get("schemas", []):
                if schema not in result[layer]["schemas"]:
                    result[layer]["schemas"].append(schema)

            # Merge tables
            result[layer]["tables"].update(config.get("tables", {}))

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_lakehouse_objects(layer_name: str, lakehouse_config: dict):
    """
    Create schemas and tables for a lakehouse based on configuration.

    Args:
        layer_name: Layer identifier (bronze, silver, gold) for logging
        lakehouse_config: Dict with name_variable, schemas, and tables keys
    """
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    print(f"Creating objects for {layer_name} layer ({lh_name})...")

    # Create schemas
    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
        print(f"  Schema: {schema_name}")

    # Create tables
    for table, ddl in lakehouse_config["tables"].items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.{table} ({ddl});")
        print(f"  Table: {table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Merge til én samlet struktur ──────────────────────────────────────────────

LAKEHOUSE_METADATA = merge_lakehouse_metadata(
    YOUTUBE_METADATA,
    SHAREPOINT_METADATA,
    SHAREPOINT_EXCEL_METADATA,
    BRREG_LAKEHOUSE_METADATA
    # legg til nye kilder her
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process all lakehouses
for layer_name, lakehouse_config in LAKEHOUSE_METADATA.items():
    create_lakehouse_objects(layer_name, lakehouse_config)

print("Lakehouse creation complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
