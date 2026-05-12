# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-lhcreate-all
# **Purpose**: Opprett alle lakehouse-skjemaer.
# **Kjøres ved:** Nytt oppsett eller skjema-endringer.
# 
# **Seksjoner:**
# 1. Lakehouse-tabeller (Bronze/Silver/Gold)


# MARKDOWN ********************

# ## Setup

# CELL ********************

import notebookutils

variables         = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
lh_workspace_name = variables.LH_WORKSPACE_NAME

print(f"Workspace: {lh_workspace_name}")
print(f"Bronze:    {variables.BRONZE_LH_NAME}")
print(f"Silver:    {variables.SILVER_LH_NAME}")
print(f"Gold:      {variables.GOLD_LH_NAME}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Lakehouse Schema Definitions

# CELL ********************

# YouTube – kun Bronze og Silver (demo, fjernes etterhvert)
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
                andre_driftsinntekter               BIGINT,
                sum_finansinntekter                 BIGINT,
                finanskostnader                     BIGINT,
                vareforbruk                         BIGINT,
                loennskostnader                     BIGINT,
                sum_driftskostnader                 BIGINT,
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
                underlagtLovgivningLandKode                 STRING,
                underlagtLovgivningLand                     STRING,
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
                institusjonellSektorkode_kode               STRING,
                institusjonellSektorkode_beskrivelse        STRING,
                registreringsdato                           DATE,
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
                underlagtLovgivningLandKode                 STRING,
                underlagtLovgivningLand                     STRING,
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
                institusjonellSektorkode_kode               STRING,
                institusjonellSektorkode_beskrivelse        STRING,
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

# MARKDOWN ********************

# ## 2. Opprett Lakehouse-objekter

# CELL ********************

def merge_lakehouse_metadata(*metadata_dicts: dict) -> dict:
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_lakehouse_objects(layer_name: str, lakehouse_config: dict):
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    print(f"Creating objects for {layer_name} layer ({lh_name})...")
    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
        print(f"  Schema: {schema_name}")
    for table, ddl in lakehouse_config["tables"].items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.{table} ({ddl});")
        print(f"  Table: {table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LAKEHOUSE_METADATA = merge_lakehouse_metadata(
    YOUTUBE_METADATA,
    SHAREPOINT_METADATA,
    SHAREPOINT_EXCEL_METADATA,
    BRREG_LAKEHOUSE_METADATA
)

for layer_name, lakehouse_config in LAKEHOUSE_METADATA.items():
    create_lakehouse_objects(layer_name, lakehouse_config)

print("\n=== Lakehouse creation complete ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
