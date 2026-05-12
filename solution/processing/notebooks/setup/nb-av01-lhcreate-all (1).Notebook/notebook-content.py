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
    # Ingen gold – YouTube er kun demo
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
            # fact_melding: én rad per melding/hendelse per bedrift
            'siva.fact_melding': '''
                melding_surrogate_id        BIGINT,
                sp_id                       STRING          NOT NULL,
                bedrift_surrogate_id        BIGINT,
                orgnr                       STRING,
                selskapsnavn                STRING,
                hendelsesdato               DATE,
                meldingstype                STRING,
                meldingsinnhold             STRING,
                url                         STRING,
                created_ts                  TIMESTAMP,
                modified_ts                 TIMESTAMP,
                _loading_ts                 TIMESTAMP''',
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
                sum_salgsinntekter                  DECIMAL(18,0),
                sum_driftsinntekter                 DECIMAL(18,0),
                andre_driftsinntekter               DOUBLE,
                sum_finansinntekter                 DECIMAL(18,0),
                finanskostnader                     DECIMAL(18,0),
                vareforbruk                         DECIMAL(18,0),
                loennskostnader                     DECIMAL(18,0),
                herav_kun_loenn                     DECIMAL(18,0),
                avskr_varige_driftsmidler           DECIMAL(18,0),
                andre_driftskostnader               DECIMAL(18,0),
                sum_driftskostnader                 DECIMAL(18,0),
                driftsresultat                      DECIMAL(18,0),
                ordinaert_resultat_foer_skatt       DECIMAL(18,0),
                ekstraordinaere_poster              DECIMAL(18,0),
                aarsresultat                        DECIMAL(18,0),
                sum_varelager                       DECIMAL(18,0),
                sum_eiendeler                       DECIMAL(18,0),
                sum_omloepsmidler                   DECIMAL(18,0),
                kundefordringer                     DECIMAL(18,0),
                kasse_bank_post                     DECIMAL(18,0),
                andre_finansielle_instr             DECIMAL(18,0),
                sum_investeringer                   DECIMAL(18,0),
                goodwill                            DECIMAL(18,0),
                utsatt_skattefordel                 DECIMAL(18,0),
                forskning_og_utvikling              DECIMAL(18,0),
                sum_egenkapital                     DECIMAL(18,0),
                aksje_selskapskapital               DECIMAL(18,0),
                utbytte                             DECIMAL(18,0),
                avsatt_utbytte                      DECIMAL(18,0),
                ekstraordinaert_utbytte             DECIMAL(18,0),
                overkursfond                        DECIMAL(18,0),
                sum_innskutt_egenkapital            DECIMAL(18,0),
                sum_opptjent_kapital                DECIMAL(18,0),
                sum_gjeld                           DECIMAL(18,0),
                sum_kortsiktig_gjeld                DECIMAL(18,0),
                leverandoergjeld                    DECIMAL(18,0),
                lederloenn                          DECIMAL(18,0),
                pensjonskostnader                   DECIMAL(18,0),
                husleiekostnader                    DECIMAL(18,0),
                beholdningsendringer                DECIMAL(18,0),
                _loading_ts                         TIMESTAMP,
                _kilde                              STRING''',
        }
    },
    'gold': {
        'name_variable': 'GOLD_LH_NAME',
        'schemas': ['siva'],
        'tables': {
            # fact_regnskap: én rad per bedrift per regnskapsår
            'siva.fact_regnskap': '''
                regnskap_surrogate_id               BIGINT,
                bedrift_surrogate_id                BIGINT,
                orgnr                               STRING      NOT NULL,
                aarstall                            INT         NOT NULL,

                -- Inntekter
                sum_salgsinntekter                  DECIMAL(18,0),
                sum_driftsinntekter                 DECIMAL(18,0),
                andre_driftsinntekter               DECIMAL(18,0),
                sum_finansinntekter                 DECIMAL(18,0),

                -- Kostnader
                finanskostnader                     DECIMAL(18,0),
                vareforbruk                         DECIMAL(18,0),
                loennskostnader                     DECIMAL(18,0),
                sum_driftskostnader                 DECIMAL(18,0),

                -- Resultat
                driftsresultat                      DECIMAL(18,0),
                ordinaert_resultat_foer_skatt       DECIMAL(18,0),
                aarsresultat                        DECIMAL(18,0),

                -- Balanse
                sum_eiendeler                       DECIMAL(18,0),
                sum_egenkapital                     DECIMAL(18,0),
                sum_gjeld                           DECIMAL(18,0),
                kasse_bank_post                     DECIMAL(18,0),
                kundefordringer                     DECIMAL(18,0),

                -- Lønn og kapital
                lederloenn                          DECIMAL(18,0),
                aksje_selskapskapital               DECIMAL(18,0),

                _loading_ts                         TIMESTAMP''',
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
                kapital_belop                               DECIMAL(18,2),
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
            # dim_bedrift: sakte-endrende dimensjon for norske bedrifter
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
                kapital_belop                               DECIMAL(18,2),
                er_aktiv                                    BOOLEAN,
                _sist_hentet                                TIMESTAMP''',
        }
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# SQL for å oppdatere metadata.column_mappings for Gold-tabeller
# Kjøres mot fs-av01-admin SQL Database
# =============================================================================

GOLD_COLUMN_MAPPINGS_SQL = """
-- ── siva.dim_bedrift ──────────────────────────────────────────────────────────
-- Ryd opp gamle hvis de finnes
DELETE FROM metadata.column_mappings WHERE mapping_id = 'siva_dim_bedrift';

INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('siva_dim_bedrift',  1, 'organisasjonsnummer',                      'organisasjonsnummer',                      'string',    'Organisasjonsnummer (nøkkel)'),
('siva_dim_bedrift',  2, 'navn',                                      'navn',                                      'string',    'Foretaksnavn'),
('siva_dim_bedrift',  3, 'organisasjonsform_kode',                    'organisasjonsform_kode',                    'string',    'Organisasjonsform kode'),
('siva_dim_bedrift',  4, 'organisasjonsform_beskrivelse',             'organisasjonsform_beskrivelse',             'string',    'Organisasjonsform beskrivelse'),
('siva_dim_bedrift',  5, 'institusjonellSektorkode_kode',             'institusjonellSektorkode_kode',             'string',    'Institusjonell sektorkode'),
('siva_dim_bedrift',  6, 'institusjonellSektorkode_beskrivelse',      'institusjonellSektorkode_beskrivelse',      'string',    'Institusjonell sektorkode beskrivelse'),
('siva_dim_bedrift',  7, 'naeringskode1_kode',                        'naeringskode1_kode',                        'string',    'Primær næringskode'),
('siva_dim_bedrift',  8, 'naeringskode1_beskrivelse',                 'naeringskode1_beskrivelse',                 'string',    'Primær næringskode beskrivelse'),
('siva_dim_bedrift',  9, 'naeringskode2_kode',                        'naeringskode2_kode',                        'string',    'Sekundær næringskode'),
('siva_dim_bedrift', 10, 'naeringskode2_beskrivelse',                 'naeringskode2_beskrivelse',                 'string',    'Sekundær næringskode beskrivelse'),
('siva_dim_bedrift', 11, 'naeringskode3_kode',                        'naeringskode3_kode',                        'string',    'Tertiær næringskode'),
('siva_dim_bedrift', 12, 'naeringskode3_beskrivelse',                 'naeringskode3_beskrivelse',                 'string',    'Tertiær næringskode beskrivelse'),
('siva_dim_bedrift', 13, 'antallAnsatte',                             'antallAnsatte',                             'int',       'Antall ansatte'),
('siva_dim_bedrift', 14, 'registreringsdato',                         'registreringsdato',                         'date',      'Registreringsdato'),
('siva_dim_bedrift', 15, 'stiftelsesdato',                            'stiftelsesdato',                            'date',      'Stiftelsesdato'),
('siva_dim_bedrift', 16, 'konkurs',                                   'konkurs',                                   'boolean',   'Er under konkurs'),
('siva_dim_bedrift', 17, 'underAvvikling',                            'underAvvikling',                            'boolean',   'Er under avvikling'),
('siva_dim_bedrift', 18, 'erIKonsern',                                'erIKonsern',                                'boolean',   'Er del av konsern'),
('siva_dim_bedrift', 19, 'forretningsadresse_kommunenummer',          'forretningsadresse_kommunenummer',          'string',    'Kommunenummer'),
('siva_dim_bedrift', 20, 'forretningsadresse_kommune',                'forretningsadresse_kommune',                'string',    'Kommune'),
('siva_dim_bedrift', 21, 'forretningsadresse_postnummer',             'forretningsadresse_postnummer',             'string',    'Postnummer'),
('siva_dim_bedrift', 22, 'forretningsadresse_poststed',               'forretningsadresse_poststed',               'string',    'Poststed'),
('siva_dim_bedrift', 23, 'forretningsadresse_landkode',               'forretningsadresse_landkode',               'string',    'Landkode'),
('siva_dim_bedrift', 24, 'kapital_belop',                             'kapital_belop',                             'double',    'Aksjekapital beløp'),
('siva_dim_bedrift', 25, '_loading_ts',                               '_loading_ts',                               'current_timestamp', 'Lastet tidspunkt');

-- ── siva.fact_melding ─────────────────────────────────────────────────────────
DELETE FROM metadata.column_mappings WHERE mapping_id = 'siva_fact_melding';

INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('siva_fact_melding',  1, 'sp_id',          'sp_id',          'string',           'SharePoint item ID (nøkkel)'),
('siva_fact_melding',  2, 'orgnr',           'orgnr',          'string',           'Organisasjonsnummer'),
('siva_fact_melding',  3, 'selskapsnavn',    'selskapsnavn',   'string',           'Selskapsnavn'),
('siva_fact_melding',  4, 'hendelsesdato',   'hendelsesdato',  'date',             'Dato for hendelsen'),
('siva_fact_melding',  5, 'meldingstype',    'meldingstype',   'string',           'Type melding'),
('siva_fact_melding',  6, 'meldingsinnhold', 'meldingsinnhold','string',           'Innhold i meldingen'),
('siva_fact_melding',  7, 'url',             'url',            'string',           'URL/lenke'),
('siva_fact_melding',  8, 'created_ts',      'created_ts',     'timestamp',        'Opprettet tidspunkt'),
('siva_fact_melding',  9, 'modified_ts',     'modified_ts',    'timestamp',        'Sist endret tidspunkt'),
('siva_fact_melding', 10, '_loading_ts',     '_loading_ts',    'current_timestamp','Lastet tidspunkt');

-- ── siva.fact_regnskap ────────────────────────────────────────────────────────
DELETE FROM metadata.column_mappings WHERE mapping_id = 'siva_fact_regnskap';

INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('siva_fact_regnskap',  1, 'orgnr',                          'orgnr',                          'string',           'Organisasjonsnummer (nøkkel)'),
('siva_fact_regnskap',  2, 'aarstall',                        'aarstall',                        'int',              'Regnskapsår (nøkkel)'),
('siva_fact_regnskap',  3, 'sum_salgsinntekter',              'sum_salgsinntekter',              'double',           'Sum salgsinntekter'),
('siva_fact_regnskap',  4, 'sum_driftsinntekter',             'sum_driftsinntekter',             'double',           'Sum driftsinntekter'),
('siva_fact_regnskap',  5, 'andre_driftsinntekter',           'andre_driftsinntekter',           'double',           'Andre driftsinntekter'),
('siva_fact_regnskap',  6, 'sum_finansinntekter',             'sum_finansinntekter',             'double',           'Sum finansinntekter'),
('siva_fact_regnskap',  7, 'finanskostnader',                 'finanskostnader',                 'double',           'Finanskostnader'),
('siva_fact_regnskap',  8, 'vareforbruk',                     'vareforbruk',                     'double',           'Vareforbruk'),
('siva_fact_regnskap',  9, 'loennskostnader',                 'loennskostnader',                 'double',           'Lønnskostnader'),
('siva_fact_regnskap', 10, 'sum_driftskostnader',             'sum_driftskostnader',             'double',           'Sum driftskostnader'),
('siva_fact_regnskap', 11, 'driftsresultat',                  'driftsresultat',                  'double',           'Driftsresultat'),
('siva_fact_regnskap', 12, 'ordinaert_resultat_foer_skatt',   'ordinaert_resultat_foer_skatt',   'double',           'Ordinært resultat før skatt'),
('siva_fact_regnskap', 13, 'aarsresultat',                    'aarsresultat',                    'double',           'Årsresultat'),
('siva_fact_regnskap', 14, 'sum_eiendeler',                   'sum_eiendeler',                   'double',           'Sum eiendeler'),
('siva_fact_regnskap', 15, 'sum_egenkapital',                 'sum_egenkapital',                 'double',           'Sum egenkapital'),
('siva_fact_regnskap', 16, 'sum_gjeld',                       'sum_gjeld',                       'double',           'Sum gjeld'),
('siva_fact_regnskap', 17, 'kasse_bank_post',                 'kasse_bank_post',                 'double',           'Kasse/bank/post'),
('siva_fact_regnskap', 18, 'kundefordringer',                 'kundefordringer',                 'double',           'Kundefordringer'),
('siva_fact_regnskap', 19, 'lederloenn',                      'lederloenn',                      'double',           'Lederlønn'),
('siva_fact_regnskap', 20, 'aksje_selskapskapital',           'aksje_selskapskapital',           'double',           'Aksje/selskapskapital'),
('siva_fact_regnskap', 21, '_loading_ts',                     '_loading_ts',                     'current_timestamp','Lastet tidspunkt');
"""

print("Gold column mappings SQL klar")
print(GOLD_COLUMN_MAPPINGS_SQL[:200])


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
