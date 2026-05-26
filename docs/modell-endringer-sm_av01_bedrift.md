# Modellendringer – `sm_av01_bedrift`

Dokumenterer endringer på den semantiske modellen `solution/consumption/sm_av01_bedrift.SemanticModel/` for å (a) lukke BPA-advarsler og (b) gjøre modellen klar for Copilot, Q&A og generell AI-bruk.

**Modell:** `sm_av01_bedrift`
**Endret:** 2026-05-19
**Verktøy:** Endret direkte i TMDL (Tabular Model Definition Language) – Power BI Desktop ikke involvert i selve endringene. Modellen må lukkes og åpnes på nytt for at endringene skal tre i kraft.

---

## Bakgrunn

To problemstillinger ble adressert samtidig:

1. **BPA-advarsler** (Best Practice Analyzer i Tabular Editor) flagget 58 issues på tvers av seks regler.
2. **AI-modenhet** – modellen var skrevet med database-tekniske navn (`bedrift_surrogate_id`, `created_ts`, …) og hadde tynne beskrivelser. Copilot og Q&A trenger menneskespråk, ikke kolonnenavn fra DWH.

Endringene er gjort i to runder.

---

## Runde 1 – BPA og navnerydding

### 1.1 Fjernet automatisk summering på numeriske kolonner

Regel: **"Do not summarize numeric columns"** (19 kolonner).

Numeriske kolonner i faktatabeller skal ikke ha implisitt aggregering – brukere skal eksplisitt velge måltall i stedet. Endret `summarizeBy: sum` → `summarizeBy: none` på:

- `Bedrifter[Kapital]`
- Alle 18 beløpsfelter i `Regnskap` (Sum salgsinntekter, Sum driftsinntekter, Driftsresultat, Årsresultat, Sum eiendeler, Sum egenkapital, Sum gjeld, osv.)

### 1.2 Omdøpt surrogatnøkler til `XxxKey`-konvensjon

Regler: **"First letter of objects must be capitalized"** og **"Mark primary keys"**.

| Tidligere | Nå | Tabeller |
|---|---|---|
| `bedrift_surrogate_id` | **`BedriftKey`** | Bedrifter (PK), Regnskap (FK), Meldingslogg (FK) |
| `dato_surrogate_id` | **`DatoKey`** | Dato (PK), Meldingslogg (FK) |
| `regnskap_surrogate_id` | **`RegnskapKey`** | Regnskap (PK) |
| `regnskapsaar_dato_id` | **`RegnskapsårDatoKey`** | Regnskap (rolle-spillende FK til Dato) |
| `melding_surrogate_id` | **`MeldingKey`** | Meldingslogg (PK) |

PK-kolonnene har fått `isKey`. Alle relasjoner i `relationships.tmdl` er oppdatert. `sourceColumn` er **beholdt uendret** slik at koblingen til kildedata (Fabric Warehouse `lh_av01_gold.siva.*`) fortsatt fungerer.

### 1.3 Omdøpt skjulte hjelpekolonner

| Tidligere | Nå | Tabell |
|---|---|---|
| `maaned` | `MånedNr` | Dato (sortByColumn for `Måned`) |
| `ukedag_nr` | `UkedagNr` | Dato (sortByColumn for `Ukedag`) |
| `created_ts` | `Opprettet` | Meldingslogg |
| `modified_ts` | `Endret` | Meldingslogg |

`sortByColumn`-referansene på `Måned` og `Ukedag` er oppdatert.

### 1.4 Markert tekniske kolonner

På alle skjulte tekniske kolonner (PK/FK + `_loading_ts` + timestamps) er det satt:

- `isHidden`
- `isAvailableInMDX: false` (ytelse: hindrer at de havner i MDX-attribute-hierarki)
- `displayFolder: _Tekniske`
- `///` beskrivelse som forklarer formålet

**Unntak:** `MånedNr` og `UkedagNr` i Dato har **ikke** `isAvailableInMDX: false`. Disse brukes som `sortByColumn`-mål fra synlige kolonner (Måned, Ukedag), og sortByColumn krever at hjelpekolonnen har attribute hierarchy aktivert. De er fortsatt skjult og ligger i `_Sortering` displayFolder.

### 1.5 Format på prosentmål

Regel: **"Percentages should be formatted with thousands separators and 1 decimal"** (4 mål).

Endret `formatString: 0.0%` → `formatString: #,##0.0%;-#,##0.0%;0.0%` på:
- `Driftsmargin %`
- `EK-andel %`
- `Inntektsvekst %`
- `Resultatvekst %`

### 1.6 Format på tekstmål

Regel: **"Provide format string for measures"** (4 mål).

Lagt til `dataType: string` på:
- `Oppdatert`
- `Dataalder`
- `Siste oppdateringstype`
- `Siste oppdateringsstatus`

Dette gjør at BPA-reglene som er designet for numeriske mål ikke flagger disse, og er en presis annotasjon av at målene returnerer tekst (via `SWITCH`/`FORMAT`/`IF`).

---

## Runde 2 – AI-beriking

### 2.1 Beskrivelser på alle synlige kolonner

63 nye `///` beskrivelser lagt til på tvers av Bedrifter (21), Dato (8), Regnskap (19), Meldingslogg (7), Oppdateringsmetadata (8). Alle beløpsfelter har **(NOK)** eksplisitt for at Copilot ikke skal gjette på enhet.

### 2.2 Skjult mellomregningsmål

Mål som kun brukes som ledd i andre beregninger og ikke gir mening i visualer er nå `isHidden`:

- `Driftsinntekter ifjor` (brukes av `Inntektsvekst %`)
- `Årsresultat ifjor` (brukes av `Resultatvekst %`)

De er fortsatt aktive i DAX, men dukker ikke opp i feltlista eller Copilot-forslag.

### 2.3 Skjult Årstall i Regnskap

`Regnskap[Årstall]` er skjult. Time intelligence skal gå via Dato-dimensjonen (`Dato[År]`, `Dato[Kvartal]`, osv.) for å være konsistent på tvers av faktatabeller og fungere med `SAMEPERIODLASTYEAR`.

### 2.4 Synonymordbok (Linguistic Schema)

Ny fil: `cultures/nb-NO.tmdl` med 27 entiteter og ~120 synonymer. Den er lagt til som **ekstra translasjon** (`ref cultureInfo nb-NO` i `model.tmdl`). Modellens hovedkultur er beholdt som `en-US` – Power BI/Analysis Services tillater **ikke** å endre `Culture`/`Collation` på en eksisterende modell (vil gi feilmeldingen "Culture and Collation properties of the Model object may be changed only before any other object has been created"). Den nb-NO linguistic schema fungerer fint som tilleggskultur – Power BI Q&A og Copilot leser synonymer fra alle kulturer modellen har.

`sourceQueryCulture: nb-NO` er allerede satt, så norsk talluttrykk i Power Query / DAX håndteres riktig.

Eksempler:
- **Bedrift** → selskap, foretak, firma, virksomhet, AS, organisasjon
- **Organisasjonsnummer** → orgnr, org.nr
- **Driftsinntekter** → omsetning, inntekt, salgsinntekter, topline
- **Årsresultat** → overskudd, underskudd, fortjeneste, bunnlinje
- **Driftsmargin %** → margin, lønnsomhet, EBIT-margin
- **EK-andel %** → egenkapitalandel, soliditet
- **Sum eiendeler** → totalkapital, balansesum
- **Ansatte** → medarbeidere, arbeidstakere
- **Meldinger** → hendelser, kunngjøringer, BRREG-meldinger

`en-US.tmdl` er beholdt for kompatibilitet og kan slettes manuelt hvis engelsk Q&A ikke er aktuelt.

---

## Eksempler på spørsmål modellen kan besvare

### Tellinger og oversikt
- Hvor mange aktive bedrifter har vi?
- Hvor mange ansatte totalt?
- Hvor mange meldinger har kommet inn siste uke?
- Hvor mange bedrifter er under konkurs?

### Geografi
- Hvilke kommuner har flest aktive bedrifter?
- Topp 10 kommuner etter antall ansatte?
- Hvor mange selskaper er registrert i Trondheim?

### Bransje og klassifisering
- Hvilken næring har størst samlet omsetning?
- Antall bedrifter per organisasjonsform?
- Hvilken sektor har høyest gjennomsnittlig EK-andel?

### Økonomi
- Topp 10 selskaper etter driftsinntekter siste år
- Bedrifter med høyest overskudd
- Bedrifter med lavest soliditet
- Hvilke firmaer har høyest driftsmargin?

### Trend og vekst
- Hvordan har omsetningen utviklet seg siste 5 år?
- Hvilke bedrifter hadde størst inntektsvekst i fjor?
- Total omsetning per kvartal?

### Meldinger fra BRREG
- Hvilke meldingstyper er vanligst?
- Antall konkursmeldinger per måned (trend)?
- Hvilke selskaper har hatt flest hendelser siste år?

### Datakvalitet
- Når ble dataene sist oppdatert? (mål: `Oppdatert`)
- Hvor gamle er dataene? (mål: `Dataalder`)
- Var siste oppdatering vellykket? (mål: `Siste oppdateringsstatus`)

---

## Navnekonvensjoner – etablerte mønstre

For fremtidige tilføyelser i denne modellen:

| Type | Konvensjon | Eksempel |
|---|---|---|
| Synlige kolonner | Norsk, naturlig språk, mellomrom OK | `Antall ansatte`, `Sum driftsinntekter` |
| Synlige mål | Norsk, evt. % eller (NOK) i navn | `Driftsmargin %`, `Inntektsvekst %` |
| Skjulte PK | PascalCase + `Key`-suffiks | `BedriftKey`, `RegnskapKey` |
| Skjulte FK | Samme som PK-en på andre siden | `BedriftKey` (i fakta) |
| Rolle-spillende FK | Forklarende prefiks | `RegnskapsårDatoKey` |
| Skjulte sortering-hjelpere | PascalCase + `Nr`-suffiks | `MånedNr`, `UkedagNr` |
| Skjulte tekniske timestamps | Norsk verb i partisipp | `Opprettet`, `Endret`, `_loading_ts` |
| `displayFolder` for skjulte | Underscore-prefiks så de havner nederst | `_Tekniske`, `_Sortering` |

På alle skjulte kolonner: sett alltid `isHidden`, `isAvailableInMDX: false`, `displayFolder: _Tekniske`, og en kort `///` beskrivelse.

På alle synlige kolonner og mål: alltid en `///` beskrivelse med enhet og typisk bruk. Beløp skal ha **(NOK)** eksplisitt.

---

## Filer som er endret

```
solution/consumption/sm_av01_bedrift.SemanticModel/definition/
├── model.tmdl                          # ref cultureInfo nb-NO lagt til (culture forblir en-US)
├── relationships.tmdl                  # alle FK-referanser oppdatert
├── cultures/
│   ├── en-US.tmdl                      # uendret (kan slettes ved behov)
│   └── nb-NO.tmdl                      # NY – linguistic schema med synonymer
└── tables/
    ├── Bedrifter.tmdl                  # BedriftKey + beskrivelser
    ├── Dato.tmdl                       # DatoKey, MånedNr, UkedagNr + beskrivelser
    ├── Meldingslogg.tmdl               # MeldingKey, BedriftKey, DatoKey, Opprettet, Endret + beskrivelser
    ├── Oppdateringsmetadata.tmdl       # beskrivelser
    ├── Regnskap.tmdl                   # RegnskapKey, BedriftKey, RegnskapsårDatoKey, summarizeBy: none, Årstall skjult + beskrivelser
    └── _Måltall.tmdl                   # prosentformat, dataType: string, mellomregningsmål skjult, (NOK)
```

Ingen endringer er gjort i `sm_av01_bedrift.Report/` – rapporten skal fungere uendret siden ingen omdøpte kolonner var i bruk i visualer.

---

## Når modellen åpnes igjen

1. **Lukk Power BI Desktop** før noen åpner .pbip-en (ellers risikerer dere at modellen i minnet overskriver TMDL-endringene ved neste lagring).
2. Commit gjerne endringene i git først.
3. Åpne `sm_av01_bedrift.pbip`. Power BI Desktop leser TMDL og bygger modellen i minnet.
4. **Re-marker Dato som date table** (Modeling → Mark as date table → kolonne `Dato`). Vi har lagt til `isKey` på `DatoKey`, og det er trygt å ha en sekundær key – men "Mark as date table" bør verifiseres etter slike endringer.
5. Lagre én gang – Power BI Desktop normaliserer TMDL og sjekker syntaks.
6. **Kjør BPA på nytt** i Tabular Editor – de fleste av de opprinnelige advarslene skal være borte.
7. Test Copilot/Q&A med spørsmål som "Hvilke selskaper har høyest omsetning?" eller "Antall konkursmeldinger i 2025".

---

## Kjente vurderinger

- **Kun én `isKey` per tabell:** `Dato[Dato]` hadde opprinnelig `isKey` (satt automatisk av "Mark as date table"), men Power BI/AS tillater bare én `IsKey`-kolonne per tabell. Vi fjernet `isKey` fra `Dato[Dato]` og beholdt den på `DatoKey` (den faktiske primærnøkkelen). Date table-funksjonen styres av `dataCategory: Time` på `Dato[Dato]`, ikke av `isKey`, så time intelligence fungerer fortsatt. Du må kanskje re-marker som date table i Power BI Desktop første gang du åpner modellen.
- **Modellens hovedkultur er `en-US`:** Vi forsøkte først å endre til `nb-NO`, men Power BI/AS tillater ikke å endre `Culture` på en eksisterende modell (gir feilen *"Culture and Collation properties of the Model object may be changed only before any other object has been created"*). Synonymene fungerer fint som ekstra translasjon i `nb-NO.tmdl`. Hvis dere ønsker `nb-NO` som hovedkultur må modellen bygges fra scratch.
- **`SummarizationSetBy = Automatic` annotasjoner er beholdt** på de fleste kolonner. Disse er kun metadata for sporing og overstyrer ikke `summarizeBy: none`-egenskapen. For maksimal robusthet kan annotasjonen settes til `User` på de 19 kolonnene som ble endret, men det er ikke teknisk nødvendig.
