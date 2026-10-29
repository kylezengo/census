# Census Data Explorer

Interactive Dash app for exploring ACS demographic data — choropleth maps,
scatter plots, correlation matrices, age profiles, and time-series trends (including
race-segmented trends back to 1790 where the census measured it), plus NAEP state
test scores back to 1998, and housing-shortage indicators set against metro
land-use regulation.

## Setup

Requires a free Census API key: https://api.census.gov/data/key_signup.html

The historical (pre-2000) data also needs a free IPUMS API key: register for
NHGIS at https://www.nhgis.org, then create a key at
https://account.ipums.org/api_keys and add it to `.env` as `ipums_api_key`.

```bash
pip install -r requirements.txt
cp .env.example .env    # then paste your key into .env
```

## Building the data

Run once, in order. Each script skips work if its outputs already exist;
pass `--force` to re-download.

| Step | Script | Produces |
|---|---|---|
| 1 | `build_zcta_to_dma.py` | `zcta_to_dma.csv` (ZIP→ZCTA→DMA mapping) |
| 2 | `download_shape_files.py` | `*_geom.shp` and friends |
| 3 | `download.py` | `c_*_2024.csv` (cross-sectional ACS) |
| 4 | `download_timeseries.py` | `c_timeseries_*.csv` (2009–2024, incl. race) |
| 5 | `download_naep.py` | `c_naep_state*.csv` (NAEP scores, 1998–2024; no key needed) |
| 6 | `download_census2000.py` | `c_census2000_*.csv` (Census 2000 long form, prepended to the timeseries) |
| 7 | `download_nhgis.py` | `c_nhgis_*.csv` (decennial censuses 1790–1990 via NHGIS; needs `ipums_api_key`) |
| 8 | `download_age_profile.py` | `c_age_profile_2024.csv` (metrics by age, for the Age Profile tab) |
| 9 | `download_housing.py` | `c_housing_state.csv`, `c_housing_metro.csv` (housing-shortage indicators and the Wharton land-use regulation index, for the Housing tab; needs `openpyxl`) |
| 10 | `elections/fetch_elections.py` | MIT Election Lab House, Senate and President returns, 1976–2024, cleaned into `elections/data/`. The House file sits behind a Dataverse guestbook: download `1976-2024-house.tab` by hand from [doi:10.7910/DVN/IG0UN2](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2) into `elections/raw/` first |
| 11 | `elections/fetch_house_inputs.py` | District presidential leans for 2006–2026 lines (The Downballot) and the 2026 House candidate list (Wikipedia) |
| — | `elections/mrp.py` | Backtest only, not part of the forecast: tests whether a demographic (MRP) estimate from the CES survey improves the House model beyond presidential lean. It doesn't (2014–2022). Needs the CES cumulative file (CC0, [doi:10.7910/DVN/II2DB6](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/II2DB6)) in `elections/raw/ces/` and `census_api_key` |
| — | `elections/pageviews.py` | Backtest only, not part of the forecast: tests whether candidates' Wikipedia page views (2016–2024, Wikimedia pageviews API) explain what the Senate forecast misses. They don't. Wikimedia throttles scripts without contact details; set a `wiki_user_agent` environment variable to run it at a normal pace |
| 12 | `elections/senate_polls.py`, `elections/house_model.py` | `c_forecast_{senate,house}_2026.csv` and `c_forecast_{senate,house}_seats_2026.csv` (2026 forecasts), plus `c_backtest_{senate,house}_{races,seats}.csv` (every election 2010–2024 re-forecast with only earlier data), for the Elections tab; needs `scipy` and `lxml`. The Senate forecast blends `senate_model.py`'s fundamentals with state polls (FiveThirtyEight's archive for 1998–2022, Wikipedia race pages for 2024 and 2026). The 2026 Senate race list and the generic-ballot average are set in `senate_model.py` |

Data files are gitignored — they total ~1.1 GB.

## Running

```bash
python app.py           # http://127.0.0.1:8050
DEV_MODE=true python app.py   # adds tract + block-group tabs (slow to load)
```

## Deploying

Cloud Run, in GCP project `kylezengo` (id `project-c7d7310f-ae74-4c95-b02`,
under `zengokp-org`).

**One-time setup:**

```bash
gcloud config set project project-c7d7310f-ae74-4c95-b02
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  containerregistry.googleapis.com storage.googleapis.com
```

**Upload the data** — it isn't in the repo, and `fetch_data.py` pulls it from
GCS at container start. Only the production set is needed (~324 MB); the tract
and block-group files are `DEV_MODE` only.

```bash
gsutil -m cp \
  c_state_2024.csv c_dma_2024.csv c_county_state_2024.csv \
  c_zcta_dma_2024.csv c_congressional_district_2024.csv state_name_2024.csv \
  c_age_profile_2024.csv \
  c_timeseries_state.csv c_timeseries_county.csv \
  c_timeseries_state_race.csv c_timeseries_county_race.csv \
  c_timeseries_state_age.csv c_timeseries_county_age.csv \
  c_naep_state.csv c_naep_state_race.csv \
  c_census2000_state.csv c_census2000_county.csv \
  c_census2000_state_race.csv c_census2000_county_race.csv \
  c_census2000_state_age.csv c_census2000_county_age.csv \
  c_nhgis_state.csv c_nhgis_county.csv \
  c_nhgis_state_race.csv c_nhgis_county_race.csv \
  c_nhgis_state_age.csv c_nhgis_county_age.csv \
  c_housing_state.csv c_housing_metro.csv \
  c_forecast_senate_2026.csv c_forecast_senate_seats_2026.csv \
  c_forecast_house_2026.csv c_forecast_house_seats_2026.csv \
  c_backtest_senate_races.csv c_backtest_senate_seats.csv \
  c_backtest_house_races.csv c_backtest_house_seats.csv \
  zcta_to_dma.csv \
  state_geom.* county_geom.* zcta_geom.* congressional_district_geom.* \
  gs://kylezengo-census-data/
```

Re-upload whenever you re-run a download script, and keep `fetch_data.py`'s
file list in sync when adding new data.

**Deploy:**

```bash
gcloud builds submit --config cloudbuild.yaml \
  --substitutions _TAG=$(git rev-parse --short HEAD)
```

`_TAG` names the image; passing the git sha keeps images traceable to
commits (it defaults to `manual`). `$COMMIT_SHA` only works from a build
trigger, not `builds submit`.

The Cloud Run service account needs `roles/storage.objectViewer` on the bucket.

Sized at 2 GB memory: peak RSS is ~985 MB while loading the data, so 1 GB
leaves too little headroom. Scales to zero between visits; the tradeoff is a
slow (~30s) first request after idle.

## Layout

| File | Role |
|---|---|
| `app.py` | Dash layout + callbacks |
| `config.py` | Static config: metric lists, presets, labels, CPI |
| `census_common.py` | Shared helpers for the download scripts |
| `fetch_data.py` | Pulls data from GCS at container startup |

## Data sources

- **dma_polygons.geojson** — https://team.carto.com/u/andrew/tables/dma_master_polygons/public
- **ZIPCodetoZCTACrosswalk2021UDS.xlsx** — https://udsmapper.org/zip-code-to-zcta-crosswalk/
- **zip_to_dma.csv** — https://gist.github.com/clarkenheim/023882f8d77741f4d5347f80d95bc259
- **c_naep_state*.csv** — NAEP Data Service, https://www.nationsreportcard.gov/DataService/
- **c_housing_metro.csv** (regulation index) — Wharton Residential Land Use Regulatory
  Index 2018, https://real-estate.wharton.upenn.edu/profile/gyourko/. Cite as: Joseph
  Gyourko, Jonathan Hartley, and Jacob Krimmel. "The Local Residential Land Use
  Regulatory Environment Across U.S. Housing Markets: Evidence from a New Wharton
  Index." NBER Working Paper 26573, 2019.
- **c_nhgis_*.csv** — IPUMS NHGIS time series tables. Cite as: Jonathan Schroeder,
  David Van Riper, Steven Manson, Grace Cooper, Zachary Krause, Tracy Kugler, Tsu Zhu,
  and Steven Ruggles. *IPUMS National Historical Geographic Information System:
  Version 21.0* [dataset]. Minneapolis, MN: IPUMS. 2026.
  http://doi.org/10.18128/D050.V21.0. NHGIS terms forbid redistributing the data, so
  the CSVs stay out of git and the GCS bucket must stay private.
- **c_forecast_*.csv** (election history) — MIT Election Data and Science Lab, Harvard
  Dataverse: U.S. House 1976–2024 (doi:10.7910/DVN/IG0UN2), U.S. Senate statewide
  1976–2024 (doi:10.7910/DVN/PEJ5QU), U.S. President 1976–2024 (doi:10.7910/DVN/42MVDX).
- **c_forecast_*.csv** (district leans) — The Downballot, presidential results by
  congressional district, https://www.the-downballot.com/p/the-downballots-calculations-of-presidential
- **c_forecast_*.csv** (2026 candidates) — Wikipedia, "2026 United States House of
  Representatives elections" and "2026 United States Senate elections" (CC BY-SA).
  Generic ballot: RealClearPolling average, read 2026-10-28; 2012–2024 finals from
  Wikipedia's House election pages and FiveThirtyEight.
- **c_forecast_senate*.csv** (polls) — FiveThirtyEight pollster-ratings `raw_polls.csv`,
  https://github.com/fivethirtyeight/data/tree/master/pollster-ratings (1998–2022), and the
  polling tables on Wikipedia's 2024 and 2026 Senate race pages (CC BY-SA), read 2026-10-28.

## Notes

ACS 5-year estimates pool the prior five years — a point labelled 2024 covers
2020–2024 — so trends lag real turning points.

The Trends series opens with a Census 2000 point from the decennial long form
(SF3), the ACS's predecessor; there is no data for 2001–08. Its income and
poverty refer to 1999, and the inflation adjustment uses 1999 prices for income.
Householder-age medians for 2000 are interpolated from income bins (2000 only
published 10-year brackets); checked to within ~0.5% typically.

Before 2000 the series uses NHGIS *time series* tables only — measures NHGIS has
harmonized across censuses — so each metric starts when that exists: population
1790, % male 1820, most others 1970, White non-Hispanic and median income 1980.
Home value, rent and % Asian (NHGIS merges Asian with Pacific Islander) stay 2000+.
Income by race and by householder age have no time series, so they come from the
1980 and 1990 source tables: race medians are the Census Bureau's published ones
(1980 lacks Asian and White non-Hispanic; its US row is interpolated from summed
state bins, a method that reproduces every published 1980 median exactly), and
1990 age medians are interpolated from 9 coarse bins (~1% off typically). County boundaries are as drawn in each census, and territories
are separate series (e.g. "Dakota Territory"). The national row is the full US
where NHGIS has it and the contiguous US otherwise (1880–1950 except 1900/1910).
