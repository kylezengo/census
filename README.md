# Census Data Explorer

Interactive Dash app for exploring ACS demographic data — choropleth maps,
scatter plots, correlation matrices, and time-series trends (including
race-segmented trends back to 2009).

## Setup

Requires a free Census API key: https://api.census.gov/data/key_signup.html

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

Data files are gitignored — they total ~1.1 GB.

## Running

```bash
python app.py           # http://127.0.0.1:8050
DEV_MODE=true python app.py   # adds tract + block-group tabs (slow to load)
```

## Deploying

Cloud Run, in GCP project `kylezengo` (under `zengokp-org`).

**One-time setup:**

```bash
gcloud config set project kylezengo
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
  c_timeseries_state.csv c_timeseries_county.csv \
  c_timeseries_state_race.csv c_timeseries_county_race.csv \
  zcta_to_dma.csv \
  state_geom.* county_geom.* zcta_geom.* congressional_district_geom.* \
  gs://kylezengo-census-data/
```

Re-upload whenever you re-run a download script, and keep `fetch_data.py`'s
file list in sync when adding new data.

**Deploy:**

```bash
gcloud builds submit --config cloudbuild.yaml
```

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

## Notes

ACS 5-year estimates pool the prior five years — a point labelled 2024 covers
2020–2024 — so trends lag real turning points.
