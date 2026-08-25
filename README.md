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

`Dockerfile` builds a gunicorn image. At startup `fetch_data.py` pulls the
data files from GCS (`GCS_BUCKET` env var) since they aren't in the repo —
keep its file list in sync when adding new data.

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
