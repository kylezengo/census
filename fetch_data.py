"""Download data files from GCS before app startup."""

import os
from google.cloud import storage

BUCKET = os.environ["GCS_BUCKET"]

FILES = [
    # CSVs
    "c_state.csv",
    "c_dma.csv",
    "c_county_state.csv",
    "c_zcta_dma.csv",
    "c_tract.csv",
    "c_block_group.csv",
    "c_timeseries_state.csv",
    "c_timeseries_county.csv",
    "state_name.csv",
    "zcta_to_dma.csv",
    # Shapefiles (all sidecar files required by geopandas)
    "state_geom.shp",
    "state_geom.shx",
    "state_geom.dbf",
    "state_geom.prj",
    "state_geom.cpg",
    "county_geom.shp",
    "county_geom.shx",
    "county_geom.dbf",
    "county_geom.prj",
    "county_geom.cpg",
    "zcta_geom.shp",
    "zcta_geom.shx",
    "zcta_geom.dbf",
    "zcta_geom.prj",
    "zcta_geom.cpg",
    "tract_geom.shp",
    "tract_geom.shx",
    "tract_geom.dbf",
    "tract_geom.prj",
    "tract_geom.cpg",
    "block_group_geom.shp",
    "block_group_geom.shx",
    "block_group_geom.dbf",
    "block_group_geom.prj",
    "block_group_geom.cpg",
]

client = storage.Client()
bucket = client.bucket(BUCKET)

print(f"Downloading {len(FILES)} files from gs://{BUCKET}...")
for name in FILES:
    blob = bucket.blob(name)
    blob.download_to_filename(name)
    print(f"  {name}")

print("Done.")
