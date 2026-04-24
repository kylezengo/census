"""Download data files from GCS before app startup."""

import os
from google.cloud import storage

BUCKET = os.environ["GCS_BUCKET"]
ACS_YEAR = 2024
DEV_MODE = os.environ.get("DEV_MODE") == "true"

FILES = [
    # ACS year-specific CSVs
    f"c_state_{ACS_YEAR}.csv",
    f"c_dma_{ACS_YEAR}.csv",
    f"c_county_state_{ACS_YEAR}.csv",
    f"c_zcta_dma_{ACS_YEAR}.csv",
    f"c_congressional_district_{ACS_YEAR}.csv",
    f"state_name_{ACS_YEAR}.csv",
    # Timeseries (all years, no suffix)
    "c_timeseries_state.csv",
    "c_timeseries_county.csv",
    # Static mapping
    "zcta_to_dma.csv",
    # Shapefiles
    "state_geom.shp", "state_geom.shx", "state_geom.dbf", "state_geom.prj", "state_geom.cpg",
    "county_geom.shp", "county_geom.shx", "county_geom.dbf", "county_geom.prj", "county_geom.cpg",
    "zcta_geom.shp", "zcta_geom.shx", "zcta_geom.dbf", "zcta_geom.prj", "zcta_geom.cpg",
    "congressional_district_geom.shp", "congressional_district_geom.shx",
    "congressional_district_geom.dbf", "congressional_district_geom.prj", "congressional_district_geom.cpg",
]

if DEV_MODE:
    FILES += [
        f"c_tract_{ACS_YEAR}.csv",
        f"c_block_group_{ACS_YEAR}.csv",
        "tract_geom.shp", "tract_geom.shx", "tract_geom.dbf", "tract_geom.prj", "tract_geom.cpg",
        "block_group_geom.shp", "block_group_geom.shx", "block_group_geom.dbf",
        "block_group_geom.prj", "block_group_geom.cpg",
    ]

client = storage.Client()
bucket = client.bucket(BUCKET)

print(f"Downloading {len(FILES)} files from gs://{BUCKET}...")
for name in FILES:
    blob = bucket.blob(name)
    blob.download_to_filename(name)
    print(f"  {name}")

print("Done.")
