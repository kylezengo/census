"""Download ACS 5-year time series (2009–2022) at state and county level."""

import os
import time
import requests
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

ACS_BASE = "https://api.census.gov/data/{year}/acs/acs5"
YEARS = list(range(2009, 2025))


def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


_start = time.time()
_log(f"Starting timeseries download ({len(YEARS)} years, state + county)")

load_dotenv()
census_api_key = os.getenv("census_api_key")

# Focused summary variables — all fit in one API call per year per geo level
VARS = [
    "B01001_001E",  # Total population
    "B01001_002E",  # Male
    "B01001B_001E",  # Black or African American alone
    "B01001D_001E",  # Asian alone
    "B01001H_001E",  # White alone, not Hispanic or Latino
    "B01001I_001E",  # Hispanic or Latino
    "B19013_001E",  # Median household income
    "B25077_001E",  # Median home value
    "B25064_001E",  # Median gross rent
    "B17001_001E",  # Poverty status — total
    "B17001_002E",  # Below poverty level
    "B23025_003E",  # Civilian labor force
    "B23025_005E",  # Unemployed
    "B15003_001E",  # Educational attainment — total (25+)
    "B15003_022E",  # Bachelor's degree
    "B15003_023E",  # Master's degree
    "B15003_024E",  # Professional degree
    "B15003_025E",  # Doctorate
    "B25003_001E",  # Tenure — total
    "B25003_002E",  # Owner occupied
    "B25003_003E",  # Renter occupied
]

VAR_STR = ",".join(VARS)

# Prefix intermediates with _ so they're easy to drop after deriving pct_ cols
RENAME = {
    "B01001_001E": "Pop",
    "B01001_002E": "_pop_male",
    "B01001B_001E": "_pop_black",
    "B01001D_001E": "_pop_asian",
    "B01001H_001E": "_pop_white_nh",
    "B01001I_001E": "_pop_hispanic",
    "B19013_001E": "Median Household Income",
    "B25077_001E": "Median Home Value",
    "B25064_001E": "Median Gross Rent",
    "B17001_001E": "_poverty_total",
    "B17001_002E": "_poverty_below",
    "B23025_003E": "_labor_force",
    "B23025_005E": "_unemployed",
    "B15003_001E": "_educ_total",
    "B15003_022E": "_educ_bachelors",
    "B15003_023E": "_educ_masters",
    "B15003_024E": "_educ_professional",
    "B15003_025E": "_educ_doctorate",
    "B25003_001E": "_tenure_total",
    "B25003_002E": "_tenure_owner",
    "B25003_003E": "_tenure_renter",
}

MAX_RETRIES = 3
MAX_WORKERS = 6

# B23025 (Employment Status) and B15003 (Educational Attainment) introduced in 2012
VARS_PRE2012 = [
    v for v in VARS if not v.startswith("B23025") and not v.startswith("B15003")
]
VAR_STR_PRE2012 = ",".join(VARS_PRE2012)


def _fetch_year(year, for_clause):
    url = ACS_BASE.format(year=year)
    var_str = VAR_STR if year >= 2012 else VAR_STR_PRE2012
    params = {"get": f"NAME,{var_str}", "for": for_clause, "key": census_api_key}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                df["year"] = year
                return df
            if r.status_code == 404:
                _log(f"  {year} 404 — skipping (variables not available this year)")
                return None
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} ERROR {r.status_code}: {r.text[:120]}")
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} EXCEPTION: {e}")
    return None


def _process(df):
    """Cast to float, clean sentinel, derive pct_ metrics, drop intermediates."""
    for col in VARS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").replace(
                -666666666.0, np.nan
            )
    df = df.rename(columns=RENAME)
    df["pct_male"] = df["_pop_male"] / df["Pop"]
    df["pct_white_nh"] = df["_pop_white_nh"] / df["Pop"]
    df["pct_black"] = df["_pop_black"] / df["Pop"]
    df["pct_hispanic"] = df["_pop_hispanic"] / df["Pop"]
    df["pct_asian"] = df["_pop_asian"] / df["Pop"]
    df["pct_poverty"] = df["_poverty_below"] / df["_poverty_total"]
    df["pct_unemployed"] = df["_unemployed"] / df["_labor_force"]
    df["pct_bachelors_plus"] = (
        df[
            [
                "_educ_bachelors",
                "_educ_masters",
                "_educ_professional",
                "_educ_doctorate",
            ]
        ].sum(axis=1)
        / df["_educ_total"]
    )
    df["pct_owner_occupied"] = df["_tenure_owner"] / df["_tenure_total"]
    df["pct_renter_occupied"] = df["_tenure_renter"] / df["_tenure_total"]
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df.drop(columns=[c for c in df.columns if c.startswith("_")])


# State ########################################################################################
_log(f"Fetching state timeseries ({len(YEARS)} years, {MAX_WORKERS} workers)...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(_fetch_year, y, "state:*"): y for y in YEARS}
    state_dfs = []
    for future in as_completed(futures):
        y = futures[future]
        result = future.result()
        if result is not None:
            state_dfs.append(result)
            _log(f"  state {y}: {len(result)} rows")

ts_state = pd.concat(state_dfs, ignore_index=True)
ts_state = _process(ts_state)
ts_state = ts_state.drop(columns=["state"], errors="ignore").rename(
    columns={"NAME": "state"}
)
ts_state = ts_state.sort_values(["state", "year"]).reset_index(drop=True)
_log(
    f"State timeseries complete: {len(ts_state)} rows, {len(ts_state.columns)} columns"
)

# County #######################################################################################
_log(f"Fetching county timeseries ({len(YEARS)} years, {MAX_WORKERS} workers)...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(_fetch_year, y, "county:*"): y for y in YEARS}
    county_dfs = []
    for future in as_completed(futures):
        y = futures[future]
        result = future.result()
        if result is not None:
            county_dfs.append(result)
            _log(f"  county {y}: {len(result)} rows")

ts_county = pd.concat(county_dfs, ignore_index=True)
ts_county = _process(ts_county)
ts_county["GEOID"] = ts_county["state"] + ts_county["county"]
ts_county = ts_county.drop(columns=["state", "county"], errors="ignore")
ts_county = ts_county.sort_values(["NAME", "year"]).reset_index(drop=True)
_log(
    f"County timeseries complete: {len(ts_county)} rows, {len(ts_county.columns)} columns"
)

# Save #########################################################################################
ts_state.to_csv("c_timeseries_state.csv", index=False)
_log("Saved c_timeseries_state.csv")
ts_county.to_csv("c_timeseries_county.csv", index=False)
_log("Saved c_timeseries_county.csv")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
