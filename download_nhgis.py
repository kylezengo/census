"""Download pre-2000 decennial census data from IPUMS NHGIS for the Trends tab.

Uses NHGIS time series tables only — the ones NHGIS has harmonized so a measure
means the same thing across census years. Coverage therefore varies by metric:
population from 1790, sex from 1820, most others from 1970 or 1980, and nothing
for home value or rent (no time series exists). The output columns match
download_timeseries.py so the app can prepend these rows.

Requires a free IPUMS account registered for NHGIS, with its API key in .env as
`ipums_api_key`. Data use must cite NHGIS (see the README).
"""

import io
import os
import sys
import time
import zipfile

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from census_common import log as _log, skip_if_downloaded

API = "https://api.ipums.org/extracts"
PARAMS = {"collection": "nhgis", "version": "2"}
US_LABEL = "United States"

OUTPUTS = {
    "state": "c_nhgis_state.csv",
    "county": "c_nhgis_county.csv",
    "state_race": "c_nhgis_state_race.csv",
    "county_race": "c_nhgis_county_race.csv",
}

_start = time.time()
skip_if_downloaded(list(OUTPUTS.values()), "NHGIS data")
load_dotenv()
if not os.getenv("ipums_api_key"):
    sys.exit(
        "ipums_api_key not set — add it to .env. Free key (after registering for "
        "NHGIS at https://www.nhgis.org): https://account.ipums.org/api_keys"
    )
HEADERS = {"Authorization": os.getenv("ipums_api_key")}


def _decades(first, last=1990):
    return [str(y) for y in range(first, last + 1, 10)]


# Time series table -> first decade wanted. Everything stops at 1990: 2000 on comes
# from download_census2000.py (SF3) and the ACS.
TABLES = {
    "A00": 1790,  # total population
    "A08": 1820,  # persons by sex
    "B18": 1970,  # persons by race (single race)
    "A35": 1970,  # Hispanic or Latino persons
    "CY6": 1980,  # single race/ethnicity — White non-Hispanic
    "B69": 1970,  # educational attainment, 25+
    "B84": 1970,  # labor force and employment status, 16+
    "B79": 1980,  # median household income (previous year)
    "AX6": 1970,  # persons for whom poverty status is determined
    "CL6": 1970,  # persons below poverty level
    "B37": 1970,  # occupied housing units by tenure
    "B39": 1970,  # tenure by race of householder (White, Black)
    "AE2": 1980,  # tenure by race of householder (adds AIAN)
    "CZ5": 1980,  # tenure by Hispanic origin of householder
    "D03": 1980,  # tenure by Hispanic origin by race (White non-Hispanic); no nation level
}
NO_NATION = {"D03"}


def _request_extract():
    levels = ["nation", "state", "county"]
    body = {
        "timeSeriesTables": {
            t: {
                "geogLevels": [g for g in levels if not (t in NO_NATION and g == "nation")],
                "years": _decades(first),
            }
            for t, first in TABLES.items()
        },
        "timeSeriesTableLayout": "time_by_row_layout",
        "dataFormat": "csv_header",
        "description": "census explorer: pre-2000 trends",
    }
    r = requests.post(API, params=PARAMS, headers=HEADERS, json=body, timeout=60)
    if r.status_code != 200:
        sys.exit(f"NHGIS extract request failed {r.status_code}: {r.text[:300]}")
    number = r.json()["number"]
    _log(f"Submitted NHGIS extract #{number}; waiting for it to build...")
    for _ in range(180):
        status = requests.get(f"{API}/{number}", params=PARAMS, headers=HEADERS, timeout=60).json()
        if status["status"] == "completed":
            return status["downloadLinks"]["tableData"]["url"]
        if status["status"] in ("failed", "canceled"):
            sys.exit(f"NHGIS extract #{number} {status['status']}")
        time.sleep(10)
    sys.exit(f"NHGIS extract #{number} still not done after 30 minutes")


def _read_tables(url):
    content = requests.get(url, headers=HEADERS, timeout=600).content
    frames = {}
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for level in ("nation", "state", "county"):
            name = next(n for n in z.namelist() if n.endswith(f"_ts_nominal_{level}.csv"))
            # csv_header adds a second header row of descriptions; skip it.
            frames[level] = pd.read_csv(
                z.open(name), skiprows=[1], encoding="latin-1", dtype=str
            )
    for df in frames.values():
        num = [c for c in df.columns if c[:3] in TABLES]
        df[num] = df[num].apply(pd.to_numeric, errors="coerce")
        df["year"] = df["YEAR"].astype(int)
    return frames


def _col(df, code):
    """A time series column, or all-NaN if this level/table didn't include it."""
    return df[code] if code in df.columns else pd.Series(np.nan, index=df.index)


def _totals(df):
    out = df[["year"]].copy()
    pop = _col(df, "A00AA")
    out["Pop"] = pop
    out["pct_male"] = _col(df, "A08AA") / (_col(df, "A08AA") + _col(df, "A08AB"))
    out["pct_black"] = _col(df, "B18AB") / pop
    out["pct_hispanic"] = _col(df, "A35AA") / pop
    out["pct_white_nh"] = _col(df, "CY6AA") / pop
    educ = _col(df, "B69AA") + _col(df, "B69AB") + _col(df, "B69AC")
    out["pct_bachelors_plus"] = _col(df, "B69AC") / educ
    out["pct_unemployed"] = _col(df, "B84AE") / _col(df, "B84AC")
    out["Median Household Income"] = _col(df, "B79AA").where(_col(df, "B79AA") > 0)
    out["pct_poverty"] = _col(df, "CL6AA") / _col(df, "AX6AA")
    occupied = _col(df, "B37AA") + _col(df, "B37AB")
    out["pct_owner_occupied"] = _col(df, "B37AA") / occupied
    out["pct_renter_occupied"] = _col(df, "B37AB") / occupied
    return out.replace([np.inf, -np.inf], np.nan)


# Race segment: only population and tenure have NHGIS time series by race.
# group -> (population column, owner column, renter column)
RACE_COLS = {
    "White alone": ("B18AA", "B39AA", "B39AE"),
    "Black": ("B18AB", "B39AB", "B39AF"),
    "American Indian / Alaska Native": ("B18AC", "AE2AC", "AE2AI"),
    "Hispanic or Latino": ("A35AA", "CZ5AB", "CZ5AD"),
    "White (non-Hispanic)": ("CY6AA", "D03AA", "D03AM"),
}


def _race(df, keys):
    frames = []
    for race, (pop, owner, renter) in RACE_COLS.items():
        part = df[keys + ["year"]].copy()
        part["race"] = race
        part["Pop"] = _col(df, pop)
        occupied = _col(df, owner) + _col(df, renter)
        part["pct_owner_occupied"] = _col(df, owner) / occupied
        part["pct_renter_occupied"] = _col(df, renter) / occupied
        frames.append(part)
    out = pd.concat(frames, ignore_index=True).replace([np.inf, -np.inf], np.nan)
    metrics = ["Pop", "pct_owner_occupied", "pct_renter_occupied"]
    return out.dropna(subset=metrics, how="all")


def _with_us_from_states(nation, state, codes):
    """D03 has no nation level, so build the US row by summing the states."""
    sums = state.groupby("year")[codes].sum(min_count=1).reset_index()
    return nation.drop(columns=codes, errors="ignore").merge(sums, on="year", how="left")


def _county_names(county):
    """Join on today's ACS county name when the FIPS code still exists; otherwise
    keep NHGIS's historical name, formatted like the ACS ("X County, State")."""
    acs = pd.read_csv("c_timeseries_county.csv", usecols=["NAME", "GEOID"], dtype=str)
    acs = acs.drop_duplicates("GEOID", keep="last").set_index("GEOID")["NAME"]
    geoid = county["STATEFP"].fillna("") + county["COUNTYFP"].fillna("")
    geoid = geoid.where(geoid.str.len() == 5)
    historical = county["COUNTY"] + ", " + county["STATE"]
    return geoid, geoid.map(acs).fillna(historical)


_log(f"Requesting {len(TABLES)} NHGIS time series tables (nation, state, county)...")
frames = _read_tables(_request_extract())
# NHGIS has two national rows: G1, the United States as reported, and G2, the
# contiguous US. 1880-1950 mostly has only G2 (1900 and 1910 have both), so take
# G1 where it exists and G2 otherwise. The gap is Alaska and Hawaii, then
# territories: under 0.5% of the total.
nation = (
    frames["nation"].sort_values("GISJOIN").drop_duplicates("year", keep="first")
)
nation = _with_us_from_states(nation, frames["state"], ["D03AA", "D03AM"])
# Territories (e.g. "Dakota Territory") stay their own series: their borders
# rarely match the later states. Military abroad (1900) isn't a place.
state = frames["state"][frames["state"]["STATE"] != "Persons in the Military"].copy()
county = frames["county"]
nation["state_name"] = US_LABEL
state["state_name"] = state["STATE"]
county["GEOID"], county["NAME_OUT"] = _county_names(county)
_log(
    f"  rows: nation {len(nation)}, state {len(state)}, county {len(county)} "
    f"({county['GEOID'].isna().sum()} historical county rows without a current FIPS)"
)

st = pd.concat([nation, state], ignore_index=True)
st_tot = _totals(st).assign(state=st["state_name"])
st_race = _race(st.assign(state=st["state_name"]), ["state"])
co_tot = _totals(county).assign(NAME=county["NAME_OUT"], GEOID=county["GEOID"])
co_race = _race(county.assign(NAME=county["NAME_OUT"]), ["NAME"])

st_tot.sort_values(["state", "year"]).to_csv(OUTPUTS["state"], index=False)
co_tot.sort_values(["NAME", "year"]).to_csv(OUTPUTS["county"], index=False)
st_race.sort_values(["state", "race", "year"]).to_csv(OUTPUTS["state_race"], index=False)
co_race.sort_values(["NAME", "race", "year"]).to_csv(OUTPUTS["county_race"], index=False)
for f in OUTPUTS.values():
    _log(f"Saved {f}")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
