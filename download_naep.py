"""Download NAEP (Nation's Report Card) state average scores, 2003-2024.

Source: NAEP Data Service, https://www.nationsreportcard.gov/DataService/ (no key).
Math and reading, grades 4 and 8 — the series where every state has been
assessed every cycle since 2003. State results cover public schools only, so
the national row is "National public" (NP), relabelled to match the ACS frames.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

from census_common import MAX_RETRIES, MAX_WORKERS, log as _log, skip_if_downloaded

API = "https://www.nationsreportcard.gov/DataService/GetAdhocData.aspx"
YEARS = [2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2022, 2024]

# 50 states + DC + National public. The API has no "all states" shorthand.
JURISDICTIONS = (
    "AL,AK,AZ,AR,CA,CO,CT,DE,DC,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,"
    "MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY,NP"
)

# output column -> (subject, grade, composite scale)
SERIES = {
    "naep_math_g4": ("mathematics", 4, "MRPCM"),
    "naep_math_g8": ("mathematics", 8, "MRPCM"),
    "naep_read_g4": ("reading", 4, "RRPCM"),
    "naep_read_g8": ("reading", 8, "RRPCM"),
}

# SDRACE is NAEP's trend race variable. Its categories are mutually exclusive
# (White/Black are non-Hispanic) and Asian includes Pacific Islanders, so the
# mapping onto the ACS group names is close but not exact. "Unclassified" is dropped.
RACE_MAP = {
    "White": "White (non-Hispanic)",
    "Black": "Black",
    "Hispanic": "Hispanic or Latino",
    "Asian/Pacific Islander": "Asian",
    "American Indian/Alaska Native": "American Indian / Alaska Native",
    "Two or more races": "Two or more races",
}

US_LABEL = "United States"

_start = time.time()
skip_if_downloaded(["c_naep_state.csv", "c_naep_state_race.csv"], "NAEP data")


def _fetch(col, variable):
    """One series x one breakdown, all jurisdictions and years. Long df."""
    subject, grade, scale = SERIES[col]
    params = {
        "type": "data",
        "subject": subject,
        "grade": grade,
        "subscale": scale,
        "variable": variable,
        "jurisdiction": JURISDICTIONS,
        "stattype": "MN:MN",
        "Year": ",".join(map(str, YEARS)),
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API, params=params, timeout=180)
            # Error payloads embed raw newlines from .NET stack traces.
            body = json.loads(r.text, strict=False)
            if r.status_code == 200 and body.get("status") == 200:
                df = pd.DataFrame(body["result"])
                # Suppressed cells (small samples) come back as 999 with
                # isStatDisplayable=0.
                df["value"] = df["value"].where(df["isStatDisplayable"] == 1, np.nan)
                df["metric"] = col
                _log(f"  {col} {variable}: {len(df)} rows")
                return df[["jurisLabel", "year", "varValueLabel", "metric", "value"]]
            msg = str(body.get("result"))[:120]
        except (requests.RequestException, ValueError) as e:
            msg = str(e)
        if attempt < MAX_RETRIES - 1:
            time.sleep(2**attempt)
    raise SystemExit(f"NAEP fetch failed for {col} {variable}: {msg}")


def _fetch_all(variable):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(lambda c: _fetch(c, variable), SERIES))
    df = pd.concat(dfs, ignore_index=True)
    df["jurisLabel"] = df["jurisLabel"].replace("National public", US_LABEL)
    return df.rename(columns={"jurisLabel": "state"})


_log(f"Fetching NAEP totals ({len(SERIES)} series, {len(YEARS)} years)...")
totals = _fetch_all("TOTAL")
naep_state = (
    totals.pivot_table(index=["state", "year"], columns="metric", values="value")
    .reset_index()
    .rename_axis(columns=None)
)

_log("Fetching NAEP by race/ethnicity...")
race = _fetch_all("SDRACE")
race = race[race["varValueLabel"].isin(RACE_MAP)]
race["race"] = race["varValueLabel"].map(RACE_MAP)
# Fully suppressed state x race x year rows drop out here; the chart just skips them.
naep_race = (
    race.pivot_table(index=["state", "year", "race"], columns="metric", values="value")
    .reset_index()
    .rename_axis(columns=None)
)

naep_state.sort_values(["state", "year"]).to_csv("c_naep_state.csv", index=False)
_log(f"Saved c_naep_state.csv ({len(naep_state)} rows)")
naep_race.sort_values(["state", "race", "year"]).to_csv(
    "c_naep_state_race.csv", index=False
)
_log(f"Saved c_naep_state_race.csv ({len(naep_race)} rows)")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
