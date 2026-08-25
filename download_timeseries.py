"""Download ACS 5-year time series (2009-2024) at state and county level."""

import time
import requests
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from census_common import MAX_RETRIES, MAX_WORKERS, log as _log, require_api_key, skip_if_downloaded

ACS_BASE = "https://api.census.gov/data/{year}/acs/acs5"
YEARS = list(range(2009, 2025))

_start = time.time()
_log(f"Starting timeseries download ({len(YEARS)} years, state + county)")

census_api_key = require_api_key()

skip_if_downloaded(
    [
        "c_timeseries_state.csv",
        "c_timeseries_county.csv",
        "c_timeseries_state_race.csv",
        "c_timeseries_county_race.csv",
        "c_timeseries_state_age.csv",
        "c_timeseries_county_age.csv",
    ],
    "Timeseries data",
)

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

# B23025 (Employment Status) and B15003 (Educational Attainment) introduced in 2012
VARS_PRE2012 = [
    v for v in VARS if not v.startswith("B23025") and not v.startswith("B15003")
]
VAR_STR_PRE2012 = ",".join(VARS_PRE2012)

# Race-iterated tables ##########################################################################
# ACS iterates these tables by race/ethnicity via a letter suffix. All are available back to
# 2009, so the race series covers the same span as the all-races series above.
# Note the groups overlap: "White alone" includes Hispanic white respondents, who are also
# counted in "Hispanic or Latino". "White alone, not Hispanic" is the non-overlapping variant.
RACE_SUFFIXES = {
    "A": "White alone",
    "B": "Black",
    "C": "American Indian / Alaska Native",
    "D": "Asian",
    "E": "Native Hawaiian / Pacific Islander",
    "F": "Some other race",
    "G": "Two or more races",
    "H": "White (non-Hispanic)",
    "I": "Hispanic or Latino",
}

# 18 vars per race. All 9 races at once would be 162 — over the API's 50-var cap — so we
# issue one request per race per year.
RACE_VAR_TEMPLATES = {
    "Pop": ["B01001{s}_001E"],
    "Median Household Income": ["B19013{s}_001E"],
    "_poverty_total": ["B17001{s}_001E"],
    "_poverty_below": ["B17001{s}_002E"],
    "_educ_total": ["C15002{s}_001E"],
    "_educ_bachelors_plus": ["C15002{s}_006E", "C15002{s}_011E"],  # male + female
    "_tenure_total": ["B25003{s}_001E"],
    "_tenure_owner": ["B25003{s}_002E"],
    "_tenure_renter": ["B25003{s}_003E"],
    # C23002 splits by sex and by under/over 65; civilian labor force and unemployed
    # each need four lines summed.
    "_labor_force": [
        "C23002{s}_006E",  # male 16-64 civilian in labor force
        "C23002{s}_011E",  # male 65+ in labor force
        "C23002{s}_019E",  # female 16-64 civilian in labor force
        "C23002{s}_024E",  # female 65+ in labor force
    ],
    "_unemployed": [
        "C23002{s}_008E",
        "C23002{s}_013E",
        "C23002{s}_021E",
        "C23002{s}_026E",
    ],
}


# Age-of-householder tables ####################################################################
# B19049 breaks median household income by the householder's age. Only income is
# offered this way — there is no age-iterated poverty/education/tenure — so age
# segmentation covers a single metric. Available 2009-2024, same span as the rest.
AGE_BRACKETS = {
    "B19049_002E": "Under 25",
    "B19049_003E": "25 to 44",
    "B19049_004E": "45 to 64",
    "B19049_005E": "65 and over",
}

AGE_VAR_STR = ",".join(AGE_BRACKETS)


def _race_vars(suffix):
    """Flat list of API variable names for one race suffix."""
    return [
        tmpl.format(s=suffix)
        for tmpls in RACE_VAR_TEMPLATES.values()
        for tmpl in tmpls
    ]


def _fetch_all_years(for_clause, label):
    _log(f"Fetching {label} timeseries ({len(YEARS)} years, {MAX_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_year, y, for_clause): y for y in YEARS}
        dfs = []
        for future in as_completed(futures):
            y = futures[future]
            result = future.result()
            if result is not None:
                dfs.append(result)
                _log(f"  {label} {y}: {len(result)} rows")
    return pd.concat(dfs, ignore_index=True)


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


def _fetch_race_year(year, suffix, for_clause):
    """Fetch one race's variables for one year. Returns a df with a `race` column."""
    url = ACS_BASE.format(year=year)
    var_str = ",".join(_race_vars(suffix))
    params = {"get": f"NAME,{var_str}", "for": for_clause, "key": census_api_key}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                df["year"] = year
                # Collapse this race's suffixed columns into suffix-free metric names
                # before concatenation, so all races share one schema.
                out = df[["NAME", "year"]].copy()
                out["race"] = RACE_SUFFIXES[suffix]
                for out_name, tmpls in RACE_VAR_TEMPLATES.items():
                    cols = [tmpl.format(s=suffix) for tmpl in tmpls]
                    vals = df[cols].apply(pd.to_numeric, errors="coerce").replace(
                        -666666666.0, np.nan
                    )
                    out[out_name] = vals.sum(axis=1, min_count=1)
                return out
            if r.status_code == 404:
                return None
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} {suffix} ERROR {r.status_code}: {r.text[:120]}")
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} {suffix} EXCEPTION: {e}")
    return None


def _fetch_all_race_years(for_clause, label):
    """One request per (year, race) — 16 x 9 = 144 calls per geo level."""
    jobs = [(y, s) for y in YEARS for s in RACE_SUFFIXES]
    _log(f"Fetching {label} race timeseries ({len(jobs)} requests, {MAX_WORKERS} workers)...")
    dfs = []
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_race_year, y, s, for_clause): (y, s) for y, s in jobs
        }
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result is not None:
                dfs.append(result)
            if done % 25 == 0 or done == len(jobs):
                _log(f"  {label} race: {done}/{len(jobs)} requests done")
    return pd.concat(dfs, ignore_index=True)


def _fetch_age_year(year, for_clause):
    """One request per year: all four age brackets, reshaped long."""
    url = ACS_BASE.format(year=year)
    params = {"get": f"NAME,{AGE_VAR_STR}", "for": for_clause, "key": census_api_key}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                out = df[["NAME"]].copy()
                for col, label in AGE_BRACKETS.items():
                    out[label] = pd.to_numeric(df[col], errors="coerce").replace(
                        -666666666.0, np.nan
                    )
                out = out.melt(
                    id_vars="NAME",
                    var_name="age",
                    value_name="Median Household Income",
                )
                out["year"] = year
                return out
            if r.status_code == 404:
                return None
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} age ERROR {r.status_code}: {r.text[:120]}")
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                _log(f"  {year} age EXCEPTION: {e}")
    return None


def _fetch_all_age_years(for_clause, label):
    _log(f"Fetching {label} age timeseries ({len(YEARS)} years)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_fetch_age_year, y, for_clause) for y in YEARS]
        dfs = [f.result() for f in as_completed(futures)]
    return pd.concat([d for d in dfs if d is not None], ignore_index=True)


def _process_race(out):
    """Derive pct_ metrics from the collapsed per-race counts, drop intermediates."""
    out = out.copy()
    out["pct_poverty"] = out["_poverty_below"] / out["_poverty_total"]
    out["pct_bachelors_plus"] = out["_educ_bachelors_plus"] / out["_educ_total"]
    out["pct_unemployed"] = out["_unemployed"] / out["_labor_force"]
    out["pct_owner_occupied"] = out["_tenure_owner"] / out["_tenure_total"]
    out["pct_renter_occupied"] = out["_tenure_renter"] / out["_tenure_total"]
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    return out.drop(columns=[c for c in out.columns if c.startswith("_")])


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


# State (+ US total, appended as a "United States" row) ########################################
ts_state = _process(_fetch_all_years("state:*", "state"))
ts_state = ts_state.drop(columns=["state"], errors="ignore").rename(
    columns={"NAME": "state"}
)
ts_us = _process(_fetch_all_years("us:*", "us"))
ts_us = ts_us.drop(columns=["us"], errors="ignore").rename(columns={"NAME": "state"})
ts_state = pd.concat([ts_state, ts_us], ignore_index=True)
ts_state = ts_state.sort_values(["state", "year"]).reset_index(drop=True)
_log(
    f"State timeseries complete: {len(ts_state)} rows, {len(ts_state.columns)} columns"
)

# County #######################################################################################
ts_county = _process(_fetch_all_years("county:*", "county"))
ts_county["GEOID"] = ts_county["state"] + ts_county["county"]
ts_county = ts_county.drop(columns=["state", "county"], errors="ignore")
ts_county = ts_county.sort_values(["NAME", "year"]).reset_index(drop=True)
_log(
    f"County timeseries complete: {len(ts_county)} rows, {len(ts_county.columns)} columns"
)

# Race-segmented (long format: one row per geography x year x race) ############################
raw_state_race = _fetch_all_race_years("state:*", "state")
raw_us_race = _fetch_all_race_years("us:*", "us")
raw_county_race = _fetch_all_race_years("county:*", "county")

ts_state_race = pd.concat(
    [_process_race(raw_state_race), _process_race(raw_us_race)], ignore_index=True
)
ts_state_race = ts_state_race.rename(columns={"NAME": "state"})
ts_state_race = ts_state_race.sort_values(["state", "race", "year"]).reset_index(drop=True)
_log(f"State race timeseries complete: {len(ts_state_race)} rows")

ts_county_race = _process_race(raw_county_race)
ts_county_race = ts_county_race.sort_values(["NAME", "race", "year"]).reset_index(drop=True)
_log(f"County race timeseries complete: {len(ts_county_race)} rows")

# Age-of-householder income (long format: geography x year x age bracket) ######################
ts_state_age = pd.concat(
    [_fetch_all_age_years("state:*", "state"), _fetch_all_age_years("us:*", "us")],
    ignore_index=True,
).rename(columns={"NAME": "state"})
ts_state_age = ts_state_age.sort_values(["state", "age", "year"]).reset_index(drop=True)
_log(f"State age timeseries complete: {len(ts_state_age)} rows")

ts_county_age = _fetch_all_age_years("county:*", "county")
ts_county_age = ts_county_age.sort_values(["NAME", "age", "year"]).reset_index(drop=True)
_log(f"County age timeseries complete: {len(ts_county_age)} rows")

# Backfill 2009-2011 education/unemployment in the all-races series ############################
# B15003/B23025 only start in 2012, but the race-iterated C15002/C23002 reach back to
# 2009. Groups A-G partition the population (H/I are overlays), so summing them
# reproduces the all-races figure exactly — verified to 6dp against B15003 for 2012.
_EXCLUSIVE_RACES = [
    RACE_SUFFIXES[s] for s in "ABCDEFG"
]


def _backfill_pre2012(all_df, raw_race, name_col):
    counts = raw_race[raw_race["race"].isin(_EXCLUSIVE_RACES)]
    counts = counts[counts["year"] < 2012]
    if counts.empty:
        return all_df
    agg = counts.groupby(["NAME", "year"], as_index=False)[
        ["_educ_total", "_educ_bachelors_plus", "_labor_force", "_unemployed"]
    ].sum(min_count=1)
    agg["pct_bachelors_plus"] = agg["_educ_bachelors_plus"] / agg["_educ_total"]
    agg["pct_unemployed"] = agg["_unemployed"] / agg["_labor_force"]
    agg = agg.rename(columns={"NAME": name_col})[
        [name_col, "year", "pct_bachelors_plus", "pct_unemployed"]
    ]
    merged = all_df.merge(agg, on=[name_col, "year"], how="left", suffixes=("", "_bf"))
    for col in ["pct_bachelors_plus", "pct_unemployed"]:
        merged[col] = merged[col].fillna(merged[f"{col}_bf"])
    return merged.drop(columns=[c for c in merged.columns if c.endswith("_bf")])


ts_state = _backfill_pre2012(
    ts_state, pd.concat([raw_state_race, raw_us_race], ignore_index=True), "state"
)
ts_county = _backfill_pre2012(ts_county, raw_county_race, "NAME")
_log("Backfilled 2009-2011 education/unemployment from race-iterated tables")

# Save #########################################################################################
ts_state.to_csv("c_timeseries_state.csv", index=False)
_log("Saved c_timeseries_state.csv")
ts_county.to_csv("c_timeseries_county.csv", index=False)
_log("Saved c_timeseries_county.csv")
ts_state_race.to_csv("c_timeseries_state_race.csv", index=False)
_log("Saved c_timeseries_state_race.csv")
ts_county_race.to_csv("c_timeseries_county_race.csv", index=False)
_log("Saved c_timeseries_county_race.csv")
ts_state_age.to_csv("c_timeseries_state_age.csv", index=False)
_log("Saved c_timeseries_state_age.csv")
ts_county_age.to_csv("c_timeseries_county_age.csv", index=False)
_log("Saved c_timeseries_county_age.csv")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
