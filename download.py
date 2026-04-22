""" Download census data"""

import os
import re
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ACS_URL = "https://api.census.gov/data/2024/acs/acs5"


def _log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


_start = time.time()
_log("Starting Census download")

# Authentication
load_dotenv()
census_api_key = os.getenv("census_api_key")

# Load files
zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={"zcta": object})


# --- DOWNLOAD DATA ---
# Get variable options
_log("Fetching variable definitions from Census API...")
response = requests.get(
    "https://api.census.gov/data/2022/acs/acs5/variables.json", timeout=20
)
variables_json = response.json()

variables = pd.DataFrame.from_dict(variables_json["variables"], orient="index")
variables = variables.reset_index(names="variable")
_log(f"  Loaded {len(variables)} variable definitions")

# Get data for variables
var_misc = [
    "B11012_001E",  # N Households
    "B19001_014E",  # N Household Income 100-124
    "B19001_015E",  # N Household Income 125-149
    "B19001_016E",  # N Household Income 150-199
    "B19001_017E",  # N Household Income 200+
    "B19049_003E",  # Median Household Income 25-44
    "B19013_001E",  # Median Household Income (overall)
    "B25077_001E",  # Median Home Value
    "B25064_001E",  # Median Gross Rent
]

groups = [
    "B01001",  # Sex by Age
    "B01001A",  # Sex by Age (White Alone)
    "B01001B",  # Sex by Age (Black or African American Alone)
    "B01001C",  # Sex by Age (American Indian and Alaska Native Alone)
    "B01001D",  # Sex by Age (Asian Alone)
    "B01001E",  # Sex by Age (Native Hawaiian and Other Pacific Islander Alone)
    "B01001F",  # Sex by Age (Some Other Race Alone)
    "B01001G",  # Sex by Age (Two or More Races)
    "B01001H",  # Sex by Age (White Alone, Not Hispanic or Latino)
    "B01001I",  # Sex by Age (Hispanic or Latino)
    "B15003",  # Educational Attainment
    "B23025",  # Employment Status
    "B17001",  # Poverty Status
    "B25003",  # Tenure (owner vs renter)
    "misc",
]

MAX_VARS_PER_CALL = 49

var_groups = {}
metrics = []
for g in groups:
    if g == "misc":
        var_groups[g] = ",".join(var_misc)
        metrics.extend(var_misc)
    else:
        vars_list = variables.loc[variables["group"] == g, "variable"].tolist()
        metrics.extend(vars_list)
        if len(vars_list) <= MAX_VARS_PER_CALL:
            var_groups[g] = ",".join(vars_list)
        else:
            for chunk_i, start in enumerate(
                range(0, len(vars_list), MAX_VARS_PER_CALL)
            ):
                chunk = vars_list[start:start + MAX_VARS_PER_CALL]
                var_groups[f"{g}_chunk{chunk_i}"] = ",".join(chunk)

_log(f"Variable groups: {len(var_groups)} groups, {len(metrics)} total metrics")

# Make API calls for each geography level
# Merge on FIPS identifiers (not NAME) to avoid silent mismatches if NAME strings differ
geo_join_cols = {
    "state": ["NAME", "state"],
    "county": ["NAME", "state", "county"],
    "zip code tabulation area": ["NAME", "zip code tabulation area"],
}

geo_level = ["state", "county", "zip code tabulation area"]
dfs = {}
for level in geo_level:
    join_cols = geo_join_cols[level]
    _log(f"Fetching {level} data ({len(var_groups)} API calls)...")
    var_dfs = []
    for i, var_str in enumerate(var_groups.values(), 1):
        params = {"get": f"NAME,{var_str}", "for": f"{level}:*", "key": census_api_key}
        response = requests.get(ACS_URL, params=params, timeout=20)

        if response.status_code != 200:
            _log(f"  ERROR group {i}: {response.status_code} {response.text}")
        else:
            data = response.json()
            data_df = pd.DataFrame(data[1:], columns=data[0])
            var_dfs.append(data_df)
            _log(f"  {level}: group {i}/{len(var_groups)} done ({len(data_df)} rows)")

    merged_df = var_dfs[0]
    for chunk_df in var_dfs[1:]:
        chunk_df = chunk_df[[c for c in chunk_df.columns if c in join_cols or c not in merged_df.columns]]
        merged_df = merged_df.merge(chunk_df, how="outer", on=join_cols)

    dfs[level] = merged_df
    _log(
        f"  {level}: complete ({len(merged_df)} rows, {len(merged_df.columns)} columns)"
    )

# Parallel download helpers ########################################################################
MAX_WORKERS = 5
MAX_RETRIES = 3


def _fetch_geo(for_clause, in_clause, merge_cols):
    """Fetch all variable groups for one geography unit, with retry on failure."""
    group_dfs = []
    for group_var_str in var_groups.values():
        req_params = {
            "get": f"NAME,{group_var_str}",
            "for": for_clause,
            "in": in_clause,
            "key": census_api_key,
        }
        for attempt in range(MAX_RETRIES):
            resp = requests.get(ACS_URL, params=req_params, timeout=20)
            if resp.status_code == 200:
                rows = resp.json()
                group_dfs.append(pd.DataFrame(rows[1:], columns=rows[0]))
                break
            if attempt < MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                print(
                    f"Error {resp.status_code} after {MAX_RETRIES} attempts: {resp.text}"
                )

    if not group_dfs:
        return None
    merged = group_dfs[0]
    for grp_df in group_dfs[1:]:
        grp_df = grp_df[[c for c in grp_df.columns if c in merge_cols or c not in merged.columns]]
        merged = merged.merge(grp_df, how="outer", on=merge_cols)
    return merged


def _fetch_state_tracts(state_fips):
    return _fetch_geo(
        "tract:*", f"state:{state_fips}", ["NAME", "state", "county", "tract"]
    )


def _fetch_county_block_groups(state_fips, county):
    return _fetch_geo(
        "block group:*",
        f"state:{state_fips} county:{county}",
        ["NAME", "state", "county", "tract", "block group"],
    )


# tract ############################################################################################
states = dfs["state"]["state"].unique()
_log(
    f"Fetching tracts for {len(states)} states "
    f"({len(var_groups)} groups each, {MAX_WORKERS} workers)..."
)
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(_fetch_state_tracts, fips): fips for fips in states}
    tract_dfs = []
    for i, future in enumerate(as_completed(futures), 1):
        fips = futures[future]
        result = future.result()
        if result is not None:
            tract_dfs.append(result)
            _log(
                f"  tracts: {i}/{len(states)} states done (state {fips}, {len(result)} tracts)"
            )
        else:
            _log(f"  tracts: WARNING — state {fips} returned no data")

dfs["tract"] = pd.concat(tract_dfs, ignore_index=True)
_log(f"Tracts complete: {len(dfs['tract'])} total rows")

# block group ######################################################################################
state_counties = {
    "36": ["005", "047", "061", "081", "085"],  # NY counties (NYC)
    "06": ["037", "075"],  # CA counties (LA, SF)
}
county_pairs = [
    (state_fips, county)
    for state_fips, counties in state_counties.items()
    for county in counties
]
_log(f"Fetching block groups for {len(county_pairs)} counties...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {
        executor.submit(_fetch_county_block_groups, s, c): (s, c)
        for s, c in county_pairs
    }
    block_group_dfs = []
    for i, future in enumerate(as_completed(futures), 1):
        s, c = futures[future]
        result = future.result()
        if result is not None:
            block_group_dfs.append(result)
            _log(
                f"  block groups: {i}/{len(county_pairs)} done "
                f"(state {s} county {c}, {len(result)} block groups)"
            )
        else:
            _log(f"  block groups: WARNING — state {s} county {c} returned no data")

dfs["block group"] = pd.concat(block_group_dfs, ignore_index=True)
_log(f"Block groups complete: {len(dfs['block group'])} total rows")

# build data frames ################################################################################
_log("Building data frames...")
c_state = dfs["state"]
c_state = c_state.drop(columns="state")
c_state = c_state.rename(columns={"NAME": "state"})

c_county = dfs["county"]

c_zcta = dfs["zip code tabulation area"]

state_name = dfs["state"][["state", "NAME"]].rename(columns={"NAME": "state_NAME"})
c_county_state = c_county.merge(state_name, how="left", on="state")
c_county_state["GEOID"] = c_county_state["state"] + c_county_state["county"]

#
c_tract = dfs["tract"]

c_tract["GEOID"] = (
    c_tract["state"]
    + c_tract["county"]
    + c_tract["tract"].str.zfill(6)  # pad tract to 6 digits if needed
)

#
c_block_group = dfs["block group"]

c_block_group["GEOID"] = (
    c_block_group["state"]
    + c_block_group["county"]
    + c_block_group["tract"].str.zfill(6)
    + c_block_group["block group"]
)

# --- CLEAN UP ---
_log("Cleaning up and computing derived metrics...")
# Join ZCTA to DMA and create c_dma
c_zcta = c_zcta.rename(columns={"zip code tabulation area": "zcta"})

c_zcta_dma = c_zcta.merge(zcta_to_dma, how="left", on="zcta")
c_zcta_dma[metrics] = c_zcta_dma[metrics].astype(float)

c_dma = c_zcta_dma.groupby("dma", as_index=False, dropna=False).sum(numeric_only=True)


def _clean_census_label(concept, label):
    """Convert a Census API concept + label into a short readable column name.

    Handles any group downloaded via this pipeline — new groups added to `groups`
    will fall through to the generic fallback and still produce usable names.
    """
    if not isinstance(concept, str):
        concept = ""
    if not isinstance(label, str):
        label = ""

    def _scrub(text):
        text = re.sub(r"!!", " ", text)
        text = re.sub(r"[,:\$]", "", text)
        return re.sub(r"\s+", " ", text).strip()

    # Sex by Age — total population and all racial/ethnic subgroups (B01001, B01001A–I, etc.)
    m = re.match(r"Sex by Age\s*(?:\(([^)]*)\))?$", concept, re.IGNORECASE)
    if m:
        subgroup = (m.group(1) or "").replace(",", "").strip()
        demo = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["Pop", subgroup, demo]))

    # Household Income counts (B19001)
    if (
        re.search(r"Household Income.*Past 12 Months", concept, re.IGNORECASE)
        and "Median" not in concept
    ):
        breakdown = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["N Household Income", breakdown]))

    # Median Household Income total (B19013) — single overall estimate, no age breakdown
    if (
        re.match(
            r"Median Household Income in the Past 12 Months", concept, re.IGNORECASE
        )
        and "Age" not in concept
    ):
        return "Median Household Income"

    # Median Household Income by age of householder (B19049)
    if re.search(r"Median.*Household Income", concept, re.IGNORECASE):
        age = _scrub(label.rsplit("!!", 1)[-1])
        return " ".join(
            filter(
                None, ["Median Household Income", age if age.lower() != "total" else ""]
            )
        )

    # Households by Type (B11012)
    if re.match(r"Households by Type", concept, re.IGNORECASE):
        return "Households by Type - " + _scrub(label)

    # Educational Attainment (B15003)
    if re.search(r"Educational Attainment", concept, re.IGNORECASE):
        detail = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["Education", detail]))

    # Employment Status (B23025)
    if re.search(r"Employment Status", concept, re.IGNORECASE):
        detail = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["Employment", detail]))

    # Poverty Status (B17001)
    if re.search(r"Poverty Status", concept, re.IGNORECASE):
        detail = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["Poverty", detail]))

    # Tenure — owner vs renter occupied (B25003)
    if re.match(r"Tenure", concept, re.IGNORECASE):
        detail = _scrub(re.sub(r"^Estimate!!Total:!*", "", label))
        return " ".join(filter(None, ["Housing Tenure", detail]))

    # Median Home Value (B25077)
    if re.search(r"Median Value", concept, re.IGNORECASE):
        return "Median Home Value"

    # Median Gross Rent (B25064)
    if re.search(r"Median Gross Rent", concept, re.IGNORECASE):
        return "Median Gross Rent"

    # Generic fallback — strips year annotations and special chars
    clean = re.sub(r"\(in \d{4}[^)]*\)", "", concept + " - " + label)
    return _scrub(clean)


# Rename columns for clarity
concept_label_map = {
    row["variable"]: _clean_census_label(row["concept"], row["label"])
    for _, row in variables.iterrows()
}

#
decade_aggregations = [
    ("Under 10 years", ["Under 5 years", "5 to 9 years"]),
    ("10 to 19 years", ["10 to 14 years", "15 to 17 years", "18 and 19 years"]),
    ("20 to 29 years", ["20 years", "21 years", "22 to 24 years", "25 to 29 years"]),
    ("30 to 39 years", ["30 to 34 years", "35 to 39 years"]),
    ("40 to 49 years", ["40 to 44 years", "45 to 49 years"]),
    ("50 to 59 years", ["50 to 54 years", "55 to 59 years"]),
    (
        "60 to 69 years",
        ["60 and 61 years", "62 to 64 years", "65 and 66 years", "67 to 69 years"],
    ),
    ("70 to 79 years", ["70 to 74 years", "75 to 79 years"]),
    ("80 years and over", ["80 to 84 years", "85 years and over"]),
]

decade_labels = [d[0] for d in decade_aggregations]

# Racial/ethnic subgroup share of total population
racial_pct = {
    "pct_white_alone": "Pop White Alone",
    "pct_white_nh": "Pop White Alone Not Hispanic or Latino",
    "pct_black": "Pop Black or African American Alone",
    "pct_hispanic": "Pop Hispanic or Latino",
    "pct_asian": "Pop Asian Alone",
    "pct_aian": "Pop American Indian and Alaska Native Alone",
    "pct_nhpi": "Pop Native Hawaiian and Other Pacific Islander Alone",
    "pct_other_race": "Pop Some Other Race Alone",
    "pct_two_or_more": "Pop Two or More Races",
}

# Derived metrics computed using raw Census codes BEFORE column rename
pre_rename_derived = {
    "pct_bachelors_plus": {
        "num": ["B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"],
        "denom": "B15003_001E",
    },
    "pct_unemployed": {
        "num": ["B23025_005E"],
        "denom": "B23025_003E",
    },
    "pct_poverty": {
        "num": ["B17001_002E"],
        "denom": "B17001_001E",
    },
    "pct_owner_occupied": {
        "num": ["B25003_002E"],
        "denom": "B25003_001E",
    },
    "pct_renter_occupied": {
        "num": ["B25003_003E"],
        "denom": "B25003_001E",
    },
}

pop_df_names = ["state", "dma", "county", "zcta", "tract", "block_group"]
pop_dfs = [c_state, c_dma, c_county_state, c_zcta_dma, c_tract, c_block_group]
for name, df in zip(pop_df_names, pop_dfs):
    _log(f"  Computing derived metrics for {name} ({len(df)} rows)...")
    # Replace Census suppressed-value sentinel before any derived calculations
    available_metrics = [m for m in metrics if m in df.columns]
    df[available_metrics] = (
        df[available_metrics].astype(float).replace(-666666666.0, np.nan)
    )
    # Compute pct_ derived metrics using raw Census codes before rename
    for pct_col, spec in pre_rename_derived.items():
        num_cols = [c for c in spec["num"] if c in df.columns]
        denom_col = spec["denom"]
        if num_cols and denom_col in df.columns:
            df[pct_col] = df[num_cols].sum(axis=1) / df[denom_col]
    df.rename(columns=concept_label_map, inplace=True)

    df["pct_male"] = df["Pop Male"] / df["Pop"]
    df["Household Income 200+_ratio"] = (
        df["N Household Income 200000 or more"]
        / df["Households by Type - Estimate Total"]
    )
    for gender in ["Male", "Female"]:
        for decade, cols in decade_aggregations:
            colnames = [f"Pop {gender} {c}" for c in cols]
            new_col = f"Pop {gender} {decade}"
            df[new_col] = df[colnames].sum(axis=1)
    for decade in decade_labels:
        male_col = f"Pop Male {decade}"
        female_col = f"Pop Female {decade}"
        ratio_col = f"pct_male_{decade}"
        df[ratio_col] = df[male_col] / (df[male_col] + df[female_col])
    for pct_col, source_col in racial_pct.items():
        if source_col in df.columns:
            df[pct_col] = df[source_col] / df["Pop"]

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

# Drop all Median columns from c_dma — summing medians over ZCTAs is meaningless
median_cols_to_drop = [c for c in c_dma.columns if "Median" in c]
c_dma = c_dma.drop(columns=median_cols_to_drop)

# --- SAVE ---
_log("Saving CSVs...")
state_name.to_csv("state_name.csv", index=False)

for filename, df in [
    ("c_state.csv", c_state),
    ("c_dma.csv", c_dma),
    ("c_county_state.csv", c_county_state),
    ("c_zcta_dma.csv", c_zcta_dma),
    ("c_tract.csv", c_tract),
    ("c_block_group.csv", c_block_group),
]:
    df.to_csv(filename, index=False)
    _log(f"  Saved {filename} ({len(df)} rows, {len(df.columns)} columns)")

_log(f"Done! Total time: {time.time() - _start:.0f}s")
