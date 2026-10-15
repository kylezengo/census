"""Download Census 2000 long-form (SF3) data as a year-2000 point for the Trends tab.

Produces the same columns as download_timeseries.py, so the app can append these
rows to the ACS series (2009-2024). SF3 is the 1-in-6 sample that carried the
detailed questions before the ACS replaced it. Income and poverty refer to 1999;
everything else to 2000.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

from census_common import MAX_RETRIES, MAX_WORKERS, log as _log, require_api_key, skip_if_downloaded

SF3 = "https://api.census.gov/data/2000/dec/sf3"
YEAR = 2000

OUTPUTS = {
    "state": "c_census2000_state.csv",
    "county": "c_census2000_county.csv",
    "state_race": "c_census2000_state_race.csv",
    "county_race": "c_census2000_county_race.csv",
    "state_age": "c_census2000_state_age.csv",
    "county_age": "c_census2000_county_age.csv",
}

_start = time.time()
skip_if_downloaded(list(OUTPUTS.values()), "Census 2000 data")
census_api_key = require_api_key()

# Totals ########################################################################################
# Matched to the ACS tables used in download_timeseries.py (B01001, B19013, ...).
TOTAL_VARS = {
    "P001001": "Pop",
    "P008002": "_pop_male",
    "P006003": "_pop_black",  # Black alone (incl. Hispanic), like B01001B
    "P006005": "_pop_asian",
    "P007003": "_pop_white_nh",
    "P007010": "_pop_hispanic",
    "P053001": "Median Household Income",  # 1999 dollars
    "H085001": "Median Home Value",  # all owner-occupied units, like B25077
    "H063001": "Median Gross Rent",
    "P087001": "_poverty_total",
    "P087002": "_poverty_below",
    "P043005": "_lf_male",  # civilian labor force
    "P043012": "_lf_female",
    "P043007": "_unemp_male",
    "P043014": "_unemp_female",
    "P037001": "_educ_total",
    **{f"P037{i:03d}": f"_educ_{i}" for i in (15, 16, 17, 18, 32, 33, 34, 35)},
    "H007001": "_tenure_total",
    "H007002": "_tenure_owner",
    "H007003": "_tenure_renter",
}

# Race-iterated tables ##########################################################################
# SF3's suffix letters differ from the ACS: here H is Hispanic and I is White
# non-Hispanic (the ACS has them the other way round). Mapped by label, not letter.
RACES = {
    "A": "White alone",
    "B": "Black",
    "C": "American Indian / Alaska Native",
    "D": "Asian",
    "E": "Native Hawaiian / Pacific Islander",
    "F": "Some other race",
    "G": "Two or more races",
    "H": "Hispanic or Latino",
    "I": "White (non-Hispanic)",
}

RACE_VAR_TEMPLATES = {
    "Pop": ["P145{s}001"],
    "Median Household Income": ["P152{s}001"],
    "_poverty_total": ["P159{s}001"],
    "_poverty_below": ["P159{s}002"],
    "_educ_total": ["P148{s}001"],
    # Bachelor's + graduate/professional, male and female
    "_educ_bachelors_plus": ["P148{s}008", "P148{s}009", "P148{s}016", "P148{s}017"],
    "_labor_force": ["P150{s}005", "P150{s}012"],
    "_unemployed": ["P150{s}007", "P150{s}014"],
}

# Tenure by race isn't iterated: H011 lists the seven single races as lines under
# owner (3-9) and renter (11-17); Hispanic and White non-Hispanic are H012/H013.
TENURE_VARS = {
    s: (f"H011{3 + i:03d}", f"H011{11 + i:03d}") for i, s in enumerate("ABCDEFG")
}
TENURE_VARS["H"] = ("H012002", "H012003")
TENURE_VARS["I"] = ("H013002", "H013003")

# Age of householder ############################################################################
# P055 crosses seven householder ages with the same 16 income bins as the ACS's
# B19037; each block is a subtotal at line N followed by bins N+1..N+16. The ACS
# brackets are unions of these, so counts and shares sum exactly.
P055_BLOCKS = {
    "Under 25": [2],
    "25 to 44": [19, 36],
    "45 to 64": [53, 70],
    "65 and over": [87, 104],
}
# Lower bound of each income bin; the last is open-ended ($200k+).
BIN_LOWER = [0, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 100, 125, 150, 200]
BIN_UPPER = BIN_LOWER[1:] + [None]
_BINS_UNDER_25K = range(1, 5)
_BINS_100K_PLUS = range(13, 17)
_BIN_200K_PLUS = 16
# Published medians exist only for the 10-year brackets, so only "Under 25" can use
# one directly; the merged brackets are interpolated from the bins. Checked against
# P056 on the 10-year brackets: states within 0.6% typically (2.4% worst), counties
# 0.5-0.8% typically and 95% within 2.5-5.4%; the big misses are tiny counties.
P056_UNDER_25 = "P056002"


def _p055(line):
    return f"P055{line:03d}"


AGE_VARS = [_p055(n) for n in range(2, 121)] + [P056_UNDER_25]


def _get(var_list, for_clause):
    """One SF3 request. The API caps a request at 50 variables, so callers chunk."""
    params = {"get": ",".join(["NAME", *var_list]), "for": for_clause, "key": census_api_key}
    for attempt in range(MAX_RETRIES + 2):
        try:
            r = requests.get(SF3, params=params, timeout=120)
            # Under load the API answers 200 with an HTML "busy" page, so check the body.
            if r.status_code == 200 and r.text.startswith("["):
                data = r.json()
                return pd.DataFrame(data[1:], columns=data[0])
            msg = f"{r.status_code}: {r.text[:120]}"
        except requests.RequestException as e:
            msg = str(e)
        time.sleep(2 ** attempt)
    raise SystemExit(f"SF3 request failed for {for_clause} ({len(var_list)} vars): {msg}")


def _fetch(var_list, for_clause):
    """Fetch any number of variables, 45 per request, joined on NAME + geo columns."""
    chunks = [var_list[i : i + 45] for i in range(0, len(var_list), 45)]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(lambda c: _get(c, for_clause), chunks))
    keys = [c for c in dfs[0].columns if c not in var_list]
    out = dfs[0]
    for df in dfs[1:]:
        out = out.merge(df, on=keys)
    out[var_list] = out[var_list].apply(pd.to_numeric, errors="coerce")
    return out, keys


def _clean_medians(df, cols):
    # SF3 reports 0 where a median can't be computed (no households in the group).
    for c in cols:
        df[c] = df[c].where(df[c] > 0)


def _process_totals(raw):
    df = raw.rename(columns=TOTAL_VARS)
    _clean_medians(df, ["Median Household Income", "Median Home Value", "Median Gross Rent"])
    df["pct_male"] = df["_pop_male"] / df["Pop"]
    df["pct_white_nh"] = df["_pop_white_nh"] / df["Pop"]
    df["pct_black"] = df["_pop_black"] / df["Pop"]
    df["pct_hispanic"] = df["_pop_hispanic"] / df["Pop"]
    df["pct_asian"] = df["_pop_asian"] / df["Pop"]
    df["pct_poverty"] = df["_poverty_below"] / df["_poverty_total"]
    df["pct_unemployed"] = (df["_unemp_male"] + df["_unemp_female"]) / (
        df["_lf_male"] + df["_lf_female"]
    )
    bach = [f"_educ_{i}" for i in (15, 16, 17, 18, 32, 33, 34, 35)]
    df["pct_bachelors_plus"] = df[bach].sum(axis=1) / df["_educ_total"]
    df["pct_owner_occupied"] = df["_tenure_owner"] / df["_tenure_total"]
    df["pct_renter_occupied"] = df["_tenure_renter"] / df["_tenure_total"]
    df["year"] = YEAR
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.drop(columns=[c for c in df.columns if c.startswith("_")])


def _process_race(raw, keys):
    frames = []
    for s, race in RACES.items():
        out = raw[keys].copy()
        out["race"] = race
        for name, tmpls in RACE_VAR_TEMPLATES.items():
            out[name] = raw[[t.format(s=s) for t in tmpls]].sum(axis=1, min_count=1)
        owner, renter = TENURE_VARS[s]
        out["_tenure_owner"] = raw[owner]
        out["_tenure_renter"] = raw[renter]
        frames.append(out)
    df = pd.concat(frames, ignore_index=True)
    _clean_medians(df, ["Median Household Income"])
    tenure_total = df["_tenure_owner"] + df["_tenure_renter"]
    df["pct_poverty"] = df["_poverty_below"] / df["_poverty_total"]
    df["pct_bachelors_plus"] = df["_educ_bachelors_plus"] / df["_educ_total"]
    df["pct_unemployed"] = df["_unemployed"] / df["_labor_force"]
    df["pct_owner_occupied"] = df["_tenure_owner"] / tenure_total
    df["pct_renter_occupied"] = df["_tenure_renter"] / tenure_total
    df["year"] = YEAR
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.drop(columns=[c for c in df.columns if c.startswith("_")])


def _interp_median(bins):
    """Median from 16 binned counts (thousands of $), by linear interpolation within
    the bin holding the midpoint. A median in the open top bin is reported at $200k."""
    total = bins.sum(axis=1)
    cum = bins.cumsum(axis=1)
    half = total / 2
    out = np.full(len(bins), np.nan)
    for i in range(len(bins)):
        if not total.iloc[i] > 0:
            continue
        k = int(np.argmax(cum.iloc[i].to_numpy() >= half.iloc[i]))
        lo, hi = BIN_LOWER[k], BIN_UPPER[k]
        if hi is None:
            out[i] = lo * 1000
            continue
        below = cum.iloc[i, k - 1] if k else 0
        out[i] = (lo + (half.iloc[i] - below) / bins.iloc[i, k] * (hi - lo)) * 1000
    return out


def _process_age(raw, keys):
    frames = []
    for label, starts in P055_BLOCKS.items():
        # 16 bins summed across the 10-year blocks that make up this bracket
        bins = pd.DataFrame(
            {
                o: sum(raw[_p055(st + o)] for st in starts)
                for o in range(1, 17)
            }
        )
        total = bins.sum(axis=1)
        part = raw[keys].copy()
        part["age"] = label
        if label == "Under 25":
            med = raw[P056_UNDER_25].where(raw[P056_UNDER_25] > 0)
        else:
            med = _interp_median(bins)
        part["Median Household Income"] = med
        part["pct_income_under_25k"] = bins[list(_BINS_UNDER_25K)].sum(axis=1) / total
        part["pct_income_100k_plus"] = bins[list(_BINS_100K_PLUS)].sum(axis=1) / total
        part["pct_income_200k_plus"] = bins[_BIN_200K_PLUS] / total
        part["Households"] = total
        frames.append(part)
    df = pd.concat(frames, ignore_index=True)
    df["year"] = YEAR
    return df.replace([np.inf, -np.inf], np.nan)


def _download(for_clause, label):
    _log(f"Fetching {label}...")
    totals, keys = _fetch(list(TOTAL_VARS), for_clause)
    race_vars = sorted(
        {t.format(s=s) for s in RACES for tmpls in RACE_VAR_TEMPLATES.values() for t in tmpls}
        | {v for pair in TENURE_VARS.values() for v in pair}
    )
    race_raw, _ = _fetch(race_vars, for_clause)
    age_raw, _ = _fetch(AGE_VARS, for_clause)
    _log(f"  {label}: {len(totals)} geographies")
    return (
        _process_totals(totals),
        _process_race(race_raw, keys),
        _process_age(age_raw, keys),
    )


def _state_frame(df):
    return df.drop(columns=["state", "us"], errors="ignore").rename(columns={"NAME": "state"})


def _county_frame(df):
    df = df.assign(GEOID=df["state"] + df["county"])
    return df.drop(columns=["state", "county"])


st_tot, st_race, st_age = _download("state:*", "states")
us_tot, us_race, us_age = _download("us:*", "US")
co_tot, co_race, co_age = _download("county:*", "counties")

pd.concat([_state_frame(st_tot), _state_frame(us_tot)]).sort_values("state").to_csv(
    OUTPUTS["state"], index=False
)
pd.concat([_state_frame(st_race), _state_frame(us_race)]).sort_values(
    ["state", "race"]
).to_csv(OUTPUTS["state_race"], index=False)
pd.concat([_state_frame(st_age), _state_frame(us_age)]).sort_values(
    ["state", "age"]
).to_csv(OUTPUTS["state_age"], index=False)
_county_frame(co_tot).sort_values("NAME").to_csv(OUTPUTS["county"], index=False)
# The ACS county race/age files carry NAME only, so match that.
co_race.drop(columns=["state", "county"]).sort_values(["NAME", "race"]).to_csv(
    OUTPUTS["county_race"], index=False
)
co_age.drop(columns=["state", "county"]).sort_values(["NAME", "age"]).to_csv(
    OUTPUTS["county_age"], index=False
)
for f in OUTPUTS.values():
    _log(f"Saved {f}")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
