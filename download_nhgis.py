"""Download pre-2000 decennial census data from IPUMS NHGIS for the Trends tab.

Mostly NHGIS time series tables — the ones NHGIS has harmonized so a measure means
the same thing across census years. Coverage therefore varies by metric:
population from 1790, sex from 1820, most others from 1970 or 1980, and nothing
for home value or rent (no time series exists). Income by race and by householder
age have no time series, so those come from the 1980 and 1990 source tables
instead (see SOURCE_DATASETS). The output columns match download_timeseries.py
so the app can prepend these rows.

Requires a free IPUMS account registered for NHGIS, with its API key in .env as
`ipums_api_key`. Data use must cite NHGIS (see the README).
"""

import io
import os
import re
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
    "state_age": "c_nhgis_state_age.csv",
    "county_age": "c_nhgis_county_age.csv",
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


def _request_timeseries():
    levels = ["nation", "state", "county"]
    return _submit({
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
    })


def _submit(body):
    """Submit an NHGIS extract, wait for it, and return the table-data URL."""
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


def _download_zip(url):
    return zipfile.ZipFile(io.BytesIO(requests.get(url, headers=HEADERS, timeout=600).content))


def _read_tables(url):
    frames = {}
    with _download_zip(url) as z:
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


# Source tables (not harmonized) ################################################
# NHGIS has no time series for income by race or by householder age, so these use
# each census's own tables. Race medians are the Census Bureau's published ones,
# except the 1980 US row (that file has no nation level), which is interpolated
# from the summed state distributions — the same method reproduces every published
# 1980 state and county median exactly. Asian is 1990 only (1980 merges it with
# Pacific Islander) and White non-Hispanic is 1990 only (1980 didn't publish it).
SOURCE_DATASETS = {
    "1980_STF4Pb": {
        "dataTables": ["NTPB71A", "NTPB72A"],  # household income in 1979; median
        "geogLevels": ["state", "county"],
        # total area; White, Black, American Indian/Eskimo/Aleut, Spanish origin
        "breakdownValues": ["bs03.ge0000", "bs04.ch01", "bs04.ch02", "bs04.ch03", "bs04.ch19"],
    },
    "1990_STF4b": {
        "dataTables": ["NPB65A"],  # median household income in 1989
        "geogLevels": ["nation", "state", "county"],
        "breakdownValues": [
            "bs09.ge00",  # total area
            "bs10.ch001",  # White
            "bs10.ch002",  # Black
            "bs10.ch003",  # American Indian, Eskimo, or Aleut
            "bs10.ch008",  # Asian (excl. Pacific Islander, like the ACS)
            "bs10.ch100",  # Hispanic origin (of any race)
            "bs10.ch120",  # White, not of Hispanic origin
        ],
    },
    "1990_STF3": {"dataTables": ["NP86"], "geogLevels": ["nation", "state", "county"]},
}

# Codebook race labels (trimmed of "(subtotal ...)" notes) -> app group names.
RACE_LABELS = {
    "White": "White alone",
    "Black": "Black",
    "American Indian, Eskimo, and Aleut": "American Indian / Alaska Native",
    "American Indian, Eskimo, or Aleut": "American Indian / Alaska Native",
    "Asian": "Asian",
    "Spanish origin": "Hispanic or Latino",
    "Hispanic origin": "Hispanic or Latino",
    "White, not of Hispanic origin": "White (non-Hispanic)",
}

# 1980 household income bins (NTPB71A, 17) and 1990's by age (NP86, 9 per age).
BINS_1980 = [0, 2.5, 5, 7.5, 10, 12.5, 15, 17.5, 20, 22.5, 25, 27.5, 30, 35, 40, 50, 75]
BINS_1990 = [0, 5, 10, 15, 25, 35, 50, 75, 100]
# NP86's seven householder ages -> the app's four brackets (indexes into NP86 blocks)
AGE_BLOCKS_1990 = {"Under 25": [0], "25 to 44": [1, 2], "45 to 64": [3, 4], "65 and over": [5, 6]}


def _interp_median(bins, lowers):
    """Median from binned counts by linear interpolation within the bin holding the
    midpoint (lowers in $1000s; last bin open-ended, reported at its floor)."""
    b = np.asarray(bins, dtype=float)
    uppers = lowers[1:] + [None]
    out = np.full(len(b), np.nan)
    for i, row in enumerate(b):
        total = np.nansum(row)
        if not total > 0:
            continue
        cum = np.nancumsum(row)
        k = int(np.argmax(cum >= total / 2))
        if uppers[k] is None:
            out[i] = lowers[k] * 1000
            continue
        below = cum[k - 1] if k else 0
        out[i] = (lowers[k] + (total / 2 - below) / row[k] * (uppers[k] - lowers[k])) * 1000
    return out


def _table_code(dataset, table):
    url = f"https://api.ipums.org/metadata/datasets/{dataset}/data_tables/{table}"
    return requests.get(url, params=PARAMS, headers=HEADERS, timeout=60).json()["nhgisCode"]


def _breakdown_races(codebook):
    """Column suffix ('' for the first breakdown) -> app race group, read from the
    codebook rather than assumed: suffix letters aren't stable across files."""
    out = {}
    for m in re.finditer(r"Breakdown \d+(?: \((\w+)\))?:\s*\n(.*?)(?:\n\s*\n|\Z)", codebook, re.S):
        race = re.search(r"Race/Ethnicity: (.+?) \(\w+\)\s*$", m.group(2), re.M)
        if race:
            # e.g. "All persons by race: Asian or Pacific Islander: Asian (008)" or
            # "Spanish origin subtotal (subtotal of summaries 20-23) (19)"
            label = race.group(1).split(" (")[0].split(": ")[-1]
            label = label.replace(" subtotal", "").strip()
            out[m.group(1) or ""] = RACE_LABELS[label]
    return out


def _read_sources(url):
    """{(year, level): [(frame, codebook text)]} for the source-table extract."""
    out = {}
    with _download_zip(url) as z:
        for name in z.namelist():
            m = re.search(r"_(\d{4})_(nation|state|county)\.csv$", name)
            if not m:
                continue
            df = pd.read_csv(z.open(name), skiprows=[1], encoding="latin-1", dtype=str)
            codebook = z.read(name.replace(".csv", "_codebook.txt")).decode("latin-1")
            out.setdefault((int(m.group(1)), m.group(2)), []).append((df, codebook))
    return out


def _geo_keys(df, level, county_names):
    """Geography columns matching the time-series output for this level."""
    if level == "nation":
        return pd.DataFrame({"state": US_LABEL}, index=df.index)
    if level == "state":
        return pd.DataFrame({"state": df["STATE"]}, index=df.index)
    geoid = df["STATEA"].str.zfill(2) + df["COUNTYA"].str.zfill(3)
    return pd.DataFrame({"NAME": geoid.map(county_names)}, index=df.index)


def _race_income(sources, year, level, code, county_names):
    """Long frame of published medians by race for one census year and level."""
    frames = []
    for df, codebook in sources.get((year, level), []):
        races = _breakdown_races(codebook)
        for sfx, race in races.items():
            col = f"{code}{sfx}001"
            if col not in df.columns:
                continue
            part = _geo_keys(df, level, county_names)
            part["race"] = race
            med = pd.to_numeric(df[col], errors="coerce")
            part["Median Household Income"] = med.where(med > 0)
            frames.append(part)
    return frames


def _race_income_us_1980(sources, dist_code):
    """1980 has no nation level: sum the state distributions, then interpolate."""
    df, codebook = sources[(1980, "state")][0]
    df = df[df["STATE"] != "Puerto Rico"]
    rows = []
    for sfx, race in _breakdown_races(codebook).items():
        cols = [f"{dist_code}{sfx}{k + 1:03d}" for k in range(len(BINS_1980))]
        bins = df[cols].apply(pd.to_numeric, errors="coerce").sum().to_numpy()[None, :]
        rows.append({"state": US_LABEL, "race": race,
                     "Median Household Income": _interp_median(bins, BINS_1980)[0]})
    return pd.DataFrame(rows)


def _age_1990(sources, level, code, county_names):
    df, _ = next(
        (d, c) for d, c in sources[(1990, level)] if f"{code}001" in d.columns
    )
    num = df[[c for c in df.columns if c.startswith(code)]].apply(pd.to_numeric, errors="coerce")
    nbins = len(BINS_1990)
    frames = []
    for label, blocks in AGE_BLOCKS_1990.items():
        bins = sum(
            num[[f"{code}{b * nbins + k + 1:03d}" for k in range(nbins)]].to_numpy()
            for b in blocks
        )
        total = bins.sum(axis=1)
        part = _geo_keys(df, level, county_names)
        part["age"] = label
        # Interpolated: 1990 published medians only for 10-year ages, and these
        # 9 bins are coarse — ~1% off typically (checked against published totals).
        part["Median Household Income"] = _interp_median(bins, BINS_1990)
        # Zero-household brackets in tiny counties give 0/0 -> NaN, intended.
        with np.errstate(invalid="ignore", divide="ignore"):
            part["pct_income_under_25k"] = bins[:, :4].sum(axis=1) / total
            part["pct_income_100k_plus"] = bins[:, 8] / total
        part["pct_income_200k_plus"] = np.nan  # 1990's top bin is $100k+
        part["Households"] = total
        part["year"] = 1990
        frames.append(part)
    return pd.concat(frames, ignore_index=True).replace([np.inf, -np.inf], np.nan)


_log(f"Requesting {len(TABLES)} NHGIS time series tables (nation, state, county)...")
frames = _read_tables(_request_timeseries())
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

_log("Requesting 1980/1990 source tables (income by race and by householder age)...")
sources = _read_sources(_submit({
    "datasets": SOURCE_DATASETS,
    "breakdownAndDataTypeLayout": "single_file",
    "dataFormat": "csv_header",
    "description": "census explorer: 1980/1990 income by race and age",
}))
med80, dist80 = _table_code("1980_STF4Pb", "NTPB72A"), _table_code("1980_STF4Pb", "NTPB71A")
med90, age90 = _table_code("1990_STF4b", "NPB65A"), _table_code("1990_STF3", "NP86")


def _county_names_in(year):
    """GEOID -> county name as the time-series rows have it for that census year."""
    names = county[county["year"] == year].set_index("GEOID")["NAME_OUT"]
    return names[names.index.notna()]


inc = {"state": [], "county": []}
for year, code in [(1980, med80), (1990, med90)]:
    names = _county_names_in(year)
    levels = ["state", "county"] if year == 1980 else ["nation", "state", "county"]
    for level in levels:
        key = "county" if level == "county" else "state"
        inc[key] += [f.assign(year=year) for f in _race_income(sources, year, level, code, names)]
inc["state"].append(_race_income_us_1980(sources, dist80).assign(year=1980))
names90 = _county_names_in(1990)
st_race = st_race.merge(
    pd.concat(inc["state"]), on=["state", "year", "race"], how="outer"
)
co_race = co_race.merge(
    pd.concat(inc["county"]).dropna(subset=["NAME"]), on=["NAME", "year", "race"], how="outer"
)
st_age = pd.concat(
    [_age_1990(sources, lvl, age90, names90) for lvl in ("nation", "state")], ignore_index=True
)
co_age = _age_1990(sources, "county", age90, names90).dropna(subset=["NAME"])

st_tot.sort_values(["state", "year"]).to_csv(OUTPUTS["state"], index=False)
co_tot.sort_values(["NAME", "year"]).to_csv(OUTPUTS["county"], index=False)
st_race.sort_values(["state", "race", "year"]).to_csv(OUTPUTS["state_race"], index=False)
co_race.sort_values(["NAME", "race", "year"]).to_csv(OUTPUTS["county_race"], index=False)
st_age.sort_values(["state", "age"]).to_csv(OUTPUTS["state_age"], index=False)
co_age.sort_values(["NAME", "age"]).to_csv(OUTPUTS["county_age"], index=False)
for f in OUTPUTS.values():
    _log(f"Saved {f}")
_log(f"Done! Total time: {time.time() - _start:.0f}s")
