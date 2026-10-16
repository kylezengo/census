"""Download ACS 5-year metrics broken out by age, for the Age Profile tab.

One long file: geography x metric x sex x age group -> value. Long rather than
wide because every metric has its own age brackets, and the app plots age on the
x-axis. Covers the US, states, counties and congressional districts; ZCTAs are
left out because age-specific rates there rest on very small samples.

Age groups come straight from each table, merged where a table mixes widths
(e.g. B01001's "20 years", "21 years", "22 to 24 years" become 20-24) so the
x-axis steps evenly. Merging is driven by the numeric ranges, and refuses any
source group that would straddle two merged groups.
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

from census_common import MAX_RETRIES, MAX_WORKERS, log as _log, require_api_key, skip_if_downloaded

YEAR = 2024
ACS = f"https://api.census.gov/data/{YEAR}/acs/acs5"
OUTPUT = f"c_age_profile_{YEAR}.csv"

_start = time.time()
skip_if_downloaded([OUTPUT], "Age profile data")
census_api_key = require_api_key()

GEOS = {
    "State": [("us:*", None), ("state:*", None)],
    "County": [("county:*", None)],
    "Congressional District": [("congressional district:*", "state:*")],
}

# Merged age groups per table: (low, high) inclusive, high None = open-ended.
_FIVES = [(a, a + 4) for a in range(0, 85, 5)] + [(85, None)]
BINS = {
    "B01001": _FIVES,
    "B17001": [(0, 4), (5, 11), (12, 17), (18, 24), (25, 34), (35, 44),
               (45, 54), (55, 64), (65, 74), (75, None)],
    "B23001": [(16, 19), (20, 24), (25, 29), (30, 34), (35, 44), (45, 54),
               (55, 59), (60, 64), (65, 69), (70, 74), (75, None)],
    "B15001": [(18, 24), (25, 34), (35, 44), (45, 64), (65, None)],
    "B25007": [(15, 24), (25, 34), (35, 44), (45, 54), (55, 59), (60, 64),
               (65, 74), (75, 84), (85, None)],
    "B19049": [(0, 24), (25, 44), (45, 64), (65, None)],
}


def _age_range(text):
    """'Under 5 years' -> (0, 4); '20 and 21 years' -> (20, 21); '85 years and
    over' -> (85, None); 'Householder 15 to 24 years' -> (15, 24)."""
    text = text.replace("Householder ", "").replace("householder ", "")
    if m := re.fullmatch(r"[Uu]nder (\d+) years", text):
        return 0, int(m.group(1)) - 1
    if m := re.fullmatch(r"(\d+) years and over", text):
        return int(m.group(1)), None
    if m := re.fullmatch(r"(\d+) (?:to|and) (\d+) years", text):
        return int(m.group(1)), int(m.group(2))
    if m := re.fullmatch(r"(\d+) years", text):
        return int(m.group(1)), int(m.group(1))
    return None


def _bin_of(table, rng):
    lo, hi = rng
    for b_lo, b_hi in BINS[table]:
        if lo >= b_lo and (b_hi is None or (hi is not None and hi <= b_hi)):
            return b_lo, b_hi
    raise ValueError(f"{table}: age group {rng} straddles the merged groups")


def _bin_label(rng):
    lo, hi = rng
    if hi is None:
        return f"{lo}+"
    if lo == 0:
        return f"Under {hi + 1}"
    return f"{lo}–{hi}"


def _labels(table):
    url = f"{ACS}/groups/{table}.json"
    v = requests.get(url, timeout=60).json()["variables"]
    out = {}
    for code, meta in v.items():
        if code.startswith(table + "_") and code.endswith("E"):
            # "Estimate!!Total:!!Male:!!20 years" -> ["Male", "20 years"]
            parts = [p.rstrip(":") for p in meta["label"].split("!!")]
            out[code] = parts[parts.index("Total") + 1 :] if "Total" in parts else parts[1:]
    return out


def _get(table, for_clause, in_clause):
    params = {"get": f"NAME,group({table})", "for": for_clause, "key": census_api_key}
    if in_clause:
        params["in"] = in_clause
    for attempt in range(MAX_RETRIES + 2):
        try:
            r = requests.get(ACS, params=params, timeout=120)
            if r.status_code == 200 and r.text.startswith("["):
                data = r.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                df = df.loc[:, ~df.columns.duplicated()]
                return df
            msg = f"{r.status_code}: {r.text[:120]}"
        except requests.RequestException as e:
            msg = str(e)
        time.sleep(2 ** attempt)
    raise SystemExit(f"{table} {for_clause} failed: {msg}")


def _long_counts(table, df, labels):
    """Melt one table to rows of (NAME, sex, age bin, leaf path, count)."""
    rows = []
    for code, parts in labels.items():
        sex = next((p for p in parts if p in ("Male", "Female")), None)
        age = next((r for p in parts if (r := _age_range(p))), None)
        if age is None:
            continue
        rows.append((code, sex, _bin_of(table, age), tuple(parts)))
    meta = pd.DataFrame(rows, columns=["code", "sex", "bin", "path"])
    vals = df[["NAME"] + list(meta["code"])].melt(id_vars="NAME", var_name="code")
    vals["value"] = pd.to_numeric(vals["value"], errors="coerce")
    # ACS sentinels (-666666666 etc.) mean "not computable"
    vals.loc[vals["value"] < 0, "value"] = np.nan
    return vals.merge(meta, on="code")


def _rate(counts, num_mask, den_mask, by_sex=True):
    """Sum numerator and denominator rows per NAME x bin (x sex), divide."""
    keys = ["NAME", "bin"] + (["sex"] if by_sex else [])
    num = counts[num_mask].groupby(keys)["value"].sum(min_count=1)
    den = counts[den_mask].groupby(keys)["value"].sum(min_count=1)
    out = (num / den).rename("value").reset_index()
    if not by_sex:
        out["sex"] = "Both"
    return out


def _with_both(counts, num_mask, den_mask):
    """Rate by sex plus the both-sexes rate (summing male and female counts)."""
    by_sex = _rate(counts, num_mask, den_mask)
    both = _rate(counts, num_mask, den_mask, by_sex=False)
    return pd.concat([by_sex, both], ignore_index=True)


def _leaf(counts, *names):
    return counts["path"].map(lambda p: p[-1] in names)


def _has(counts, name):
    return counts["path"].map(lambda p: name in p)


def _metrics(raw, labels):
    """All Age Profile metrics for one geography level, long format."""
    out = []

    pop = _long_counts("B01001", raw["B01001"], labels["B01001"])
    leaf = pop["path"].map(len) == 2  # [sex, age]
    pop = pop[leaf]
    by = pop.groupby(["NAME", "bin", "sex"])["value"].sum(min_count=1).reset_index()
    both = by.groupby(["NAME", "bin"])["value"].sum(min_count=1).reset_index().assign(sex="Both")
    allpop = pd.concat([by, both], ignore_index=True)
    totals = allpop.groupby(["NAME", "sex"])["value"].transform("sum")
    out.append(allpop.assign(value=allpop["value"] / totals, metric="pct_of_population"))
    male = by[by["sex"] == "Male"].set_index(["NAME", "bin"])["value"]
    pct_male = (male / both.set_index(["NAME", "bin"])["value"]).rename("value").reset_index()
    out.append(pct_male.assign(sex="Both", metric="pct_male"))

    pov = _long_counts("B17001", raw["B17001"], labels["B17001"])
    below = _has(pov, "Income in the past 12 months below poverty level")
    above = _has(pov, "Income in the past 12 months at or above poverty level")
    out.append(_with_both(pov, below, below | above).assign(metric="pct_poverty"))

    emp = _long_counts("B23001", raw["B23001"], labels["B23001"])
    total = emp["path"].map(len) == 2  # [sex, age] subtotal
    in_lf = _leaf(emp, "In labor force")
    # Civilian employed/unemployed; at 65+ the table has no Civilian layer.
    employed, unemployed = _leaf(emp, "Employed"), _leaf(emp, "Unemployed")
    out.append(_with_both(emp, in_lf, total).assign(metric="pct_in_labor_force"))
    out.append(_with_both(emp, unemployed, employed | unemployed).assign(metric="pct_unemployed"))

    edu = _long_counts("B15001", raw["B15001"], labels["B15001"])
    total = edu["path"].map(len) == 2
    degree = _leaf(edu, "Bachelor's degree", "Graduate or professional degree")
    out.append(_with_both(edu, degree, total).assign(metric="pct_bachelors_plus"))

    ten = _long_counts("B25007", raw["B25007"], labels["B25007"])
    owner = _has(ten, "Owner occupied")
    out.append(_rate(ten, owner, owner | _has(ten, "Renter occupied"), by_sex=False)
               .assign(metric="pct_owner_occupied"))

    inc = _long_counts("B19049", raw["B19049"], labels["B19049"])
    out.append(inc[["NAME", "bin", "value"]].assign(sex="Both", metric="Median Household Income"))

    df = pd.concat(out, ignore_index=True).replace([np.inf, -np.inf], np.nan)
    df["age"] = df["bin"].map(_bin_label)
    df["age_order"] = df["bin"].map(lambda b: b[0])
    return df.drop(columns="bin").dropna(subset=["value"])


TABLES = list(BINS)
_log(f"Fetching labels for {len(TABLES)} tables...")
labels = {t: _labels(t) for t in TABLES}

frames = []
for level, clauses in GEOS.items():
    _log(f"Fetching {level}...")
    jobs = [(t, f, i) for t in TABLES for f, i in clauses]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = list(ex.map(lambda j: (j[0], _get(*j)), jobs))
    raw = {}
    for t, df in results:
        raw[t] = pd.concat([raw[t], df], ignore_index=True) if t in raw else df
    m = _metrics(raw, labels)
    m["level"] = level
    frames.append(m)
    _log(f"  {level}: {m['NAME'].nunique()} geographies, {len(m):,} rows")

out = pd.concat(frames, ignore_index=True)
out = out[["level", "NAME", "metric", "sex", "age", "age_order", "value"]]
out.sort_values(["level", "NAME", "metric", "sex", "age_order"]).to_csv(OUTPUT, index=False)
_log(f"Saved {OUTPUT} ({len(out):,} rows). Done in {time.time() - _start:.0f}s")
