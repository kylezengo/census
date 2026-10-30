"""Download housing-shortage indicators for the Housing tab.

c_housing_state.csv — US + states, ACS 5-year 2009-2024: vacancy, housing units
    per household, young-adult headship, renter cost burden, price-to-income.
c_housing_metro.csv — metropolitan areas, ACS 2020-24, the same indicators plus
    the Wharton Residential Land Use Regulatory Index (WRLURI 2018).
c_housing_payment.csv — US + states, ACS 1-year 2005-2025: monthly mortgage payment
    on the median home at that year's 30-year fixed rate (Freddie Mac PMMS via FRED),
    as a share of median household income, plus the median home value in
    latest-year dollars (CPI-U via FRED). 1-year because 5-year windows blend
    years with very different rates. No 2020 1-year release.

WRLURI is a survey of ~2,800 local governments (Gyourko, Hartley & Krimmel 2021,
NBER w26573). Jurisdictions are mapped to 2023 metros through their county and
averaged with the survey's within-metro weights; metros with fewer than
MIN_JURIS respondents get no index. Higher = more restrictive.
"""

import io
import time

import numpy as np
import pandas as pd
import requests

from census_common import MAX_RETRIES, log as _log, require_api_key, skip_if_downloaded

YEARS = list(range(2009, 2025))
METRO_YEAR = 2024
PAYMENT_YEARS = [y for y in range(2005, 2026) if y != 2020]
OUTPUTS = ["c_housing_state.csv", "c_housing_metro.csv", "c_housing_payment.csv"]
MIN_JURIS = 5
# Payment assumptions: principal + interest only, 20% down, 30-year fixed. Taxes
# and insurance vary by place, not by rate, so they're left out.
LTV = 0.8
TERM_MONTHS = 360
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"

CBSA_URL = (
    "https://www2.census.gov/programs-surveys/metro-micro/geographies/"
    "reference-files/2023/delineation-files/list1_2023.xlsx"
)
WRLURI_URL = (
    "https://www.dropbox.com/scl/fi/ekxan963skmr436rov9j4/"
    "WHARTON-LAND-REGULATION-DATA_01_15_2020.dta?rlkey=aqdccswzltbp7w0fxzayh33nz&dl=1"
)
METRO_GEO = "metropolitan statistical area/micropolitan statistical area"

_start = time.time()
skip_if_downloaded(OUTPUTS, "Housing data")
census_api_key = require_api_key()

VARS = {
    "B01003_001E": "Pop",
    "B25001_001E": "_units",
    "B25002_003E": "_vacant",
    "B25003_001E": "_households",
    "B25003_003E": "_renter_hh",
    "B25004_002E": "_vac_for_rent",
    "B25004_003E": "_vac_rented_unocc",
    "B25070_001E": "_rb_total",
    "B25070_007E": "_rb_30_35",
    "B25070_008E": "_rb_35_40",
    "B25070_009E": "_rb_40_50",
    "B25070_010E": "_rb_50_plus",
    "B25070_011E": "_rb_not_computed",
    "B25007_004E": "_own_25_34",
    "B25007_014E": "_rent_25_34",
    "B01001_011E": "_m_25_29",
    "B01001_012E": "_m_30_34",
    "B01001_035E": "_f_25_29",
    "B01001_036E": "_f_30_34",
    "B25077_001E": "Median Home Value",
    "B19013_001E": "Median Household Income",
}


def _fetch(year, for_clause, dataset="acs5", variables=VARS):
    url = f"https://api.census.gov/data/{year}/acs/{dataset}"
    params = {"get": "NAME," + ",".join(variables), "for": for_clause, "key": census_api_key}
    for attempt in range(MAX_RETRIES + 2):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 200 and r.text.startswith("["):
                data = r.json()
                df = pd.DataFrame(data[1:], columns=data[0]).rename(columns=variables)
                for c in variables.values():
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                    # ACS sentinels (-666666666 etc.) mean "not computable"
                    df.loc[df[c] < 0, c] = np.nan
                df["year"] = year
                return df
            msg = f"{r.status_code}: {r.text[:120]}"
        except requests.RequestException as e:
            msg = str(e)
        time.sleep(2 ** attempt)
    raise SystemExit(f"{year} {for_clause} failed: {msg}")


def _derive(df):
    pop_25_34 = df[["_m_25_29", "_m_30_34", "_f_25_29", "_f_30_34"]].sum(axis=1)
    rented_stock = df["_vac_for_rent"] + df["_vac_rented_unocc"] + df["_renter_hh"]
    computed = df["_rb_total"] - df["_rb_not_computed"]
    burdened = df[["_rb_30_35", "_rb_35_40", "_rb_40_50", "_rb_50_plus"]].sum(axis=1)
    df = df.assign(
        units_per_household=df["_units"] / df["_households"],
        pct_vacant=df["_vacant"] / df["_units"],
        pct_rental_vacancy=df["_vac_for_rent"] / rented_stock,
        pct_headship_25_34=(df["_own_25_34"] + df["_rent_25_34"]) / pop_25_34,
        pct_owner_25_34=df["_own_25_34"] / pop_25_34,
        pct_rent_burdened=burdened / computed,
        pct_rent_severely_burdened=df["_rb_50_plus"] / computed,
        price_to_income=df["Median Home Value"] / df["Median Household Income"],
    )
    return df.drop(columns=[c for c in df.columns if c.startswith("_")])


# State + US time series ######################################################
frames = []
for year in YEARS:
    us = _fetch(year, "us:*").drop(columns=["us"])
    st = _fetch(year, "state:*").drop(columns=["state"])
    frames += [us, st]
    _log(f"{year}: US + {len(st)} states")
state = _derive(pd.concat(frames, ignore_index=True)).rename(columns={"NAME": "state"})
state.sort_values(["state", "year"]).to_csv("c_housing_state.csv", index=False)
_log(f"Wrote c_housing_state.csv ({len(state)} rows)")


# Metros + regulation index ###################################################
xw = pd.read_excel(io.BytesIO(requests.get(CBSA_URL, timeout=120).content), skiprows=2, dtype=str)
xw = xw.dropna(subset=["FIPS State Code"])
xw = pd.DataFrame({
    "GEOID": xw["FIPS State Code"].str.zfill(2) + xw["FIPS County Code"].str.zfill(3),
    "cbsa": xw["CBSA Code"],
    "metro": xw["Metropolitan/Micropolitan Statistical Area"] == "Metropolitan Statistical Area",
})
metro_codes = set(xw.loc[xw["metro"], "cbsa"])

w = pd.read_stata(io.BytesIO(requests.get(WRLURI_URL, timeout=300).content))
w = w.dropna(subset=["WRLURI18"]).copy()
w["GEOID"] = (
    w["statecode"].astype(int).astype(str).str.zfill(2)
    + w["countycode18"].fillna(-1).astype(int).astype(str).str.zfill(3)
)
w = w.merge(xw[xw["metro"]][["GEOID", "cbsa"]], on="GEOID", how="left")
# Connecticut's counties became planning regions in 2022, so its 2018 county
# codes miss the 2023 delineation; fall back to the survey's own CBSA code
# where that code still exists.
code18 = w["cbsacode18"].dropna().astype(int).astype(str)
fallback = code18[code18.isin(metro_codes)]
w["cbsa"] = w["cbsa"].fillna(fallback)
w = w.dropna(subset=["cbsa"])
# weight_cbsa is missing for about half of jurisdictions; use the metro weight there
w["_wt"] = w["weight_cbsa"].fillna(w["weight_metro"]).fillna(1)
w["_wx"] = w["WRLURI18"] * w["_wt"]
reg = w.groupby("cbsa").agg(_wx=("_wx", "sum"), _wt=("_wt", "sum"), wrluri_n=("WRLURI18", "size"))
reg["wrluri"] = reg["_wx"] / reg["_wt"]
reg = reg.loc[reg["wrluri_n"] >= MIN_JURIS, ["wrluri", "wrluri_n"]]

metro = _fetch(METRO_YEAR, f"{METRO_GEO}:*").rename(columns={METRO_GEO: "cbsa"})
metro = _derive(metro[metro["cbsa"].isin(metro_codes)])
metro = metro.merge(reg, left_on="cbsa", right_index=True, how="left")
metro.sort_values("Pop", ascending=False).to_csv("c_housing_metro.csv", index=False)
_log(
    f"Wrote c_housing_metro.csv ({len(metro)} metros, {metro['wrluri'].notna().sum()} with "
    f"WRLURI) in {time.time() - _start:.0f}s"
)


# Mortgage payment vs rates ###################################################
def _fred_annual(series):
    df = pd.read_csv(io.StringIO(requests.get(FRED_URL.format(series), timeout=60).text))
    df.columns = ["date", "value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.groupby(pd.to_datetime(df["date"]).dt.year)["value"].mean()


rate_by_year = _fred_annual("MORTGAGE30US")
# CPI-U here rather than config.CPI, which doesn't reach back to 2005 or out to 2025
cpi = _fred_annual("CPIAUCSL")

pay_vars = {k: v for k, v in VARS.items() if v in ("Median Home Value", "Median Household Income")}
frames = []
for year in PAYMENT_YEARS:
    us = _fetch(year, "us:*", "acs1", pay_vars).drop(columns=["us"])
    st = _fetch(year, "state:*", "acs1", pay_vars).drop(columns=["state"])
    frames += [us, st]
    _log(f"{year} (1-year): US + {len(st)} states")
pay = pd.concat(frames, ignore_index=True).rename(columns={"NAME": "state"})
pay["mortgage_rate"] = pay["year"].map(rate_by_year)
r = pay["mortgage_rate"] / 100 / 12
pay["monthly_payment"] = LTV * pay["Median Home Value"] * r / (1 - (1 + r) ** -TERM_MONTHS)
pay["pct_payment_to_income"] = 12 * pay["monthly_payment"] / pay["Median Household Income"]
pay["real_home_value"] = pay["Median Home Value"] * cpi[max(PAYMENT_YEARS)] / pay["year"].map(cpi)
pay["price_to_income"] = pay["Median Home Value"] / pay["Median Household Income"]
pay.sort_values(["state", "year"]).to_csv("c_housing_payment.csv", index=False)
_log(f"Wrote c_housing_payment.csv ({len(pay)} rows)")
