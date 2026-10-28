"""Does a demographic (MRP) estimate of each district's vote improve the House
forecast beyond presidential lean? A backtest, not part of the live forecast.

For each House election year Y from 2010 to 2022:
1. Vote model: from the CES of Y-2 (the last survey a forecaster would have),
   House vote (D vs R) among validated voters ~ sex, age, race, education,
   state, plus race x education, race x age and sex x education. L2-penalised
   logistic regression stands in for a multilevel model.
2. Turnout model: from the CES of Y-4 (the last election of the same type),
   validated turnout ~ the same predictors.
3. Frame: each district's adults by sex x age x race x education, from the ACS
   5-year release on Y's district lines, by iterative proportional fitting of
   three ACS tables (race x sex x age, sex x age x education, and race x sex x
   education for 25+).
4. mrp = the district's turnout-weighted predicted Democratic share, minus the
   nation's.

Then the House model (lean + inc, national fixed) is refit with and without
mrp on earlier years and scored on the 2014, 2018 and 2022 midterms.

Caveats: the frame counts all adults, including non-citizens; the ACS releases
for 2012, 2018 and 2022 came out after those elections (only demographics
leak); 2020 uses 2018 lines, missing North Carolina's redraw.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import dotenv_values
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import house_model as hm  # noqa: E402

DATA, RAW = HERE / "data", HERE / "raw"
ACS_CACHE = RAW / "acs_mrp"
CES = RAW / "ces" / "cumulative_2006-2025.feather"

# House election year -> (ACS 5-year vintage on that year's lines, vote CES, turnout CES)
TARGETS = {
    2010: (2009, 2008, 2006),
    2012: (2012, 2010, 2008),
    2014: (2013, 2012, 2010),
    2016: (2016, 2014, 2012),
    2018: (2018, 2016, 2014),
    2020: (2018, 2018, 2016),
    2022: (2022, 2020, 2018),
}
TEST = (2014, 2018, 2022)

SEX = ["Male", "Female"]
AGE = ["18-24", "25-34", "35-44", "45-64", "65+"]
RACE = ["White", "Black", "Hispanic", "Asian", "Other"]
EDUC = ["HS or less", "Some college", "BA", "Postgrad"]
# Census race-iterated tables: B = Black alone, D = Asian alone, H = white alone
# not Hispanic, I = Hispanic. "Other" is the remainder of the all-races table.
RACE_SUFFIX = {"White": "H", "Black": "B", "Hispanic": "I", "Asian": "D"}
REGION = {  # Census regions
    **dict.fromkeys(["CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"], "Northeast"),
    **dict.fromkeys(["IL", "IN", "MI", "OH", "WI", "IA", "KS", "MN", "MO", "NE", "ND", "SD"], "Midwest"),
    **dict.fromkeys(["DE", "FL", "GA", "MD", "NC", "SC", "VA", "DC", "WV", "AL", "KY", "MS", "TN",
                     "AR", "LA", "OK", "TX"], "South"),
    **dict.fromkeys(["AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY", "AK", "CA", "HI", "OR", "WA"], "West"),
}


# ACS frame ####################################################################

def acs_group(vintage, group):
    """One ACS table for every congressional district: estimates keyed by label."""
    path = ACS_CACHE / f"{vintage}_{group}.json"
    if not path.exists():
        key = dotenv_values(HERE.parent / ".env")["census_api_key"]
        base = f"https://api.census.gov/data/{vintage}/acs/acs5"
        labels = requests.get(f"{base}/groups/{group}.json", timeout=60).json()["variables"]
        rows = requests.get(base, params={"get": f"group({group})", "for": "congressional district:*",
                                          "in": "state:*", "key": key}, timeout=120).json()
        ACS_CACHE.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"labels": {k: v["label"] for k, v in labels.items()}, "rows": rows}))
    blob = json.loads(path.read_text())
    df = pd.DataFrame(blob["rows"][1:], columns=blob["rows"][0])
    est = [c for c in df.columns if c.endswith("E") and c in blob["labels"]]
    out = df[est].apply(pd.to_numeric, errors="coerce")
    out.columns = [tuple(p.strip(":") for p in blob["labels"][c].split("!!")[2:]) for c in est]
    out.index = pd.MultiIndex.from_arrays([df["state"], df["congressional district"]], names=["fips", "cd"])
    return out


def age_bin(label):
    """'18 and 19 years' / '45 to 64 years' / 'Under 5 years' -> AGE index, or None under 18."""
    lo = 0 if label.startswith("Under") else int(label.split()[0])
    return None if lo < 18 else 0 if lo < 25 else 1 if lo < 35 else 2 if lo < 45 else 3 if lo < 65 else 4


def educ4(label):
    if label.startswith("Graduate"):
        return 3
    if label.startswith("Bachelor"):
        return 2
    if label.startswith(("Some college", "Associate")):
        return 1
    return 0


def educ3(label):
    """B15002 / C15002 detail -> HS or less, some college, BA or higher."""
    if label.startswith(("Bachelor", "Master", "Professional", "Doctorate", "Graduate")):
        return 2
    if label.startswith(("Some college", "Associate")):
        return 1
    return 0


def margins(vintage):
    """Three margins per district: (sex, age, race), (sex, age, educ4), and for
    25+ (sex, race, educ3). Arrays indexed [district, ...]."""
    total = acs_group(vintage, "B01001")
    idx = total.index
    sar = np.zeros((len(idx), 2, 5, 5))
    for r, race in enumerate(RACE):
        t = total if race == "Other" else acs_group(vintage, f"B01001{RACE_SUFFIX[race]}").reindex(idx)
        for col in t.columns:
            if len(col) == 2 and age_bin(col[1]) is not None:
                sar[:, SEX.index(col[0]), age_bin(col[1]), r] += t[col].fillna(0).to_numpy()
    sar[..., 4] = np.clip(sar[..., 4] - sar[..., :4].sum(-1), 0, None)

    sae = np.zeros((len(idx), 2, 5, 4))
    b15001 = acs_group(vintage, "B15001").reindex(idx)
    for col in b15001.columns:
        if len(col) == 3:
            sae[:, SEX.index(col[0]), age_bin(col[1]), educ4(col[2])] += b15001[col].fillna(0).to_numpy()

    sre = np.zeros((len(idx), 2, 5, 3))
    for r, race in enumerate(RACE[:4]):
        t = acs_group(vintage, f"C15002{RACE_SUFFIX[race]}").reindex(idx)
        for col in t.columns:
            if len(col) == 2:
                sre[:, SEX.index(col[0]), r, educ3(col[1])] += t[col].fillna(0).to_numpy()
    b15002 = acs_group(vintage, "B15002").reindex(idx)
    for col in b15002.columns:
        if len(col) == 2:
            sre[:, SEX.index(col[0]), 4, educ3(col[1])] += b15002[col].fillna(0).to_numpy()
    sre[:, :, 4, :] = np.clip(sre[:, :, 4, :] - sre[:, :, :4, :].sum(2), 0, None)
    return idx, sar, sae, sre


def ipf(sar, sae, sre, iters=60):
    """Joint sex x age x race x educ4 per district matching all three margins."""
    to3 = np.array([0, 1, 2, 2])  # educ4 -> educ3
    x = np.ones(sar.shape[:4] + (4,))
    for _ in range(iters):
        x *= np.nan_to_num(sar / x.sum(-1))[..., None]
        x *= np.nan_to_num(sae / x.sum(-2))[..., None, :]
        cur = np.zeros(sre.shape)
        for e in range(4):
            cur[..., to3[e]] += x[:, :, 1:, :, e].sum(2)  # ages 25+
        ratio = np.nan_to_num(sre / cur)
        for e in range(4):
            x[:, :, 1:, :, e] *= ratio[:, :, None, :, to3[e]]
    return x


def frame(vintage):
    """Long frame: one row per district x cell, with ACS adult count n."""
    path = DATA / f"mrp_frame_{vintage}.csv"
    if path.exists():
        return pd.read_csv(path, dtype={"fips": str})
    idx, sar, sae, sre = margins(vintage)
    x = ipf(sar, sae, sre)
    s, a, r, e = np.meshgrid(range(2), range(5), range(5), range(4), indexing="ij")
    cells = pd.DataFrame({"sex": np.array(SEX)[s.ravel()], "age": np.array(AGE)[a.ravel()],
                          "race": np.array(RACE)[r.ravel()], "educ": np.array(EDUC)[e.ravel()]})
    fips_po = (pd.read_csv(DATA / "elections_house.csv")[["state_fips", "state_po"]]
               .drop_duplicates().assign(fips=lambda d: d.state_fips.map("{:02d}".format)))
    rows = []
    for i, (fips, cd) in enumerate(idx):
        if cd in ("98", "ZZ"):  # DC delegate, unassigned
            continue
        rows.append(cells.assign(fips=fips, district=int(cd), n=x[i].ravel()))
    out = pd.concat(rows).merge(fips_po[["fips", "state_po"]], how="left")
    out.to_csv(path, index=False)
    return out


# CES models ##################################################################

def ces():
    cols = ["year", "st", "gender", "age", "race_h", "educ", "vv_turnout_gvm", "voted_rep_party",
            "citizen", "weight", "vvweight_post", "weight_post"]
    d = pd.read_feather(CES, columns=cols)
    d = d[d.gender.notna() & d.educ.notna() & d.age.notna() & (d.st != "DC")]
    d["sex"] = np.where(d.gender == 1, "Male", "Female")
    d["age"] = pd.cut(d.age, [17, 24, 34, 44, 64, 200], labels=AGE).astype(str)
    d["race"] = d.race_h.map({1: "White", 2: "Black", 3: "Hispanic", 4: "Asian"}).fillna("Other")
    d["educ"] = d.educ.map({1: 0, 2: 0, 3: 1, 4: 1, 5: 2, 6: 3}).map(dict(enumerate(EDUC)))
    d["state_po"] = d.st.astype(str)
    return d


def design(df):
    x = df[["sex", "age", "race", "educ", "state_po"]].astype(str).copy()
    x["region"] = x.state_po.map(REGION)
    x["race_educ"] = x.race + "|" + x.educ
    x["race_age"] = x.race + "|" + x.age
    x["sex_educ"] = x.sex + "|" + x.educ
    x["race_region"] = x.race + "|" + x.region
    return x


def fit_logit(df, y, w):
    enc = OneHotEncoder(handle_unknown="ignore")
    model = LogisticRegression(C=0.5, max_iter=2000)
    model.fit(enc.fit_transform(design(df)), y, sample_weight=w / w.mean())
    return lambda new: model.predict_proba(enc.transform(design(new)))[:, 1]


def district_estimates(year, data):
    vintage, vote_year, turn_year = TARGETS[year]
    v = data[(data.year == vote_year) & (data.vv_turnout_gvm == "Voted")
             & data.voted_rep_party.isin(["Democratic", "Republican"])]
    vote = fit_logit(v, (v.voted_rep_party == "Democratic").astype(int),
                     v.vvweight_post.fillna(v.weight_post).fillna(v.weight).fillna(1).to_numpy())
    t = data[(data.year == turn_year) & data.vv_turnout_gvm.isin(["Voted", "No Record of Voting"])
             & (data.citizen != 2)]
    turnout = fit_logit(t, (t.vv_turnout_gvm == "Voted").astype(int), t.weight.fillna(1).to_numpy())
    f = frame(vintage).dropna(subset=["state_po"])
    f = f[f.state_po != "DC"]
    f["voters"] = f.n * turnout(f)
    f["dem"] = f.voters * vote(f)
    g = f.groupby(["state_po", "district"])[["voters", "dem"]].sum()
    est = (g.dem / g.voters).rename("mrp_dem2").reset_index()
    est["mrp"] = est.mrp_dem2 - g.dem.sum() / g.voters.sum()
    return est.assign(year=year)


# Backtest ####################################################################

def fit(train, use_mrp):
    cols = [np.ones(len(train)), train.lean, train.inc] + ([train.mrp] if use_mrp else [])
    X = np.column_stack(cols)
    y = train.dem2 - 0.5 - train.house
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    return beta, float(np.sqrt((resid**2).sum() / (len(y) - X.shape[1])))


def predict(df, beta, use_mrp, house):
    cols = [np.ones(len(df)), df.lean, df.inc] + ([df.mrp] if use_mrp else [])
    return 0.5 + house + np.column_stack(cols) @ beta


def main():
    data = ces()
    est = pd.concat([district_estimates(y, data) for y in TARGETS])
    est.to_csv(DATA / "mrp_district_estimates.csv", index=False)
    lean = pd.read_csv(DATA / "house_lean.csv")
    races = hm.house_races().merge(lean[["year", "state_po", "district", "lean"]], how="left")
    races["house"] = races.year.map(hm.house_vote())
    races = races.merge(est, how="left")
    usable = races[races.contested & races.lean.notna() & races.mrp.notna() & races.year.between(2010, 2022)]
    print("races with mrp:", usable.groupby("year").size().to_dict())
    print("corr(mrp, lean) by year:", usable.groupby("year").apply(
        lambda d: round(np.corrcoef(d.mrp, d.lean)[0, 1], 2), include_groups=False).to_dict())

    for year in TEST:
        train, test = usable[usable.year < year], usable[usable.year == year]
        line = [f"{year} (train {sorted(train.year.unique())}):"]
        for use_mrp in (False, True):
            beta, sd = fit(train, use_mrp)
            # Actual national House vote: isolates the district-level skill both share
            pred = predict(test, beta, use_mrp, test.house)
            err = pred - test.dem2
            p = 1 - hm.norm.cdf((0.5 - pred) / sd)
            won = test.dem2 > 0.5
            line.append(f"  {'lean+inc+mrp' if use_mrp else 'lean+inc':13s} MAE {err.abs().mean():.4f}, "
                        f"RMSE {np.sqrt((err**2).mean()):.4f}, called {((pred > 0.5) == won).sum()}/{len(test)}, "
                        f"Brier {((p - won) ** 2).mean():.4f}"
                        + (f", b_mrp {beta[-1]:+.2f}, b_lean {beta[1]:.2f}" if use_mrp else ""))
        print("\n".join(line))


if __name__ == "__main__":
    main()
