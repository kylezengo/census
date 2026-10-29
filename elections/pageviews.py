"""Test: do Wikipedia page views add anything to the Senate forecast?

For each Senate race 2016-2024 (page-view data starts July 2015), take each
nominee's Wikipedia article views over the same window the polls use (6-30
days before the election) and form
    views = log(D-side views / R views).
Then ask whether it explains what the backtest forecast got wrong:
    dem2 - forecast = g * views + error
fit on earlier years only and scored on the next (2018-2024). Done for both
the poll blend and fundamentals alone, since views might stand in for polls
where there are none.

Articles are found by matching each nominee's name to a link on the race's
Wikipedia page (the API's link list, 50 pages per request, cached in
raw/pageviews/links.json): the shortest link title with the nominee's last name
and first name (or its first three letters, for nicknames).
Views count the article's title as it is now; views logged under an older
title before a rename are missed.

Backtest only, not part of the forecast. Reads c_backtest_senate_races.csv.
"""

import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).parent))
import senate_model as sm  # noqa: E402
import senate_polls as sp  # noqa: E402

RAW = Path(__file__).parent / "raw" / "pageviews"
API = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/"
       "all-access/user/{}/daily/{}/{}")
ELECTION_DAY = {2016: "2016-11-08", 2018: "2018-11-06", 2020: "2020-11-03",
                2022: "2022-11-08", 2024: "2024-11-05"}
YEARS = tuple(ELECTION_DAY)
# Wikimedia throttles scripts without contact details hard; pass one in the
# wiki_user_agent environment variable to get the normal limits.
UA = {"User-Agent": os.environ.get("wiki_user_agent") or sp.UA["User-Agent"]}


def plain(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z ]", " ", s.upper())


def get(url, params=None):
    """GET with Wikimedia's rate limits respected."""
    for _ in range(10):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=60)
        except requests.ConnectionError:
            time.sleep(30)
            continue
        if r.status_code != 429:
            return r
        time.sleep(int(r.headers.get("retry-after", 5)) + 1)
    r.raise_for_status()
    return r


def race_title(year, st, special):
    names = pd.read_csv(Path(__file__).parent / "raw" / "1976-2024-senate-state.csv")
    state = names.loc[names.state_po == st, "state"].iloc[0].title()
    return f"{year} United States Senate {'special ' if special else ''}election in {state}"


def page_links(titles):
    """Article links on each page, 50 pages per request, cached."""
    path = RAW / "links.json"
    cache = json.loads(path.read_text()) if path.exists() else {}
    todo = [t for t in titles if t not in cache]
    for i in range(0, len(todo), 50):
        batch = todo[i:i + 50]
        params = {"action": "query", "titles": "|".join(batch), "prop": "links", "pllimit": "max",
                  "plnamespace": 0, "redirects": 1, "format": "json", "formatversion": 2}
        found = {t: [] for t in batch}
        while True:
            d = get(sp.WIKI_API, params).json()
            back = {r["to"]: r["from"] for r in d["query"].get("redirects", [])}
            for pg in d["query"]["pages"]:
                found.setdefault(back.get(pg["title"], pg["title"]), []).extend(
                    lk["title"] for lk in pg.get("links", []))
            if "continue" not in d:
                break
            params.update(d["continue"])
            time.sleep(2)
        cache.update(found)
        RAW.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache))
        time.sleep(2)
    return cache


def article(links, cand):
    """The nominee's article among a race page's links: the shortest title with
    the nominee's last name and first name (or its first three letters)."""
    toks = sm.name_tokens(cand)
    if not toks:
        return None
    first, last = toks[0], toks[-1]
    hits = []
    for title in links:
        words = plain(title).split()
        if words and last in words and (first in words or first[:3] == words[0][:3]):
            hits.append(title)
    return min(hits, key=len) if hits else None


def views(title, year):
    """Total user views 6-30 days before the election."""
    day = pd.Timestamp(ELECTION_DAY[year])
    start, end = (day - pd.Timedelta(days=30)).strftime("%Y%m%d"), (day - pd.Timedelta(days=6)).strftime("%Y%m%d")
    path = RAW / str(year) / f"{quote(title, safe='')}.json"
    if not path.exists():
        r = get(API.format(quote(title.replace(" ", "_"), safe=""), start, end))
        if r.status_code == 404:
            return 0.0
        r.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(r.text)
        time.sleep(1)
    return float(sum(i["views"] for i in pd.read_json(path).get("items", [])))


def collect():
    bt = pd.read_csv(sm.ROOT / "c_backtest_senate_races.csv")
    bt = bt[bt.year.isin(YEARS)].copy()
    titles = [race_title(r.year, r.state_po, r.special) for r in bt.itertuples()]
    links = page_links(titles)
    rows = []
    for r, title in zip(bt.itertuples(), titles):
        d_art, r_art = article(links.get(title, []), r.dem_cand), article(links.get(title, []), r.rep_cand)
        rows.append({"d_article": d_art, "r_article": r_art,
                     "d_views": views(d_art, r.year) if d_art else np.nan,
                     "r_views": views(r_art, r.year) if r_art else np.nan})
    return pd.concat([bt.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def oos(df, col, pred, feature):
    """Out-of-sample: for each year from 2018, fit g on earlier years and add
    g * feature to the forecast. Returns the adjusted forecasts."""
    out = df[pred].copy()
    for year in YEARS[1:]:
        train = df[(df.year < year) & df[feature].notna()]
        x, y = train[feature], train.dem2 - train[pred]
        g = float((x * y).sum() / (x * x).sum())
        test = (df.year == year) & df[feature].notna()
        out[test] = df.loc[test, pred] + g * df.loc[test, feature]
    return out


def main():
    df = collect()
    found = df.d_views.gt(0) & df.r_views.gt(0)
    print(f"races {len(df)}, both articles found with views: {found.sum()}")
    missing = df[~found]
    if len(missing):
        print(missing[["year", "state_po", "dem_cand", "rep_cand", "d_article", "r_article"]].to_string(index=False))
    df["views"] = np.where(found, np.log(df.d_views / df.r_views), np.nan)
    df["resid_blend"] = df.dem2 - df.pred
    df["resid_fund"] = df.dem2 - df.fund
    d = df[found]
    print(f"\ncorrelation of log view ratio with: actual {d.views.corr(d.dem2):.2f}, "
          f"blend residual {d.views.corr(d.resid_blend):.2f}, fundamentals residual "
          f"{d.views.corr(d.resid_fund):.2f}")
    for name, sub in (("polled", d[d.n_polls > 0]), ("unpolled", d[d.n_polls.fillna(0) == 0])):
        print(f"  {name} races ({len(sub)}): corr with blend residual {sub.views.corr(sub.resid_blend):.2f}")
    full = np.polyfit(d.views, d.resid_blend, 1)
    print(f"all-years slope on blend residual: {full[0]:.4f} per log unit")

    # Out-of-sample scoring, 2018-2024. Race sd for probabilities: the backtest's
    # implied sd, recovered from pred and p_dem.
    test = df[df.year >= YEARS[1]].copy()
    sd = ((test.pred - 0.5) / norm.ppf(test.p_dem.clip(1e-6, 1 - 1e-6))).abs().clip(0.01, 0.2)
    won = test.dem2 > 0.5
    for pred, label in (("pred", "poll blend"), ("fund", "fundamentals")):
        adj = oos(df, "views", pred, "views").loc[test.index]
        base_mae = (test[pred] - test.dem2).abs().mean()
        adj_mae = (adj - test.dem2).abs().mean()
        print(f"\n{label}: MAE {base_mae:.4f} -> {adj_mae:.4f} with views")
        if pred == "pred":
            p_adj = norm.cdf((adj - 0.5) / sd)
            print(f"  called {((test.p_dem > 0.5) == won).sum()} -> {((p_adj > 0.5) == won).sum()} of {len(test)}; "
                  f"Brier {((test.p_dem - won) ** 2).mean():.4f} -> {((p_adj - won) ** 2).mean():.4f}")
        for year in YEARS[1:]:
            t = test.year == year
            print(f"  {year}: MAE {(test[pred][t] - test.dem2[t]).abs().mean():.4f} -> "
                  f"{(adj[t] - test.dem2[t]).abs().mean():.4f}")
    df.to_csv(sm.DATA / "senate_pageviews.csv", index=False)


if __name__ == "__main__":
    main()
