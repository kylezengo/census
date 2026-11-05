"""Score the 2026 forecast against the results and the prediction markets.

Results come from Wikipedia's Senate and House election pages, whose per-race
tables give a status ("Democratic gain") and mark the winner in the candidate
list. A race counts as called only when a winner is marked; re-run as the
remaining races are called (each run saves its pages under raw/results2026/
<date>/ and the race table at data/results_2026.csv).

Scored the way the forecast was framed: a race is a "Democratic" win when the
Republican lost, so independents in NE, MT, ID and SD count on that side.
Compared: the Oct 28 and Nov 2 forecasts (data/forecast_<date>/) and the Kalshi
and Polymarket prices of Oct 29 and Nov 2 (markets.py). Each comparison uses
only races every source in it priced.

Set wiki_user_agent in the environment if Wikipedia rate-limits the default.
"""

import os
import re
import sys
from datetime import date
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent))
import markets as mk  # noqa: E402
import senate_model as sm  # noqa: E402
import senate_polls as sp  # noqa: E402

RAW = Path(__file__).parent / "raw" / "results2026"
PAGES = {"senate": "2026 United States Senate elections",
         "house": "2026 United States House of Representatives elections"}
FORECASTS = ("2026-10-28", "2026-11-02")
SNAPSHOTS = {"oct29": "2026-10-29", "nov2": "2026-11-03"}  # file dates (UTC)
WINNER = re.compile(r"▌\s+([^▌]+?)\s*\(([^)]*)\)")  # a space follows the mark only on the winner


def page(chamber):
    day = RAW / date.today().isoformat()
    path = day / f"{chamber}.html"
    if not path.exists():
        ua = {"User-Agent": os.environ.get("wiki_user_agent") or sp.UA["User-Agent"]}
        r = requests.get(sp.WIKI_API, params={"action": "parse", "page": PAGES[chamber], "prop": "text",
                                              "format": "json", "formatversion": 2, "redirects": 1},
                         headers=ua, timeout=60)
        r.raise_for_status()
        day.mkdir(parents=True, exist_ok=True)
        path.write_text(r.json()["parse"]["text"])
    return path.read_text()


def flat(t):
    t = t.copy()
    t.columns = [c[-1] if isinstance(c, tuple) else c for c in t.columns]
    return t


def winner(cands):
    """(name, party) of the first marked candidate. A "Runoff:" list means the
    race goes to December (Louisiana), so no winner yet. Unmarked one-party races
    (California's top-two) are settled for scoring: either candidate gives that
    party the seat."""
    cands = str(cands)
    marked = WINNER.findall(cands)
    if "Runoff:" in cands:
        return None, None
    if marked:
        return marked[0]
    parties = set(re.findall(r"\(([^)]*)\)", cands))
    return ("(same party)", parties.pop()) if len(parties) == 1 else (None, None)


def race_tables(chamber):
    """Every table with a status and candidates column, as (place, status, candidates)."""
    rows = []
    for t in pd.read_html(StringIO(page(chamber))):
        t = flat(t)
        cand = next((c for c in t.columns if str(c).startswith("Candidates")), None)
        status = next((c for c in t.columns if str(c) in ("Status", "Results", "Result")), None)
        first = t.columns[0]
        if cand is None or status is None or str(first) not in ("State", "Location", "District"):
            continue
        rows.append(t[[first, status, cand]].set_axis(["place", "status", "candidates"], axis=1))
    return pd.concat(rows, ignore_index=True)


def results():
    sen = race_tables("senate")
    sen["state_po"] = sen.place.str.replace(r"\s*\(.*\)", "", regex=True).map(mk.STATE_PO)
    sen = sen.dropna(subset=["state_po"]).assign(chamber="senate", district=-1)
    hou = race_tables("house")
    m = hou.place.astype(str).str.replace("\xa0", " ").str.extract(r"^(.+?) (\d+|at-large)$", flags=re.I)
    hou = hou.assign(state_po=m[0].str.title().map(mk.STATE_PO),
                     district=pd.to_numeric(m[1], errors="coerce").fillna(0).astype(int), chamber="house")
    # specials appear first on the page; the general's row is the later copy
    hou = hou.dropna(subset=["state_po"]).drop_duplicates(["state_po", "district"], keep="last")
    r = pd.concat([sen, hou], ignore_index=True)
    w = r.candidates.map(winner)
    r["winner"], r["party"] = w.str[0], w.str[1]
    r["called"] = r.winner.notna()
    r["dem_won"] = np.where(r.called, (r.party != "Republican").astype(float), np.nan)
    r = r[["chamber", "state_po", "district", "status", "winner", "party", "called", "dem_won"]]
    r.to_csv(sm.DATA / "results_2026.csv", index=False)
    return r


def brier(p, y):
    return float(((p - y) ** 2).mean())


def logloss(p, y):
    p = np.clip(p, 0.01, 0.99)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def table(r):
    """One row per called race: outcome, each forecast, each market snapshot."""
    keys = ["chamber", "state_po", "district"]
    t = r[r.called][keys + ["winner", "party", "dem_won"]].copy()
    for d in FORECASTS:
        folder = sm.DATA / f"forecast_{d}"
        sen = pd.read_csv(folder / "c_forecast_senate_2026.csv")[["state_po", "p_dem"]].assign(
            chamber="senate", district=-1)
        hou = pd.read_csv(folder / "c_forecast_house_2026.csv")[["state_po", "district", "p_dem"]].assign(
            chamber="house")
        t = t.merge(pd.concat([sen, hou]).rename(columns={"p_dem": f"ours_{d[5:]}"}), on=keys, how="left")
    for name, day in SNAPSHOTS.items():
        p = mk.race_probs(pd.read_csv(sm.DATA / f"markets_{day}.csv"))
        p = p[p.chamber.isin(["senate", "house"])]
        for src in ("kalshi", "polymarket"):
            q = p[p.source == src][keys + ["p_dem"]].rename(columns={"p_dem": f"{src}_{name}"})
            t = t.merge(q, on=keys, how="left")
    return t


def score(t, cols, label):
    d = t.dropna(subset=cols)
    print(f"\n{label}: {len(d)} races, {int(d.dem_won.sum())} Democratic-side wins")
    for c in cols:
        print(f"  {c:18s} called {((d[c] > 0.5) == (d.dem_won == 1)).sum():3d}/{len(d)}  "
              f"Brier {brier(d[c], d.dem_won):.4f}  log loss {logloss(d[c], d.dem_won):.4f}  "
              f"expected D {d[c].sum():6.1f}")


def seats_check(r):
    """Where the called wins, and NBC's estimate, land in the seat distributions."""
    called = {"senate": 34 + int(r[(r.chamber == "senate")].dem_won.sum()),  # 34 D-caucus seats not up
              "house": int(r[r.chamber == "house"].dem_won.sum())}
    for d in FORECASTS:
        for ch, f in (("senate", "c_forecast_senate_seats_2026.csv"), ("house", "c_forecast_house_seats_2026.csv")):
            s = pd.read_csv(sm.DATA / f"forecast_{d}" / f)
            cum = s.prob.cumsum()
            lo, med, hi = (s.seats[cum >= q].iloc[0] for q in (0.1, 0.5, 0.9))
            print(f"{d} {ch}: median {med}, 80% {lo}-{hi}; called so far {called[ch]}, "
                  f"P(at least that) {s.prob[s.seats >= called[ch]].sum():.3f}")
            if ch == "house":
                print(f"   P(258+) {s.prob[s.seats >= 258].sum():.4f}")


def house_detail(t):
    f = pd.read_csv(sm.DATA / "forecast_2026-11-02" / "house_forecast_2026.csv")
    h = t[t.chamber == "house"].merge(f[["state_po", "district", "inc", "incumbents", "dem", "rep", "lean"]],
                                      on=["state_po", "district"])
    h["err"] = h.dem_won - h["ours_11-02"]
    print("\nHouse by seat type (inc +1 D incumbent, -1 R incumbent, 0 open):")
    g = h.dropna(subset=["polymarket_nov2"]).groupby("inc")
    print(pd.DataFrame({"n": g.size(), "dem_won": g.dem_won.sum(), "ours": g["ours_11-02"].sum(),
                        "market": g.polymarket_nov2.sum(),
                        "brier_ours": g.apply(lambda d: brier(d["ours_11-02"], d.dem_won), include_groups=False),
                        "brier_mkt": g.apply(lambda d: brier(d.polymarket_nov2, d.dem_won), include_groups=False)}
                       ).round(3).to_string())
    miss = h[(h["ours_11-02"] > 0.5) != (h.dem_won == 1)].sort_values("ours_11-02")
    print(f"\nHouse wrong calls ({len(miss)}), Democratic-side wins {int(miss.dem_won.sum())}:")
    print(miss[["state_po", "district", "winner", "party", "inc", "lean", "ours_11-02", "polymarket_nov2"]]
          .round(3).to_string(index=False))
    b = pd.cut(h["ours_11-02"], [0, .1, .3, .5, .7, .9, 1], include_lowest=True)
    print("\nHouse calibration, our Nov 2 forecast:")
    print(h.groupby(b, observed=True).agg(n=("dem_won", "size"), mean_p=("ours_11-02", "mean"),
                                          dem_won=("dem_won", "mean")).round(3).to_string())


def senate_detail(t):
    f = pd.read_csv(sm.DATA / "forecast_2026-11-02" / "senate_forecast_2026.csv")
    s = t[t.chamber == "senate"].merge(f[["state_po", "p_fund", "n_polls"]], on="state_po")
    score(s, ["p_fund", "ours_11-02"], "senate, fundamentals vs poll blend")
    score(s[s.n_polls > 0], ["p_fund", "ours_11-02"], "senate, fundamentals vs blend, polled races")


def main():
    r = results()
    print(r.groupby("chamber").called.agg(["size", "sum"]).to_string())
    print("uncalled:", r[~r.called].assign(race=lambda x: x.state_po + x.district.astype(str))
          .race.tolist())
    print(r[r.called].groupby(["chamber", "party"]).size().to_string())
    t = table(r)
    t.to_csv(sm.DATA / "scored_2026.csv", index=False)
    ours = [f"ours_{d[5:]}" for d in FORECASTS]
    for ch in ("senate", "house"):
        c = t[t.chamber == ch]
        score(c, ours, f"{ch}, forecast versions")
        score(c, [ours[1], "polymarket_nov2"], f"{ch}, ours vs Polymarket Nov 2")
        score(c, [ours[1], "kalshi_nov2", "polymarket_nov2"], f"{ch}, ours vs both markets Nov 2")
        score(c, [ours[0], "kalshi_oct29", "polymarket_oct29"], f"{ch}, Oct 28 forecast vs Oct 29 markets")
    senate_detail(t)
    house_detail(t)
    print()
    seats_check(r)
    return t


if __name__ == "__main__":
    main()
