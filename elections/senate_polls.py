"""Blend state polls into the Senate fundamentals forecast.

    dem2 = fund + a * (poll - fund) + error

fund is senate_model's fundamentals prediction (the 12-spec average), made for
each year using only earlier years and that year's generic-ballot average. poll
is the race's average of nonpartisan polls fielded 6-30 days before the election
(6-60 if there are none). a, and the race and year errors, are fit on earlier
years only, so the backtest never sees the future. Races with no polls keep the
fundamentals forecast and its error.

Historical polls: FiveThirtyEight's pollster-ratings raw_polls.csv (1998-2022),
saved in raw/polls/. Generic ballot for each year comes from the same file
(House-G-US polls), so every year's national input is measured the same way.
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).parent))
import senate_model as sm  # noqa: E402

DATA, RAW = sm.DATA, Path(__file__).parent / "raw" / "polls"
RAW_POLLS = "https://raw.githubusercontent.com/fivethirtyeight/data/master/pollster-ratings/raw_polls.csv"
DAYS = (6, 30)  # we're 6 days out; the window a forecast at that point would see
BACKTEST = (2010, 2014, 2018, 2022)
MIN_TRAIN_YEAR = 1998


def raw_polls():
    path = RAW / "raw_polls.csv"
    if not path.exists():
        import requests
        RAW.mkdir(parents=True, exist_ok=True)
        r = requests.get(RAW_POLLS, timeout=120)
        r.raise_for_status()
        path.write_bytes(r.content)
    return pd.read_csv(path)


def two_party(p):
    """Democratic (or the non-Republican finisher's) share of the top-two poll vote."""
    d_first = p.cand1_party != "REP"
    dem = np.where(d_first, p.cand1_pct, p.cand2_pct)
    rep = np.where(d_first, p.cand2_pct, p.cand1_pct)
    ok = (p.cand1_party == "REP") ^ (p.cand2_party == "REP")  # exactly one Republican
    return pd.Series(np.where(ok, dem / (dem + rep), np.nan), index=p.index)


def window_mean(p, col):
    """Mean of nonpartisan polls 6-30 days out, else 6-60."""
    near = p[p.time_to_election.between(*DAYS) & p.partisan.isna()]
    far = p[p.time_to_election.between(DAYS[0], 60) & p.partisan.isna()]
    use = near if len(near) else far
    return pd.Series({col: use.d2.mean() if len(use) else np.nan, "n_polls": len(use)})


def generic_ballot_history(p):
    g = p[p.type_simple.str.startswith("House-G") & (p.location == "US")].copy()
    g["d2"] = g.cand1_pct / (g.cand1_pct + g.cand2_pct)
    return g.groupby("cycle").apply(window_mean, "gb", include_groups=False).gb


def senate_poll_averages(p):
    """Poll average per race, keyed like senate_model's races. A state with two
    November races that year is matched by candidate surname."""
    s = p[(p.type_simple == "Sen-G") & p.electiondate.str[5:7].eq("11")].copy()
    s["d2"] = two_party(s)
    cands = pd.read_csv(DATA / "elections_senate.csv")
    cands = cands[cands.stage == "gen"]
    rows = []
    for (year, st, race_id), d in s.groupby(["cycle", "location", "race_id"]):
        ours = cands[(cands.year == year) & (cands.state_po == st)]
        specials = ours.special.unique()
        if len(specials) > 1:
            names = {sm.name_tokens(n)[-1] for n in pd.concat([d.cand1_name, d.cand2_name]) if sm.name_tokens(n)}
            hit = [sp for sp in specials
                   if names & {t[-1] for t in ours[ours.special == sp].candidate.map(sm.name_tokens) if t}]
            if len(hit) != 1:
                continue
            special = hit[0]
        elif len(specials) == 1:
            special = specials[0]
        else:
            continue
        rows.append({"year": year, "state_po": st, "special": special, **window_mean(d, "poll")})
    return pd.DataFrame(rows)


ELECTION_DAY_2026 = pd.Timestamp("2026-11-03")
WIKI_API = "https://en.wikipedia.org/w/api.php"
UA = {"User-Agent": "census-research/0.1 (personal research script)"}


def surname(label):
    """'Osborn (I)' -> 'OSBORN', 'D. Graham (appointed)' -> 'GRAHAM'."""
    return sm.name_tokens(re.sub(r"\(.*?\)", "", label))[-1]


def wiki_page(st, special):
    """A 2026 Senate race's Wikipedia page, saved under raw/polls/wiki2026/.
    Wikipedia rate-limits the API, so wait out any 429 and pause between pages."""
    path = RAW / "wiki2026" / f"{st}{'_special' if special else ''}.html"
    if not path.exists():
        import time
        import requests
        names = pd.read_csv(Path(__file__).parent / "raw" / "1976-2024-senate-state.csv")
        state = names.loc[names.state_po == st, "state"].iloc[0].title()
        title = f"2026 United States Senate {'special ' if special else ''}election in {state}"
        params = {"action": "parse", "page": title, "prop": "text", "format": "json",
                  "formatversion": 2, "redirects": 1}
        for _ in range(5):
            r = requests.get(WIKI_API, params=params, headers=UA, timeout=60)
            if r.status_code != 429:
                break
            time.sleep(int(r.headers.get("retry-after", 30)) + 5)
        r.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(r.json()["parse"]["text"])
        time.sleep(4)
    return path.read_text()


def poll_end_date(text, year=2026):
    """'October 22–25, 2026' -> Oct 25; 'Sep 28 – Oct 2, 2026' -> Oct 2."""
    text = re.sub(r"\[.*?\]", "", str(text)).replace("–", "-").replace("—", "-")
    last = text.split("-")[-1].strip()
    if not re.search(r"\d{4}", last):
        last = f"{last}, {year}"
    if not re.match(r"[A-Za-z]", last):  # 'October 22-25, 2026' -> '25, 2026'
        month = re.match(r"([A-Za-z]+)", text)
        last = f"{month.group(1) if month else ''} {last}"
    return pd.to_datetime(last, errors="coerce")


def pct(v):
    m = re.match(r"\s*<?(\d+(?:\.\d+)?)\s*%", str(v))
    return float(m.group(1)) if m else np.nan


def polls_2026(st, special, dem_label, rep_label):
    """Head-to-head general-election polls for one 2026 race: the table whose
    columns include both nominees. Returns pollster, end date, D-side and R
    shares, likely-voter flag and partisan flag, one row per poll question."""
    import io
    dem, rep = surname(dem_label), surname(rep_label)
    rows = []
    # Some pages carry malformed spans like rowspan="2"" that crash read_html
    html = re.sub(r'\b(rowspan|colspan)=["\']?(\d+)[^\s>]*', r'\1="\2"', wiki_page(st, special))
    for t in pd.read_html(io.StringIO(html)):
        cols = [" ".join(map(str, c)) if isinstance(c, tuple) else str(c) for c in t.columns]
        if not any("Poll source" in c for c in cols):
            continue
        dcol = [c for c in cols if dem in sm.name_tokens(re.sub(r"\(.*?\)", "", c))]
        rcol = [c for c in cols if rep in sm.name_tokens(re.sub(r"\(.*?\)", "", c))]
        if len(dcol) != 1 or len(rcol) != 1:
            continue
        t.columns = cols
        date_col = next(c for c in cols if c.startswith("Date"))
        source_col = next(c for c in cols if "Poll source" in c)
        size_col = next((c for c in cols if c.startswith("Sample")), None)
        for _, r in t.iterrows():
            d, rp = pct(r[dcol[0]]), pct(r[rcol[0]])
            if np.isnan(d) or np.isnan(rp):
                continue
            source = re.sub(r"\[.*?\]", "", str(r[source_col])).strip()
            rows.append({"state_po": st, "special": special, "pollster": source,
                         "end": poll_end_date(r[date_col]), "dem": d, "rep": rp,
                         "lv": "(LV)" in str(r[size_col]) if size_col else False,
                         "partisan": bool(re.search(r"\((?:D|R)\)", source))})
    return pd.DataFrame(rows, columns=["state_po", "special", "pollster", "end", "dem", "rep",
                                       "lv", "partisan"])


def poll_average_2026(polls):
    """Same rule as the history: nonpartisan polls 6-30 days out (else 6-60),
    one per pollster and end date, likely-voter version preferred."""
    if polls.empty:
        return np.nan, 0
    p = polls.assign(days=(ELECTION_DAY_2026 - polls.end).dt.days, d2=polls.dem / (polls.dem + polls.rep))
    p = p[~p.partisan].sort_values("lv", ascending=False).drop_duplicates(["pollster", "end"])
    near = p[p.days.between(*DAYS)]
    use = near if len(near) else p[p.days.between(DAYS[0], 60)]
    return (use.d2.mean() if len(use) else np.nan), len(use)


def fundamentals(usable, test, year, gb_dem2):
    """senate_model's 12-spec average for one year's races, trained on earlier years."""
    preds, sds = [], []
    for window, intercept, shift in sm.SPECS:
        m = sm.fit(usable[(usable.year < year) & (usable.year >= year - window)], intercept)
        house = gb_dem2 - 0.5 + (np.mean(sm.GB_MISSES) if shift else 0.0)
        preds.append(sm.mean_pred(test, m, house))
        sds.append(np.sqrt(m["sd"] ** 2 + sm.nat_sd(m) ** 2))
    return np.mean(preds, axis=0), float(np.mean(sds))


def poll_group(n):
    """A single poll earns far less weight than two or more (1998-2022: a 0.39
    vs 0.78-0.88), so the blend is fit separately for each."""
    return np.where(n >= 2, "multi", "single")


def fit_blend(train):
    """Per poll group: a by least squares on (poll - fund) -> (actual - fund), and
    the leftover error split into year and race parts like senate_model.fit."""
    out = {}
    for g, d in train.groupby(poll_group(train.n_polls)):
        x, y = d.poll - d.fund, d.dem2 - d.fund
        a = float((x * y).sum() / (x * x).sum())
        resid = y - a * x
        year_mean = resid.groupby(d.year).transform("mean")
        out[g] = {"a": a, "n": len(d),
                  "race_sd": float(np.sqrt(((resid - year_mean) ** 2).sum() / (len(resid) - 1))),
                  "year_sd": float(resid.groupby(d.year).mean().std(ddof=1))}
    return out


def blend(df, b):
    """Blended mean, and its race and year error sds (fundamentals where unpolled)."""
    g = pd.Series(poll_group(df.n_polls.fillna(0)), index=df.index)
    polled = df.poll.notna()
    a = g.map(lambda k: b[k]["a"]).where(polled, 0.0)
    mean = df.fund + a * (df.poll.fillna(df.fund) - df.fund)
    year_sd = g.map(lambda k: b[k]["year_sd"]).where(polled, FUND_YEAR_SD)
    race_sd = g.map(lambda k: b[k]["race_sd"]).where(
        polled, np.sqrt(np.maximum(df.fund_sd**2 - FUND_YEAR_SD**2, 0)))
    return mean, race_sd, year_sd


FUND_YEAR_SD = 0.015  # national part of the fundamentals error (senate_model nat_sd)


def history(p, usable):
    """Every Senate race 1998-2022 with its out-of-sample fundamentals forecast and
    poll average."""
    gb = generic_ballot_history(p)
    hist = []
    for year in sorted(gb.index):
        if year % 2 or year < MIN_TRAIN_YEAR:
            continue
        test = usable[usable.year == year].copy()
        test["fund"], test["fund_sd"] = fundamentals(usable, test, year, gb[year])
        hist.append(test)
    return pd.concat(hist).merge(senate_poll_averages(p), how="left")


def main():
    p = raw_polls()
    lean = sm.presidential_lean()
    races = sm.senate_races()
    races["lean"] = [sm.lean_before(lean, y, s) for y, s in zip(races.year, races.state_po)]
    races["house"] = races.year.map(sm.house_vote())
    usable = races[~races.excluded & races.lean.notna() & (races.year >= 1982)]
    hist = history(p, usable)
    print(f"races 1998-2022: {len(hist)}, with polls: {hist.poll.notna().sum()}")

    out = []
    for year in BACKTEST:
        b = fit_blend(hist[(hist.year < year) & hist.poll.notna()])
        t = hist[hist.year == year].copy()
        polled = t.poll.notna()
        t["blend"], race_sd, year_sd = blend(t, b)
        t["p_blend"] = norm.cdf((t.blend - 0.5) / np.hypot(race_sd, year_sd))
        t["p_fund"] = norm.cdf((t.fund - 0.5) / t.fund_sd)
        won = t.dem2 > 0.5
        print(f"\n{year}: " + ", ".join(f"{k} a {v['a']:.2f} (n {v['n']})" for k, v in b.items())
              + f"; {polled.sum()}/{len(t)} races polled")
        for name, col, pcol in (("fundamentals", "fund", "p_fund"), ("blend", "blend", "p_blend")):
            e = (t[col] - t.dem2)[polled]
            print(f"  {name:12s} polled-race MAE {e.abs().mean():.3f}, mean err {e.mean():+.3f}; "
                  f"called {((t[pcol] > 0.5) == won).sum()}/{len(t)}, Brier {((t[pcol] - won) ** 2).mean():.3f}")
        out.append(t)
    bt = pd.concat(out)
    bt.to_csv(DATA / "senate_polls_backtest.csv", index=False)
    won = bt.dem2 > 0.5
    for name, pcol in (("fundamentals", "p_fund"), ("blend", "p_blend")):
        print(f"all backtests {name}: called {((bt[pcol] > 0.5) == won).sum()}/{len(bt)}, "
              f"Brier {((bt[pcol] - won) ** 2).mean():.3f}")
    bins = pd.cut(bt.p_blend, [0, 0.1, 0.3, 0.5, 0.7, 0.9, 1])
    print(bt.groupby(bins, observed=True).agg(n=("p_blend", "size"), mean_p=("p_blend", "mean"),
                                              dem_won=("dem2", lambda x: (x > 0.5).mean())).round(2))

    # 2026: fundamentals with the current generic ballot, blended with current polls
    b = fit_blend(hist[hist.poll.notna()])
    f = pd.DataFrame(sm.RACES_2026, columns=["state_po", "special", "inc", "holder", "dem_side", "rep", "ind"])
    f["lean"] = [sm.lean_before(lean, 2026, s) for s in f.state_po]
    f["fund"], f["fund_sd"] = fundamentals(usable, f, 2026, sm.GENERIC_BALLOT[2026])
    polls = [polls_2026(r.state_po, r.special, r.dem_side, r.rep) for r in f.itertuples()]
    pd.concat(polls).to_csv(DATA / "senate_polls_2026.csv", index=False)
    f["poll"], f["n_polls"] = zip(*(poll_average_2026(x) for x in polls))
    f.loc[f.n_polls == 0, "poll"] = np.nan
    f["pred"], race_sd, year_sd = blend(f, b)

    # Shared national draw scaled by each race's year sd, plus independent race noise
    z = rng.normal(0, 1, sm.N_SIMS)[:, None]
    eps = rng.normal(0, 1, (sm.N_SIMS, len(f)))
    dem_win = (f.pred.to_numpy() + z * year_sd.to_numpy() + eps * race_sd.to_numpy()) > 0.5
    ind = f.ind.to_numpy()
    seats = sm.D_NOT_UP + dem_win[:, ~ind].sum(axis=1)
    f["p_dem"] = dem_win.mean(axis=0)
    f["p_fund"] = norm.cdf((f.fund - 0.5) / f.fund_sd)
    f.to_csv(DATA / "senate_forecast_2026.csv", index=False)
    sm.export_for_app("senate", f[["state_po", "special", "dem_side", "rep", "inc", "lean", "fund",
                                   "poll", "n_polls", "pred", "p_fund", "p_dem"]], seats)

    print(f"\n2026 blend: " + ", ".join(f"{k} a {v['a']:.2f}" for k, v in b.items()))
    print(f.sort_values("p_dem", ascending=False)[
        ["state_po", "dem_side", "rep", "fund", "poll", "n_polls", "pred", "p_fund", "p_dem"]].round(3).to_string(index=False))
    print(f"\nDemocratic caucus seats: median {np.median(seats):.0f}, 80% interval "
          f"{np.percentile(seats, 10):.0f}-{np.percentile(seats, 90):.0f}")
    print(f"P(Democrats reach 51): {(seats >= 51).mean():.2f}")
    print(f"independent wins (ID, MT, NE, SD), expected: {dem_win[:, ind].sum(axis=1).mean():.2f}; "
          f"P(Dems at 50 and an independent won): {((seats == 50) & (dem_win[:, ind].sum(axis=1) > 0)).mean():.2f}")


rng = np.random.default_rng(0)

if __name__ == "__main__":
    main()
