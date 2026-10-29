"""Presidential backtest, 2008-2024. Backtest only: no 2028 forecast yet.

Same shape as the Senate forecast, per state:
    dem2 = nat + c + b_lean * lean + error
- lean: the state's presidential lean in the previous election.
- nat: national two-party Democratic share. Training years use the actual
  vote; the test year uses the final national polling average (FiveThirtyEight
  raw_polls 6-30 days out for 2000-2020; FiveThirtyEight's final average via
  Wikipedia for 2024).
Then blended toward the state's poll average exactly as senate_polls does,
with the blend fit on earlier years only.

The national error is the root mean square of earlier years' national poll
misses (bias counts), plus the fit's own year-to-year deviation. Polling
misses also cluster by region (2016's Great Lakes states), so each Census
division shares a regional error too. Its size is estimated from earlier
years' forecast errors: the spread of division-by-year mean errors, net of the
national error and of sampling noise. It is carved out of the state error, so
each state's own probability is unchanged; only the states' errors now move
together, which widens the electoral-vote range.

Electoral votes: each state's House seats that year plus 2 (DC 3). Maine
and Nebraska are scored statewide, winner-take-all, so the actual totals can
differ by one from the official count (2008: Obama won NE-2; 2016: Trump won
ME-2). Writes c_backtest_president_{races,seats}.csv.
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).parent))
import senate_model as sm  # noqa: E402
import senate_polls as sp  # noqa: E402

BACKTEST = (2008, 2012, 2016, 2020, 2024)
FIRST_LEAN_YEAR = 1980  # MIT results start in 1976
NOMINEES = {2000: ("Gore", "Bush"), 2004: ("Kerry", "Bush"), 2008: ("Obama", "McCain"),
            2012: ("Obama", "Romney"), 2016: ("Clinton", "Trump"), 2020: ("Biden", "Trump"),
            2024: ("Harris", "Trump")}
# FiveThirtyEight's final 2024 national average (Harris 48.0, Trump 46.8), from
# Wikipedia's "Nationwide opinion polling for the 2024 United States
# presidential election"; raw_polls.csv stops at 2022.
NATIONAL_POLL_2024 = 48.0 / (48.0 + 46.8)
STATEWIDE_2024 = (sp.RAW / "pres2024" /
                  "Statewide_opinion_polling_for_the_2024_United_States_presidential_election.json")
WIN_EV = 270
# Census divisions
DIVISIONS = {
    "New England": "CT ME MA NH RI VT", "Middle Atlantic": "NJ NY PA",
    "East North Central": "IL IN MI OH WI", "West North Central": "IA KS MN MO NE ND SD",
    "South Atlantic": "DE DC FL GA MD NC SC VA WV", "East South Central": "AL KY MS TN",
    "West South Central": "AR LA OK TX", "Mountain": "AZ CO ID MT NV NM UT WY",
    "Pacific": "AK CA HI OR WA",
}
DIVISION = {st: d for d, sts in DIVISIONS.items() for st in sts.split()}
rng = np.random.default_rng(2)


def results():
    """Two-party Democratic share by state and year, and nationally."""
    p = pd.read_csv(sm.DATA / "elections_president.csv")
    p = p[p.party.isin(["DEM", "REP"])]
    st = p.pivot_table(index=["year", "state_po"], columns="party", values="votes",
                       aggfunc="sum").reset_index()
    st["dem2"] = st.DEM / (st.DEM + st.REP)
    nat = st.groupby("year")[["DEM", "REP"]].sum()
    return st[["year", "state_po", "dem2"]], nat.DEM / (nat.DEM + nat.REP)


def electoral_votes(year):
    h = pd.read_csv(sm.DATA / "elections_house.csv")
    h = h[(h.year == year) & (h.stage == "gen") & ~h.special & (h.state_po != "DC")]
    ev = h.groupby("state_po").district.nunique() + 2
    ev["DC"] = 3
    assert ev.sum() == 538, (year, ev.sum())
    return ev


def national_polls(p):
    """National two-party poll average 6-30 days out (else 6-60), by year."""
    g = p[(p.type_simple == "Pres-G") & (p.location == "US")].copy()
    g["d2"] = sp.two_party(g)
    avg = g.groupby("cycle").apply(sp.window_mean, "nat_poll", include_groups=False).nat_poll
    avg[2024] = NATIONAL_POLL_2024
    return avg


def state_polls(p):
    """State poll averages: FiveThirtyEight through 2020, Wikipedia for 2024."""
    s = p[(p.type_simple == "Pres-G") & p.location.str.fullmatch(r"[A-Z]{2}") & (p.location != "US")].copy()
    s["d2"] = sp.two_party(s)
    hist = s.groupby(["cycle", "location"]).apply(sp.window_mean, "poll", include_groups=False)
    hist = hist.reset_index().rename(columns={"cycle": "year", "location": "state_po"})

    import json
    html = json.loads(STATEWIDE_2024.read_text())["parse"]["text"]
    heads = [(m.start(), re.sub("<.*?>", "", m.group(1)).strip())
             for m in re.finditer(r"<h[23][^>]*>(.*?)</h[23]>", html)]
    names = pd.read_csv(Path(__file__).parent / "raw" / "1976-2024-senate-state.csv")
    state_po = dict(zip(names.state.str.title(), names.state_po)) | {"District Of Columbia": "DC"}
    rows, polls = [], []
    for (start, head), (end, _) in zip(heads, heads[1:] + [(len(html), "")]):
        st = state_po.get(head.title())
        if st is None:
            continue
        section = html[start:end]
        if "<table" not in section:
            continue
        d = sp.poll_tables(section, st, False, *NOMINEES[2024], year=2024)
        polls.append(d)
        avg, n = sp.poll_average_wiki(d, 2024)
        rows.append({"year": 2024, "state_po": st, "poll": avg, "n_polls": n})
    pd.concat(polls).to_csv(sm.DATA / "president_polls_2024.csv", index=False)
    out = pd.concat([hist, pd.DataFrame(rows)])
    out.loc[out.n_polls == 0, "poll"] = np.nan
    return out


def fit(train):
    """OLS of dem2 - nat on lean, with the error split into year and state parts."""
    X = np.column_stack([np.ones(len(train)), train.lean])
    y = train.dem2 - train.nat
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = pd.Series(y - X @ beta, index=train.index)
    year_mean = resid.groupby(train.year).transform("mean")
    race_sd = float(np.sqrt(((resid - year_mean) ** 2).sum() / (len(y) - 2)))
    return {"c": beta[0], "b_lean": beta[1], "race_sd": race_sd,
            "year_sd": float(resid.groupby(train.year).mean().std(ddof=1))}


def history(states, nat, nat_poll):
    """Every state 2000-2024 with its out-of-sample fundamentals forecast."""
    out = []
    for year in sorted(NOMINEES):
        m = fit(states[(states.year < year) & (states.year >= FIRST_LEAN_YEAR)])
        misses = (nat_poll - nat).dropna()
        misses = misses[misses.index < year]
        nat_err = float(np.sqrt((misses ** 2).mean())) if len(misses) else 0.02
        t = states[states.year == year].copy()
        t["fund"] = nat_poll[year] + m["c"] + m["b_lean"] * t.lean
        t["fund_sd"] = np.sqrt(m["race_sd"] ** 2 + m["year_sd"] ** 2 + nat_err ** 2)
        t["nat_sd"] = np.hypot(m["year_sd"], nat_err)
        out.append(t)
    return pd.concat(out)


def region_sd(train, b):
    """Regional error sd from earlier years' blended forecast errors: the variance
    of division-by-year mean errors (after removing each year's national mean),
    minus what state-level noise alone would produce."""
    pred, _, _ = sp.blend(train, b)
    r = (train.dem2 - pred)
    r = r - r.groupby(train.year).transform("mean")
    div = train.state_po.map(DIVISION)
    cell = r.groupby([train.year, div])
    means, n = cell.mean(), cell.size()
    within = float(((r - cell.transform("mean")) ** 2).sum() / (len(r) - len(means)))
    return float(np.sqrt(max(means.var(ddof=1) - (within / n).mean(), 0.0)))


def main():
    p = sp.raw_polls()
    states, nat = results()
    lean = sm.presidential_lean()
    states["lean"] = [sm.lean_before(lean, y, s) for y, s in zip(states.year, states.state_po)]
    states["nat"] = states.year.map(nat)
    states = states[states.lean.notna()]
    nat_poll = national_polls(p)
    hist = history(states, nat, nat_poll).merge(state_polls(p), how="left")
    print("national poll avg vs actual:",
          {y: f"{nat_poll[y]:.3f}/{nat[y]:.3f}" for y in sorted(NOMINEES)})

    out, seats_by_year = [], {}
    for year in BACKTEST:
        b = sp.fit_blend(hist[(hist.year < year) & hist.poll.notna()])
        t = hist[hist.year == year].copy()
        t["pred"], race_sd, year_sd = sp.blend(t, b)
        # Unpolled states keep the fundamentals' presidential national error
        year_sd = year_sd.where(t.poll.notna(), t.nat_sd)
        race_sd = race_sd.where(t.poll.notna(), np.sqrt(t.fund_sd ** 2 - t.nat_sd ** 2))
        t["p_dem"] = norm.cdf((t.pred - 0.5) / np.hypot(race_sd, year_sd))
        t["p_fund"] = norm.cdf((t.fund - 0.5) / t.fund_sd)
        ev = electoral_votes(year).reindex(t.state_po).to_numpy()
        # Split each state's error into a shared division part and its own part
        reg = np.minimum(region_sd(hist[hist.year < year], b), 0.9 * race_sd.to_numpy())
        own = np.sqrt(race_sd.to_numpy() ** 2 - reg ** 2)
        div = pd.Categorical(t.state_po.map(DIVISION), categories=list(DIVISIONS)).codes
        z = rng.normal(0, 1, sm.N_SIMS)[:, None]
        zr = rng.normal(0, 1, (sm.N_SIMS, len(DIVISIONS)))[:, div]
        eps = rng.normal(0, 1, (sm.N_SIMS, len(t)))
        wins = (t.pred.to_numpy() + z * year_sd.to_numpy() + zr * reg + eps * own) > 0.5
        evs = wins.astype(int) @ ev
        actual = int(ev[(t.dem2 > 0.5).to_numpy()].sum())
        seats_by_year[year] = (evs, actual, WIN_EV)
        won = t.dem2 > 0.5
        print(f"\n{year}: blend " + ", ".join(f"{k} a {v['a']:.2f}" for k, v in b.items())
              + f"; {t.poll.notna().sum()}/{len(t)} states polled; regional sd {reg.max():.3f} "
              f"(state sd {race_sd.median():.3f})")
        for name, col, pcol in (("fundamentals", "fund", "p_fund"), ("blend", "pred", "p_dem")):
            print(f"  {name:12s} MAE {(t[col] - t.dem2).abs().mean():.3f}, mean err "
                  f"{(t[col] - t.dem2).mean():+.3f}, called {((t[pcol] > 0.5) == won).sum()}/{len(t)}, "
                  f"Brier {((t[pcol] - won) ** 2).mean():.3f}")
        print(f"  D electoral votes: median {np.median(evs):.0f}, 80% interval "
              f"{np.percentile(evs, 10):.0f}-{np.percentile(evs, 90):.0f}, actual {actual}; "
              f"P(D wins) {(evs >= WIN_EV).mean():.2f}")
        for r in t[(t.p_dem > 0.5) != won].itertuples():
            print(f"    missed {r.state_po}: pred {r.pred:.3f}, P(D) {r.p_dem:.2f}, actual {r.dem2:.3f}")
        out.append(t.assign(dem_cand=NOMINEES[year][0], rep_cand=NOMINEES[year][1], special=False, inc=0))
    bt = pd.concat(out)
    won = bt.dem2 > 0.5
    for name, pcol in (("fundamentals", "p_fund"), ("blend", "p_dem")):
        print(f"all backtests {name}: called {((bt[pcol] > 0.5) == won).sum()}/{len(bt)}, "
              f"Brier {((bt[pcol] - won) ** 2).mean():.3f}")
    sm.export_backtest("president", bt[["year", "state_po", "special", "dem_cand", "rep_cand", "inc", "lean",
                                        "fund", "poll", "n_polls", "pred", "p_fund", "p_dem", "dem2"]],
                       seats_by_year)


if __name__ == "__main__":
    main()
