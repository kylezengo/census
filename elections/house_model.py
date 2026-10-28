"""House forecast: backtest on the 2010-2022 midterms, then forecast 2026.

Same shape as senate_model.py, per district:
    dem2 - 0.5 = c + house + b_lean * lean + b_inc * inc + error
- lean: the district's presidential lean on that year's lines (house_lean.csv).
- inc: +1 Democratic incumbent running, -1 Republican, 0 open. Historically,
  a candidate who won a House seat in the same state two years earlier, so
  incumbents who moved districts in redistricting still count.
- house: national House two-party Democratic share minus 0.5. Its coefficient
  is fixed at 1, since the national vote is the sum of the districts; the
  district lean data only starts in 2006, too few years to fit it.

Races with no Democrat or no Republican, two candidates of the same party in
the top two, or a third candidate above 15% are left out of fitting and scoring;
in the seat count they go to the party that won (backtest) or is running (2026).

The forecast averages 4 specifications: c fit or fixed at 0, and the generic
ballot raw or shifted by its historical miss.

Louisiana's 2026 House general is on December 12, so its seats score later.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).parent))
from senate_model import (  # noqa: E402
    GB_MISSES, GENERIC_BALLOT, export_for_app, house_vote, name_tokens, same_person,
)

DATA = Path(__file__).parent / "data"
SPECS = [(c, b) for c in (True, False) for b in (False, True)]
MAJORITY = 218
N_SIMS = 20000
rng = np.random.default_rng(0)


def house_races():
    h = pd.read_csv(DATA / "elections_house.csv")
    h = h[~h.special & (h.state_po != "DC") & (h.stage != "pre")]
    key = ["year", "state_po", "district"]

    # Winners, for incumbency: the runoff decides where there was one
    last = h.groupby(key).stage.transform(lambda x: "runoff" if (x == "runoff").any() else "gen")
    final = h[h.stage == last]
    winners = final.loc[final.groupby(key).votes.idxmax()]
    winners = winners.assign(tokens=winners.candidate.map(name_tokens))
    prior_by = {k: d for k, d in winners.groupby(["year", "state_po"])}

    rows = []
    for (year, st, dist), d in h[h.stage == "gen"].groupby(key):
        dem = d.loc[d.party == "DEM", "votes"].sum()
        rep = d.loc[d.party == "REP", "votes"].sum()
        top2 = d.nlargest(2, "votes")
        third = d.loc[d.party == "OTHER", "share"].max()
        contested = (dem > 0 and rep > 0 and top2.party.nunique() == 2
                     and (third if pd.notna(third) else 0) <= 0.15)
        w = winners[(winners.year == year) & (winners.state_po == st) & (winners.district == dist)]
        prior = prior_by.get((year - 2, st))
        inc = 0
        if prior is not None:
            for side, sign in (("DEM", 1), ("REP", -1)):
                won = prior.loc[prior.party == side, "tokens"]
                running = d.loc[d.party == side, "candidate"].map(name_tokens)
                if any(same_person(a, b) for a in running for b in won):
                    inc += sign
        rows.append({"year": year, "state_po": st, "district": dist, "contested": contested,
                     "dem2": dem / (dem + rep) if dem + rep else np.nan, "inc": inc,
                     "dem_won": bool((w.party == "DEM").any())})
    return pd.DataFrame(rows)


def fit(train, intercept=True):
    """OLS of dem2 - 0.5 - house on lean and inc (plus c)."""
    cols = [train.lean, train.inc]
    if intercept:
        cols.insert(0, np.ones(len(train)))
    X = np.column_stack(cols)
    y = train.dem2 - 0.5 - train.house
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = pd.Series(y - X @ beta, index=train.index)
    year_mean = resid.groupby(train.year).transform("mean")
    race_sd = np.sqrt(((resid - year_mean) ** 2).sum() / (len(y) - X.shape[1]))
    year_sd = resid.groupby(train.year).mean().std(ddof=1)
    c, b_lean, b_inc = beta if intercept else (0.0, *beta)
    return {"c": c, "b_lean": b_lean, "b_inc": b_inc, "sd": race_sd,
            "nat_sd": float(np.sqrt(year_sd**2 + np.var(GB_MISSES)))}


def mean_pred(df, m, house):
    return 0.5 + m["c"] + house + m["b_lean"] * df.lean + m["b_inc"] * df.inc


def gb_house(year, shift):
    return GENERIC_BALLOT[year] - 0.5 + (np.mean(GB_MISSES) if shift else 0.0)


def main():
    lean = pd.read_csv(DATA / "house_lean.csv")
    house = house_vote()
    races = house_races().merge(lean[["year", "state_po", "district", "lean"]], how="left")
    races["house"] = races.year.map(house)
    usable = races[races.contested & races.lean.notna()]
    print("races per year (contested with lean / all):",
          {y: f"{(usable.year == y).sum()}/{(races.year == y).sum()}" for y in sorted(races.year.unique())
           if y >= 2006})

    out = []
    for year in (2010, 2014, 2018, 2022):
        test = usable[usable.year == year].copy()
        preds, probs, seat_draws = [], [], []
        for intercept, shift in SPECS:
            m = fit(usable[(usable.year < year) & (usable.year >= 2006)], intercept)
            mu = mean_pred(test, m, gb_house(year, shift))
            preds.append(mu)
            probs.append(norm.cdf((mu - 0.5) / np.sqrt(m["sd"] ** 2 + m["nat_sd"] ** 2)))
            nat = rng.normal(0, m["nat_sd"], N_SIMS)[:, None]
            seat_draws.append(((mu.to_numpy() + nat + rng.normal(0, m["sd"], (N_SIMS, len(test)))) > 0.5).sum(1))
        test["pred"], test["p_dem"] = np.mean(preds, axis=0), np.mean(probs, axis=0)
        out.append(test)
        fixed = races[(races.year == year) & ~races.index.isin(test.index)]
        seats = np.concatenate(seat_draws) + fixed.dem_won.sum()
        actual = races[races.year == year].dem_won.sum()
        won = test.dem2 > 0.5
        print(f"\n{year}: {len(test)} contested, MAE {np.abs(test.pred - test.dem2).mean():.3f}, "
              f"mean error {(test.pred - test.dem2).mean():+.3f}, called {((test.p_dem > 0.5) == won).sum()}"
              f"/{len(test)}, Brier {((test.p_dem - won) ** 2).mean():.3f}")
        print(f"  D seats: median {np.median(seats):.0f}, 80% interval {np.percentile(seats, 10):.0f}-"
              f"{np.percentile(seats, 90):.0f}, actual {actual}; P(D majority) {(seats >= MAJORITY).mean():.2f}")
    bt = pd.concat(out)
    bt.to_csv(DATA / "house_backtest.csv", index=False)
    won = bt.dem2 > 0.5
    print(f"\nall backtests: {len(bt)} races, called {((bt.p_dem > 0.5) == won).sum()}, "
          f"Brier {((bt.p_dem - won) ** 2).mean():.3f}")
    bins = pd.cut(bt.p_dem, [0, 0.1, 0.3, 0.5, 0.7, 0.9, 1])
    print(bt.groupby(bins, observed=True).agg(n=("p_dem", "size"), mean_p=("p_dem", "mean"),
                                              dem_won=("dem2", lambda x: (x > 0.5).mean())).round(2))

    # 2026
    f = pd.read_csv(DATA / "house_2026_races.csv").merge(
        lean[lean.year == 2026][["state_po", "district", "lean"]], how="left")
    open_race = f.has_dem & f.has_rep
    fixed_dem = (~f.has_rep & f.has_dem).sum()
    print(f"\n2026: {open_race.sum()} contested, {fixed_dem} certain D, {(~f.has_dem).sum()} certain R")
    g = f[open_race]
    wins, preds, specs = [], [], []
    for intercept, shift in SPECS:
        m = fit(usable[usable.year >= 2006], intercept)
        mu = mean_pred(g, m, gb_house(2026, shift)).to_numpy()
        nat = rng.normal(0, m["nat_sd"], N_SIMS)[:, None]
        w = (mu + nat + rng.normal(0, m["sd"], (N_SIMS, len(g)))) > 0.5
        seats = w.sum(1) + fixed_dem
        wins.append(w)
        preds.append(mu)
        specs.append({"intercept": intercept, "gb_shift": shift, **{k: m[k] for k in ("c", "b_lean", "b_inc", "sd", "nat_sd")},
                      "median_seats": np.median(seats), "p_majority": (seats >= MAJORITY).mean()})
    w = np.vstack(wins)
    seats = w.sum(1) + fixed_dem
    f.loc[open_race, "pred"] = np.mean(preds, axis=0)
    f.loc[open_race, "p_dem"] = w.mean(0)
    f.loc[~open_race, "p_dem"] = f.loc[~open_race, "has_dem"].astype(float)
    f.to_csv(DATA / "house_forecast_2026.csv", index=False)
    export_for_app("house", f[["state_po", "district", "dem", "rep", "inc", "lean", "pred", "p_dem"]],
                   seats)
    sp = pd.DataFrame(specs)
    sp.to_csv(DATA / "house_specs_2026.csv", index=False)
    print(sp.round(3).to_string(index=False))
    print(f"\nDemocratic seats: median {np.median(seats):.0f}, 80% interval {np.percentile(seats, 10):.0f}-"
          f"{np.percentile(seats, 90):.0f}; P(D majority, {MAJORITY}+) {(seats >= MAJORITY).mean():.2f}")
    print(f"seats by rating: safe D {(f.p_dem > 0.95).sum()}, likely D {f.p_dem.between(0.8, 0.95).sum()}, "
          f"lean/tossup {f.p_dem.between(0.2, 0.8, inclusive='neither').sum()}, "
          f"likely R {f.p_dem.between(0.05, 0.2).sum()}, safe R {(f.p_dem < 0.05).sum()}")
    close = f[f.p_dem.between(0.35, 0.65)].sort_values("p_dem", ascending=False)
    print(f"\n{len(close)} closest races:")
    print(close[["state_po", "district", "dem", "rep", "inc", "lean", "pred", "p_dem"]].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
