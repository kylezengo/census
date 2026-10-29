"""Senate forecast: backtest on the 2010-2024 elections, then forecast 2026.

Model, per race, on the Democratic share of the two-party vote:
    dem2 - 0.5 = national + b_lean * lean + b_inc * inc + error
- lean: state's Democratic two-party presidential share minus the nation's,
  from the last presidential election before the race.
- inc: +1 Democratic incumbent running, -1 Republican, 0 open. "Incumbent"
  means a candidate who won a Senate election in the state within the last six
  years, so appointees who haven't yet won (Moody, Husted 2026) count as open.
- national = c + k * house: house is the national House two-party Democratic
  share minus 0.5. Training years use the actual House vote; the forecast year
  uses the generic ballot in its place. Senate races move only about two-thirds
  as much as the House vote (k ~ 0.6-0.7), so k is fit, not assumed to be 1.

Training uses only elections (and generic-ballot misses) before the test
year, so the backtest never sees the future. Two error terms are simulated: one national (shared by every race
that year: how far the year strays from its House vote, plus the generic
ballot's miss on the House vote) and one per race.

The forecast averages 12 specifications: training window (16, 24 or all years),
c fit or fixed at 0, and the generic ballot raw or shifted by its historical
miss. The 2010-2022 backtest can't tell them apart, yet they disagree a lot on
2026, so the average is reported and each one is saved for scoring after the
election.

Reads data/elections_{senate,president,house}.csv (fetch_elections.py).
Incumbency needs six years of prior winners, so races start in 1982. Writes
data/senate_backtest.csv, data/senate_forecast_2026.csv (per race, averaged)
and data/senate_specs_2026.csv (per specification).
"""

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

DATA = Path(__file__).parent / "data"
ROOT = Path(__file__).parent.parent  # the app reads c_forecast_*.csv from here
FORECAST_DATE = "2026-10-28"  # when the inputs below (polls, candidates) were read
N_SIMS = 20000
rng = np.random.default_rng(0)

# Generic ballot, two-party Democratic share. Backtest years: final
# RealClearPolitics average (2010 D 41.3 / R 50.7, 2014 43.2 / 45.6,
# 2018 49.7 / 42.4, 2022 45.5 / 48.0; presidential years from Wikipedia's
# House election pages: 2016 46.0 / 45.4, 2020 49.3 / 42.5, 2024 46.8 / 47.6
# as of 10/29; 2012 was a tie per FiveThirtyEight). 2026: RCP average
# 10/9-10/26, D 51.6 / R 43.6; other aggregators range D+7.6 to D+9.
GENERIC_BALLOT = {
    2010: 41.3 / (41.3 + 50.7),
    2012: 0.5,
    2014: 43.2 / (43.2 + 45.6),
    2016: 46.0 / (46.0 + 45.4),
    2018: 49.7 / (49.7 + 42.4),
    2020: 49.3 / (49.3 + 42.5),
    2022: 45.5 / (45.5 + 48.0),
    2024: 46.8 / (46.8 + 47.6),
    2026: 51.6 / (51.6 + 43.6),
}
BACKTEST = tuple(range(2010, 2026, 2))
# Actual House margin minus final RCP generic ballot, 2002-2020 (+ = Democrats
# beat their polls), per John McCormack. Halved to convert margin to two-party share.
GB_MISSES = dict(zip(range(2002, 2022, 2),
                     np.array([-2.9, -2.6, -3.6, 1.7, 2.6, 1.4, -3.3, -1.7, 1.1, -3.7]) / 100 / 2))
# Democratic caucus seats after each election (independents who caucus with
# them included; 2020 counts the January 2021 Georgia runoffs), for backtest
# seat totals. Control needs 50 under a Democratic Vice President, else 51.
D_CAUCUS = {2010: 53, 2012: 55, 2014: 46, 2016: 48, 2018: 47, 2020: 50, 2022: 51, 2024: 47}
DEM_VP = {2010, 2012, 2014, 2020, 2022}


def gb_misses(year):
    """Generic-ballot misses known before an election, so the backtest never
    sees later ones."""
    return np.array([v for y, v in GB_MISSES.items() if y < year])


def gb_bias(year):
    """Mean known miss, 0 before any are known."""
    m = gb_misses(year)
    return float(m.mean()) if len(m) else 0.0


def gb_var(year):
    m = gb_misses(year)
    return float(m.var()) if len(m) else 0.0

# (training window in years, fit intercept c, shift generic ballot by its mean miss)
SPECS = [(w, c, b) for w in (16, 24, 99) for c in (True, False) for b in (False, True)]

# Independents who ran as the de facto Democrat (endorsed by, or unopposed by,
# a serious Democrat, or caucusing with Democrats). (year, state) -> name fragment.
DEM_ALIGNED = {
    (2006, "VT"): "SANDERS", (2012, "VT"): "SANDERS", (2018, "VT"): "SANDERS",
    (2024, "VT"): "SANDERS", (2012, "ME"): "KING", (2018, "ME"): "KING",
    (2024, "ME"): "KING", (2014, "KS"): "ORMAN", (2022, "UT"): "MCMULLIN",
    (2024, "NE"): "OSBORN",
}

# 2026 races (Wikipedia, "2026 United States Senate elections", read 2026-10-28).
# inc: +1 D incumbent running, -1 R incumbent running, 0 open or appointee.
# holder: party holding the seat now. ind: the anti-Republican candidate is an
# independent who won't count toward a Democratic majority.
RACES_2026 = [
    # state, special, inc, holder, dem-side candidate, rep candidate, ind
    ("AL", False, 0, "R", "Wess", "Moore", False),
    ("AK", False, -1, "R", "Peltola", "Sullivan", False),
    ("AR", False, -1, "R", "Shoffner", "Cotton", False),
    ("CO", False, 1, "D", "Hickenlooper", "Baisley", False),
    ("DE", False, 1, "D", "Coons", "Katz", False),
    ("FL", True, 0, "R", "Nixon", "Moody (appointed)", False),
    ("GA", False, 1, "D", "Ossoff", "Collins", False),
    ("ID", False, -1, "R", "Achilles (I)", "Risch", True),
    ("IL", False, 0, "D", "Stratton", "Tracy", False),
    ("IA", False, 0, "R", "Turek", "Hinson", False),
    ("KS", False, -1, "R", "Hamilton", "Marshall", False),
    ("KY", False, 0, "R", "Booker", "Barr", False),
    ("LA", False, 0, "R", "Davis", "Letlow", False),
    ("ME", False, -1, "R", "Jackson", "Collins", False),
    ("MA", False, 1, "D", "Markey", "Deaton", False),
    ("MI", False, 0, "D", "El-Sayed", "Rogers", False),
    ("MN", False, 0, "D", "Flanagan", "Tafoya", False),
    ("MS", False, -1, "R", "Colom", "Hyde-Smith", False),
    # Montana is three-way: independent Seth Bodnar polls well ahead of Democrat
    # Alani Bankhead, so he is the anti-Republican candidate here.
    ("MT", False, 0, "R", "Bodnar (I)", "Alme", True),
    ("NE", False, -1, "R", "Osborn (I)", "Ricketts", True),
    ("NH", False, 0, "D", "Pappas", "Sununu", False),
    ("NJ", False, 1, "D", "Booker", "Murphy", False),
    ("NM", False, 1, "D", "Lujan", "Marker", False),
    ("NC", False, 0, "R", "Cooper", "Whatley", False),
    ("OH", True, 0, "R", "Brown", "Husted (appointed)", False),
    ("OK", False, 0, "R", "Thomas", "Hern", False),
    ("OR", False, 1, "D", "Merkley", "Smith", False),
    ("RI", False, 1, "D", "Reed", "McKay", False),
    ("SC", False, 0, "R", "Andrews", "D. Graham (appointed)", False),
    ("SD", False, -1, "R", "Bengs (I)", "Rounds", True),
    ("TN", False, -1, "R", "Bradshaw", "Hagerty", False),
    ("TX", False, 0, "R", "Talarico", "Paxton", False),
    ("VA", False, 1, "D", "Warner", "Mizusawa", False),
    ("WV", False, -1, "R", "Anderson", "Capito", False),
    ("WY", False, 0, "R", "Byrd", "Hageman", False),
]
# Seats not up in 2026: 47 D-caucus minus 13 up, 53 R minus 22 up. Vice
# President Vance breaks ties, so Democrats need 51.
D_NOT_UP, R_NOT_UP = 34, 31


def name_tokens(name):
    """Name words, for matching a candidate across years. Order varies by year:
    "RON WYDEN", "WYDEN RON", "MURKOWSKI, LISA"."""
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().upper()
    if "," in name:
        last, first = name.split(",", 1)
        name = f"{first} {last}"
    name = re.sub(r"\b(JR|SR|II|III|IV)\b|[^A-Z ]", " ", name)
    return [p for p in name.split() if len(p) > 1]


def same_person(a, b):
    """Either name's last word appears in the other. First names alone don't match."""
    return bool(a and b) and (a[-1] in b or b[-1] in a)


def presidential_lean():
    p = pd.read_csv(DATA / "elections_president.csv")
    p = p[p.party.isin(["DEM", "REP"])]
    st = p.pivot_table(index=["year", "state_po"], columns="party", values="votes", aggfunc="sum")
    nat = st.groupby("year").sum()
    lean = st.DEM / (st.DEM + st.REP) - (nat.DEM / (nat.DEM + nat.REP)).reindex(st.index, level=0)
    return lean.rename("lean").reset_index()


def lean_before(lean, year, state):
    prior = lean[(lean.year < year) & (lean.state_po == state)]
    return prior.sort_values("year").lean.iloc[-1] if len(prior) else np.nan


def senate_races():
    s = pd.read_csv(DATA / "elections_senate.csv")
    key = ["year", "state_po", "special"]
    s["side"] = s.party
    for (y, st), frag in DEM_ALIGNED.items():
        s.loc[(s.year == y) & (s.state_po == st) & s.candidate.str.contains(frag), "side"] = "DEM"

    # Winners, for incumbency: the runoff decides where there was one. GA's
    # January 2021 runoffs are filed under 2021.
    final = s[s.stage != "pre"].copy()
    final["race_year"] = final.year.where(final.year != 2021, 2020)
    last_stage = final.groupby(["race_year", "state_po", "special"]).stage.transform(
        lambda x: "runoff" if (x == "runoff").any() else "gen")
    final = final[final.stage == last_stage]
    winners = final.loc[final.groupby(["race_year", "state_po", "special"]).votes.idxmax()]
    winners = winners.assign(tokens=winners.candidate.map(name_tokens))

    g = s[s.stage == "gen"]
    rows = []
    for (year, st, special), d in g.groupby(key):
        top2 = d.nlargest(2, "votes")
        dem = d.loc[d.side == "DEM", "votes"].sum()
        rep = d.loc[d.side == "REP", "votes"].sum()
        third = d.loc[d.side == "OTHER", "share"].max()
        excluded = (dem == 0 or rep == 0 or top2.side.nunique() < 2
                    or (third if pd.notna(third) else 0) > 0.15)
        prior = winners[(winners.state_po == st) & (winners.race_year < year)
                        & (winners.race_year >= year - 6)]
        inc = 0
        for side, sign in (("DEM", 1), ("REP", -1)):
            won = prior.loc[prior.side == side, "tokens"]
            running = d.loc[d.side == side, "candidate"].map(name_tokens)
            if any(same_person(a, b) for a in running for b in won):
                inc += sign
        w = winners[(winners.race_year == year) & (winners.state_po == st) & (winners.special == special)]
        lead = {side: d.loc[d.side == side].nlargest(1, "votes").candidate for side in ("DEM", "REP")}
        rows.append(dict(year=year, state_po=st, special=special, dem2=dem / (dem + rep) if dem + rep else np.nan,
                         inc=inc, excluded=excluded, dem_won=bool((w.side == "DEM").any()),
                         dem_cand=lead["DEM"].iloc[0] if len(lead["DEM"]) else "",
                         rep_cand=lead["REP"].iloc[0] if len(lead["REP"]) else ""))
    return pd.DataFrame(rows)


def house_vote():
    """National House two-party Democratic share minus 0.5, by year."""
    h = pd.read_csv(DATA / "elections_house.csv")
    h = h[(h.stage == "gen") & ~h.special & (h.state_po != "DC")]
    nat = h.groupby(["year", "party"]).votes.sum().unstack()
    return nat.DEM / (nat.DEM + nat.REP) - 0.5


def export_for_app(chamber, races, seats):
    """Write the forecast where the Dash app reads it: c_forecast_<chamber>_2026.csv
    (one row per race) and c_forecast_<chamber>_seats_2026.csv (probability of
    each Democratic seat total, plus the inputs it was made with)."""
    races.to_csv(ROOT / f"c_forecast_{chamber}_2026.csv", index=False)
    dist = pd.Series(seats).value_counts(normalize=True).sort_index()
    pd.DataFrame({"seats": dist.index, "prob": dist.values, "as_of": FORECAST_DATE,
                  "generic_ballot_dem2": GENERIC_BALLOT[2026]}).to_csv(
        ROOT / f"c_forecast_{chamber}_seats_2026.csv", index=False)


def export_backtest(chamber, races, seats):
    """Write the backtest where the Dash app reads it: c_backtest_<chamber>_races.csv
    (one row per race and year) and c_backtest_<chamber>_seats.csv (probability of
    each Democratic seat total by year, with the actual total and the seats
    needed for control)."""
    races.to_csv(ROOT / f"c_backtest_{chamber}_races.csv", index=False)
    rows = []
    for year, (sims, actual, need) in seats.items():
        dist = pd.Series(sims).value_counts(normalize=True).sort_index()
        rows.append(pd.DataFrame({"year": year, "seats": dist.index, "prob": dist.values,
                                  "actual": actual, "need": need}))
    pd.concat(rows).to_csv(ROOT / f"c_backtest_{chamber}_seats.csv", index=False)


def fit(train, intercept=True):
    """OLS of dem2 - 0.5 on the House vote, lean and inc. The error splits into
    a year part (the year's mean residual) and a race part."""
    cols = [train.house, train.lean, train.inc]
    if intercept:
        cols.insert(0, np.ones(len(train)))
    X = np.column_stack(cols)
    y = train.dem2 - 0.5
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = pd.Series(y - X @ beta, index=train.index)
    year_mean = resid.groupby(train.year).transform("mean")
    race_sd = np.sqrt(((resid - year_mean) ** 2).sum() / (len(y) - X.shape[1]))
    year_sd = resid.groupby(train.year).mean().std(ddof=1)
    c, k, b_lean, b_inc = beta if intercept else (0.0, *beta)
    return dict(c=c, k=k, b_lean=b_lean, b_inc=b_inc, sd=race_sd, year_sd=year_sd)


def nat_sd(m, year):
    """National error: the year's own deviation, plus the generic ballot's miss on
    the House vote scaled by k, in quadrature."""
    return float(np.sqrt(m["year_sd"] ** 2 + (m["k"] ** 2) * gb_var(year)))


def mean_pred(test, m, house):
    return 0.5 + m["c"] + m["k"] * house + m["b_lean"] * test.lean + m["b_inc"] * test.inc


def gb_house(year, shift):
    """Generic ballot as a stand-in for the House vote, optionally shifted by
    its average historical miss."""
    return GENERIC_BALLOT[year] - 0.5 + (gb_bias(year) if shift else 0.0)


def simulate(f, m, house, n):
    """Democratic wins per race in 2026, (n, races) booleans."""
    nat = rng.normal(0, nat_sd(m, 2026), n)[:, None]
    race = rng.normal(0, m["sd"], (n, len(f)))
    return (mean_pred(f, m, house).to_numpy() + nat + race) > 0.5


def main():
    lean = presidential_lean()
    races = senate_races()
    races["lean"] = [lean_before(lean, y, s) for y, s in zip(races.year, races.state_po)]
    house = house_vote()
    races["house"] = races.year.map(house)
    usable = races[~races.excluded & races.lean.notna() & (races.year >= 1982)]

    # Backtest: each spec's probabilities, averaged
    out = []
    for year in BACKTEST:
        test = usable[usable.year == year].copy()
        preds, probs = [], []
        for window, intercept, shift in SPECS:
            m = fit(usable[(usable.year < year) & (usable.year >= year - window)], intercept)
            mu = mean_pred(test, m, gb_house(year, shift))
            preds.append(mu)
            probs.append(norm.cdf((mu - 0.5) / np.sqrt(m["sd"] ** 2 + nat_sd(m, year) ** 2)))
        test["pred"], test["p_dem"] = np.mean(preds, axis=0), np.mean(probs, axis=0)
        out.append(test)
        n, won = len(test), test.dem2 > 0.5
        print(f"{year}: {n} races, MAE {np.abs(test.pred - test.dem2).mean():.3f}, "
              f"mean error {(test.pred - test.dem2).mean():+.3f}, called {((test.p_dem > 0.5) == won).sum()}/{n}, "
              f"Brier {((test.p_dem - won) ** 2).mean():.3f}, D seats expected {test.p_dem.sum():.1f}, "
              f"actual {won.sum()}")
        for r in test[(test.p_dem > 0.5) != won].itertuples():
            print(f"    missed {r.state_po}{' (sp)' if r.special else ''}: pred {r.pred:.3f}, "
                  f"P(D) {r.p_dem:.2f}, actual {r.dem2:.3f}")
    bt = pd.concat(out)
    bt.to_csv(DATA / "senate_backtest.csv", index=False)
    print(f"all backtests: {len(bt)} races, called {((bt.p_dem > 0.5) == (bt.dem2 > 0.5)).sum()}, "
          f"Brier {((bt.p_dem - (bt.dem2 > 0.5)) ** 2).mean():.3f}")
    bins = pd.cut(bt.p_dem, [0, 0.1, 0.3, 0.5, 0.7, 0.9, 1])
    print(bt.groupby(bins, observed=True).agg(n=("p_dem", "size"), mean_p=("p_dem", "mean"),
                                              dem_won=("dem2", lambda x: (x > 0.5).mean())).round(2))

    # 2026: equal-weight pool of every spec's simulations
    f = pd.DataFrame(RACES_2026, columns=["state_po", "special", "inc", "holder", "dem_side", "rep", "ind"])
    f["lean"] = [lean_before(lean, 2026, s) for s in f.state_po]
    ind = f.ind.to_numpy()
    wins, preds, specs = [], [], []
    for window, intercept, shift in SPECS:
        m = fit(usable[usable.year >= 2026 - window], intercept)
        w = simulate(f, m, gb_house(2026, shift), N_SIMS)
        seats = D_NOT_UP + w[:, ~ind].sum(axis=1)
        wins.append(w)
        preds.append(mean_pred(f, m, gb_house(2026, shift)))
        specs.append(dict(window=window, intercept=intercept, gb_shift=shift, k=m["k"], c=m["c"],
                          b_lean=m["b_lean"], b_inc=m["b_inc"], race_sd=m["sd"], nat_sd=nat_sd(m, 2026),
                          median_seats=np.median(seats), p_51=(seats >= 51).mean()))
    w = np.vstack(wins)
    seats = D_NOT_UP + w[:, ~ind].sum(axis=1)
    f["pred"], f["p_dem"] = np.mean(preds, axis=0), w.mean(axis=0)
    f.to_csv(DATA / "senate_forecast_2026.csv", index=False)
    sp = pd.DataFrame(specs)
    sp.to_csv(DATA / "senate_specs_2026.csv", index=False)

    print(f"\n2026, generic ballot two-party D {GENERIC_BALLOT[2026]:.3f}, {len(SPECS)} specs averaged")
    print(sp.round(3).to_string(index=False))
    print(f.sort_values("p_dem", ascending=False)[["state_po", "dem_side", "rep", "lean", "inc", "pred", "p_dem"]]
          .round(3).to_string(index=False))
    print(f"\nDemocratic caucus seats: median {np.median(seats):.0f}, 80% interval "
          f"{np.percentile(seats, 10):.0f}-{np.percentile(seats, 90):.0f}")
    print(f"P(Democrats reach 51): {(seats >= 51).mean():.2f} (specs range "
          f"{sp.p_51.min():.2f}-{sp.p_51.max():.2f})")
    print(f"expected independent wins (ID, NE, SD): {w[:, ind].sum(axis=1).mean():.2f}")


if __name__ == "__main__":
    main()
