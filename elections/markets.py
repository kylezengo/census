"""Snapshot prediction-market prices for the 2026 House and Senate races, as a
benchmark to score the forecast against after the election. Not an input.

Sources (public, read-only, no account needed):
- Kalshi: per-race party markets in series SENATE<st>, SENATEPARTY<st>,
  KXSENATE<st>, HOUSE<st><n> and KXHOUSE<st><nn>; 2026 events end in -26.
  Chamber control: series CONTROLS and CONTROLH, event <series>-2026.
- Polymarket: events tagged "midterms" titled "<State> Senate Election
  Winner", "<ST>-<NN> House Election Winner" and "Which party will win the
  <chamber> in 2026?".

Each race's market probability is 1 - P(Republican wins), matching the
forecast's anti-Republican framing (independents in NE, MT, ID, SD count on
the Democratic side). A market's price is its bid-ask midpoint when the spread
is at most 10 cents, else its last trade.

Run before the election (prices collapse to 0 or 1 after). Each run saves the
raw responses under raw/markets/<date>/ and a tidy table at
data/markets_<date>.csv, then prints the comparison with the forecast.
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent))
import senate_model as sm  # noqa: E402

RAW = Path(__file__).parent / "raw" / "markets"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
POLY = "https://gamma-api.polymarket.com/events"
UA = {"User-Agent": "census-research/0.1 (personal research script)"}
STATES = pd.read_csv(Path(__file__).parent / "raw" / "1976-2024-senate-state.csv")[
    ["state", "state_po"]].drop_duplicates()
STATE_PO = dict(zip(STATES.state.str.title(), STATES.state_po))


def get(url, params=None):
    for _ in range(6):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=60)
        except requests.ConnectionError:
            time.sleep(10)
            continue
        if r.status_code != 429:
            r.raise_for_status()
            return r.json()
        time.sleep(int(r.headers.get("retry-after", 5)) + 1)
    raise RuntimeError(f"rate-limited: {url}")


def price(bid, ask, last):
    """Bid-ask midpoint when the spread is tight, else the last trade."""
    bid, ask, last = (float(x) if x not in (None, "") else np.nan for x in (bid, ask, last))
    if np.isfinite(bid) and np.isfinite(ask) and ask - bid <= 0.10:
        return (bid + ask) / 2
    return last


def kalshi_series():
    series = get(f"{KALSHI}/series", {"category": "Elections"})["series"]
    out = []
    for s in series:
        t = s["ticker"]
        if m := re.fullmatch(r"(?:KX)?SENATE(?:PARTY)?([A-Z]{2})", t):
            out.append((t, "senate", m.group(1), None))
        elif m := re.fullmatch(r"(?:KX)?HOUSE([A-Z]{2})(\d+|AL)", t):
            out.append((t, "house", m.group(1), 0 if m.group(2) == "AL" else int(m.group(2))))
    return out + [("CONTROLS", "control_senate", "US", None), ("CONTROLH", "control_house", "US", None)]


def kalshi(raw_dir):
    rows, raw = [], {}
    for ticker, chamber, st, dist in kalshi_series():
        if st != "US" and st not in STATE_PO.values():
            continue  # e.g. KXSENATEMID: Michigan Democratic primary, not a state code
        markets, cursor = [], None
        while True:
            d = get(f"{KALSHI}/markets", {"series_ticker": ticker, "status": "open", "limit": 1000,
                                         **({"cursor": cursor} if cursor else {})})
            markets += d.get("markets", [])
            cursor = d.get("cursor")
            if not cursor:
                break
        markets = [m for m in markets if m["event_ticker"].endswith(("-26", "-2026"))]
        raw[ticker] = markets
        time.sleep(0.3)
        for m in markets:
            text = f"{m.get('title', '')} {m.get('yes_sub_title', '')} {m['ticker']}"
            party = ("R" if re.search(r"Republican|-R$|\(R\)", text)
                     else "D" if re.search(r"Democrat|-D$|\(D\)", text) else "other")
            rows.append({"source": "kalshi", "chamber": chamber, "state_po": st, "district": dist,
                         "event": m["event_ticker"], "market": m["ticker"], "party": party,
                         "candidate": m.get("yes_sub_title"),
                         "bid": m.get("yes_bid_dollars"), "ask": m.get("yes_ask_dollars"),
                         "last": m.get("last_price_dollars"),
                         "volume": float(m.get("volume_fp") or 0)})
    (raw_dir / "kalshi.json").write_text(json.dumps(raw))
    return rows


def polymarket(raw_dir):
    events, off = [], 0
    while True:
        d = get(POLY, {"tag_slug": "midterms", "closed": "false", "limit": 100, "offset": off})
        events += d
        off += 100
        if len(d) < 100:
            break
        time.sleep(0.5)
    (raw_dir / "polymarket.json").write_text(json.dumps(events))
    rows = []
    for e in events:
        title = e["title"].strip()
        if m := re.fullmatch(r"(.+) Senate Election Winner", title):
            chamber, st, dist = "senate", STATE_PO.get(m.group(1).strip()), None
        elif m := re.fullmatch(r"([A-Z]{2})-(\d{1,2}|AL) House Election Winner", title):
            chamber, st = "house", m.group(1)
            dist = 0 if m.group(2) == "AL" else int(m.group(2))
        elif m := re.fullmatch(r"Which party will win the (Senate|House) in 2026\?", title):
            chamber, st, dist = "control_" + m.group(1).lower(), "US", None
        else:
            continue
        if st is None:
            continue
        for mk in e.get("markets", []):
            if mk.get("closed") or not mk.get("outcomePrices"):
                continue
            text = f"{mk.get('question', '')} {mk.get('groupItemTitle', '')}"
            party = ("R" if re.search(r"Republican|\(R\)", text)
                     else "D" if re.search(r"Democrat|\(D\)", text) else "other")
            yes = json.loads(mk["outcomePrices"])[0]
            rows.append({"source": "polymarket", "chamber": chamber, "state_po": st, "district": dist,
                         "event": e["slug"], "market": mk.get("slug"), "party": party,
                         "candidate": mk.get("groupItemTitle"), "bid": mk.get("bestBid"),
                         "ask": mk.get("bestAsk"), "last": mk.get("lastTradePrice", yes),
                         "volume": float(mk.get("volume") or 0)})
    return rows


def label_by_name(t):
    """Markets that name a candidate but not a party (Polymarket's Alaska race):
    the Republican is whoever shares our Republican nominee's surname."""
    sen = pd.DataFrame(sm.RACES_2026, columns=["state_po", "special", "inc", "holder", "dem_side",
                                              "rep", "ind"]).assign(chamber="senate", district=-1)
    hou = pd.read_csv(sm.DATA / "house_2026_races.csv").assign(chamber="house")
    noms = pd.concat([sen[["chamber", "state_po", "district", "rep"]],
                      hou[["chamber", "state_po", "district", "rep"]]])
    rep = {(r.chamber, r.state_po, r.district): sm.name_tokens(re.sub(r"\(.*?\)", "", str(r.rep)))
           for r in noms.itertuples()}
    for i in t.index[t.party == "other"]:
        nominee = rep.get((t.at[i, "chamber"], t.at[i, "state_po"], int(t.at[i, "district"])))
        cand = sm.name_tokens(str(t.at[i, "candidate"]))
        if nominee and cand and nominee[-1] in cand:
            t.at[i, "party"] = "R"
    return t


def race_probs(t):
    """Per source and race: 1 - P(Republican), from the Republican market with
    the most volume (Kalshi lists some races under two series)."""
    t = label_by_name(t.assign(district=t.district.fillna(-1).astype(int)))
    r = t[t.party == "R"].sort_values("volume", ascending=False)
    r = r.drop_duplicates(["source", "chamber", "state_po", "district"])
    r = r.assign(p_dem=1 - r.price)  # district -1: statewide
    return r[["source", "chamber", "state_po", "district", "p_dem", "volume"]]


def compare(probs):
    pv = probs.pivot_table(index=["chamber", "state_po", "district"], columns="source",
                           values="p_dem").reset_index()
    sen = pd.read_csv(sm.ROOT / "c_forecast_senate_2026.csv")[["state_po", "p_dem"]]
    hou = pd.read_csv(sm.ROOT / "c_forecast_house_2026.csv")[["state_po", "district", "p_dem"]]
    s = pv[pv.chamber == "senate"].drop(columns="district").merge(sen, on="state_po", how="right")
    h = pv[pv.chamber == "house"].merge(hou, on=["state_po", "district"], how="inner")
    for name, d in (("Senate", s), ("House", h)):
        for src in ("kalshi", "polymarket"):
            if src not in d:
                continue
            both = d[src].notna()
            print(f"{name} vs {src}: {both.sum()} races, mean |ours - market| "
                  f"{(d.p_dem - d[src])[both].abs().mean():.3f}, disagree on favorite in "
                  f"{((d.p_dem > 0.5) != (d[src] > 0.5))[both].sum()}")
    cols = [c for c in ("kalshi", "polymarket") if c in s]
    print(s.assign(gap=s.p_dem - s[cols].mean(axis=1)).sort_values("p_dem", ascending=False)[
        ["state_po", "p_dem", *cols, "gap"]].round(2).to_string(index=False))
    ctrl = probs[probs.chamber.str.startswith("control")]
    print("\nchamber control, P(Democrats):")
    print(ctrl[["source", "chamber", "p_dem", "volume"]].round(3).to_string(index=False))


def main():
    now = datetime.now(timezone.utc)
    raw_dir = RAW / now.strftime("%Y-%m-%d")
    raw_dir.mkdir(parents=True, exist_ok=True)
    t = pd.DataFrame(kalshi(raw_dir) + polymarket(raw_dir))
    t["price"] = [price(b, a, l) for b, a, l in zip(t.bid, t.ask, t["last"])]
    t["fetched_at"] = now.isoformat(timespec="seconds")
    t.to_csv(sm.DATA / f"markets_{now:%Y-%m-%d}.csv", index=False)
    probs = race_probs(t)
    print(probs.groupby(["source", "chamber"]).size().to_string())
    compare(probs)


if __name__ == "__main__":
    main()
