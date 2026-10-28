"""MIT Election Lab returns: US Senate and President by state, US House by district.

-> data/elections_senate.csv     one row per candidate per race, 1976-2024
-> data/elections_president.csv  same, by state
-> data/elections_house.csv      same, by district (only if the raw file is present)

Senate and President download directly from Harvard Dataverse. The House file
sits behind a Dataverse guestbook, so it has to be fetched by hand once from
https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2
and saved as raw/1976-2024-house.tab (or .csv).

Cleaning:
- Fusion candidates (NY, CT, ...) appear once per ballot line; their lines are
  summed and they take the major party among them.
- Blank, over-, under- and void votes and "none of these" are dropped, and the
  race total is recomputed from the remaining candidates. Scattered write-ins stay.
- stage is gen / runoff / pre. Louisiana's November jungle election is "gen".
- party is DEM / REP / OTHER from MIT's party_simplified. Independents who run
  with Democratic backing (King, Sanders, McMullin 2022, Osborn 2024) are OTHER
  here; aligning them is a modelling choice, not a data one.
"""

from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).parent
DATA, RAW = HERE / "data", HERE / "raw"
DATAVERSE = "https://dataverse.harvard.edu/api/access/datafile/{}?format=original"

# output name -> (Dataverse file id, raw filename)
DOWNLOADS = {
    "senate": (13887039, "1976-2024-senate-state.csv"),
    "president": (13887042, "1976-2024-president.csv"),
}
HOUSE_RAW = RAW / "1976-2024-house.tab"  # or .csv, the "original format" download

# Whole words only: candidates named Blankenship, Overby etc. must survive.
NON_VOTES = (
    r"^(?:BLANKS?\b|BLANK VOTES?\b|OVER ?VOTES?\b|UNDER ?VOTES?\b|VOID\b|VOID VOTES?\b"
    r"|NONE OF TH|EXHAUSTED BALLOTS?\b)"
)
PARTY = {"DEMOCRAT": "DEM", "REPUBLICAN": "REP"}
# The House file has only the detailed party. Minnesota's and North Dakota's
# Democrats and Minnesota's 1970s-90s Independent-Republicans are the major parties.
HOUSE_PARTY = {
    "DEMOCRAT": "DEM", "DEMOCRATIC": "DEM", "DEMOCRATIC-FARMER-LABOR": "DEM",
    "DEMOCRATIC-FARM-LABOR": "DEM", "DEMOCRATIC-NONPARTISAN LEAGUE": "DEM",
    "DEMOCRATIC-NPL": "DEM", "REPUBLICAN": "REP", "INDEPENDENT-REPUBLICAN": "REP",
    "REPUBLICAN, LIBERTARIAN": "REP",
}
# Major candidates that party_simplified codes OTHER, checked against certified
# results. Matching party_detailed by pattern instead would also sweep in
# write-ins who list a party (MD 2024 president).
OVERRIDES = {
    ("senate", 2000, "GA", "MACK F. MATTINGLY"): "REP",
    ("senate", 2008, "MS", "RONNIE MUSGROVE"): "DEM",  # nonpartisan special ballot
    ("senate", 2022, "IL", "TAMMY DUCKWORTH"): "DEM",
    ("senate", 2022, "MD", "CHRIS VAN HOLLEN"): "DEM",
    ("senate", 2020, "WY", "CYNTHIA M. LUMMIS"): "REP",
    ("senate", 2020, "WY", "MERAV BEN DAVID"): "DEM",
    ("senate", 2022, "AK", "MURKOWSKI, LISA"): "REP",
    ("senate", 2022, "AK", "TSHIBAKA, KELLY C."): "REP",
    ("senate", 2022, "AK", "CHESBRO, PATRICIA R."): "DEM",
    ("senate", 2024, "MS", "ROGER F. WICKER"): "REP",
    ("senate", 2024, "MS", "TY PINKINS"): "DEM",
    ("senate", 2024, "NV", "JACKY S. ROSEN"): "DEM",
    ("senate", 2024, "NV", "SAM BROWN"): "REP",
}


def download(file_id, name):
    path = RAW / name
    if not path.exists():
        r = requests.get(DATAVERSE.format(file_id), timeout=300)
        r.raise_for_status()
        if r.content.lstrip().startswith(b"{"):  # JSON error, e.g. guestbook required
            raise SystemExit(f"Dataverse refused file {file_id}: {r.text[:200]}")
        path.write_bytes(r.content)
    return path


def clean(df, office):
    df = df.copy()
    df["stage"] = (
        df.get("stage", pd.Series("gen", index=df.index))
        .str.lower()
        .replace({"gen runoff": "runoff", "pri": "pre"})
    )
    if "runoff" in df:  # House: runoffs are flagged, not staged
        df.loc[df["runoff"].eq(True), "stage"] = "runoff"
    df["special"] = df.get("special", False)
    df["district"] = df.get("district", "statewide")
    df["unofficial"] = df.get("unofficial", False)
    df["candidate"] = df["candidate"].fillna("SCATTERING").str.strip()
    df = df[~df["candidate"].str.contains(NON_VOTES, regex=True)]
    if "party_simplified" in df:
        df["party"] = df["party_simplified"].map(PARTY)
    else:
        df["party"] = df["party"].map(HOUSE_PARTY)
    for (off, year, st, cand), party in OVERRIDES.items():
        if off == office:
            df.loc[(df.year == year) & (df.state_po == st) & (df.candidate == cand), "party"] = party
    df["party"] = df["party"].fillna("OTHER")

    race = ["year", "state_po", "state_fips", "district", "special", "stage"]
    # A fusion candidate's major-party line decides their party; DEM and REP
    # never fuse onto one candidate in this data.
    rank = df["party"].map({"DEM": 0, "REP": 0, "OTHER": 1})
    df = df.assign(_rank=rank).sort_values("_rank")
    out = df.groupby(race + ["candidate"], as_index=False).agg(
        party=("party", "first"),
        votes=("candidatevotes", "sum"),
        writein=("writein", "all"),
        unofficial=("unofficial", "any"),
    )
    out["totalvotes"] = out.groupby(race)["votes"].transform("sum")
    out["share"] = out["votes"] / out["totalvotes"]
    out.insert(0, "office", office)
    return out.sort_values(race + ["votes"], ascending=[True] * len(race) + [False])


def main():
    DATA.mkdir(exist_ok=True)
    RAW.mkdir(exist_ok=True)
    for office, (file_id, name) in DOWNLOADS.items():
        df = pd.read_csv(download(file_id, name), low_memory=False)
        out = clean(df, office)
        out.to_csv(DATA / f"elections_{office}.csv", index=False)
        print(f"{office}: {len(out):,} rows, {out.year.min()}-{out.year.max()}")

    house = next((p for p in (HOUSE_RAW, HOUSE_RAW.with_suffix(".csv")) if p.exists()), None)
    if house:
        df = pd.read_csv(house, sep=None, engine="python")
        out = clean(df, "house")
        out.to_csv(DATA / "elections_house.csv", index=False)
        print(f"house: {len(out):,} rows, {out.year.min()}-{out.year.max()}")
    else:
        print(f"house: skipped, {HOUSE_RAW} not found (see module docstring)")


if __name__ == "__main__":
    main()
