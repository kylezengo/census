"""House model inputs: presidential lean by district, and the 2026 races.

-> data/house_lean.csv         year, state_po, district, pres_year, pres_dem2, lean
-> data/house_2026_races.csv   state_po, district, dem, rep, has_dem, has_rep, inc,
                               incumbents

Lean: The Downballot's presidential results by congressional district, on the
lines in force for each House election, from the last presidential election
before it (https://www.the-downballot.com/p/the-downballots-calculations-of-presidential).
Their sheets are exported as CSV into raw/downballot/. lean = district
two-party Democratic share minus the nation's.

2026 races: Wikipedia, "2026 United States House of Representatives elections",
one table per state. inc is +1 if a Democratic incumbent is on the ballot in the
district, -1 Republican, 0 otherwise (a new seat, or two incumbents of
opposite parties, which doesn't happen in 2026).
"""

import re
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent))
from senate_model import name_tokens  # noqa: E402

HERE = Path(__file__).parent
DATA, RAW = HERE / "data", HERE / "raw"
SHEETS = "https://docs.google.com/spreadsheets/d/{}/export?format=csv&gid={}"
WIKI = ("https://en.wikipedia.org/w/api.php?action=parse&page=2026_United_States_House_"
        "of_Representatives_elections&prop=wikitext&format=json&formatversion=2")
UA = {"User-Agent": "census-research/0.1 (personal research script)"}

# House election year -> (sheet id, gid, presidential year, Dem column, Rep column).
# Each sheet is on the lines used that year; the presidential year is the last
# one before the election. Column headers repeat across years, so they're
# picked by position after the "District"/"CD" column.
LEAN_SOURCES = {
    2006: ("1l7W130tPRF6dQ4JoqlQJSJexnEg4rZct-7kwDXqgoLE", "429358610", 2004, 5, 6),
    2008: ("1l7W130tPRF6dQ4JoqlQJSJexnEg4rZct-7kwDXqgoLE", "429358610", 2004, 5, 6),
    2010: ("1l7W130tPRF6dQ4JoqlQJSJexnEg4rZct-7kwDXqgoLE", "429358610", 2008, 3, 4),
    2012: ("1xn6nCNM97oFDZ4M-HQgoUT3X4paOiSDsRMSuxbaOBdg", "0", 2008, 5, 6),
    2014: ("1xn6nCNM97oFDZ4M-HQgoUT3X4paOiSDsRMSuxbaOBdg", "0", 2012, 3, 4),
    2016: ("1VfkHtzBTP5gf4jAu8tcVQgsBJ1IDvXEHjuMqYlOgYbA", "0", 2012, 5, 6),
    2018: ("1zLNAuRqPauss00HDz4XbTH2HqsCzMe0pR8QmD1K8jk8", "0", 2016, 3, 4),
    2020: ("1XbUXnI9OyfAuhP5P3vWtMuGc5UJlrhXbzZo3AwMuHtk", "0", 2016, 5, 6),
    2022: ("1CKngqOp8fzU22JOlypoxNsxL6KSAH920Whc-rd7ebuM", "1871835782", 2020, 3, 4),
    2024: ("1ng1i_Dm_RMDnEvauH44pgE6JCUsapcuu8F2pCfeLWFo", "620838163", 2020, 6, 7),
    2026: ("1eZfaFI-c-PFOoKx1-zZA2MP0_dxRq_LVK0re3BOQqy0", "620838163", 2024, 3, 4),
}


def sheet(sheet_id, gid):
    path = RAW / "downballot" / f"{sheet_id[:8]}_{gid}.csv"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(SHEETS.format(sheet_id, gid), timeout=120)
        r.raise_for_status()
        path.write_bytes(r.content)
    return pd.read_csv(path, header=None, dtype=str)


def parse_district(code):
    """'AK-AL' -> ('AK', 0); 'TX-09' -> ('TX', 9)."""
    st, d = code.split("-")
    return st, 0 if d == "AL" else int(d)


def national_pres():
    p = pd.read_csv(DATA / "elections_president.csv")
    p = p[p.party.isin(["DEM", "REP"])].groupby(["year", "party"]).votes.sum().unstack()
    return p.DEM / (p.DEM + p.REP)


def lean_table():
    nat = national_pres()
    frames = []
    for year, (sid, gid, pres_year, dcol, rcol) in LEAN_SOURCES.items():
        df = sheet(sid, gid)
        df = df[df[0].str.match(r"^[A-Z]{2}-(?:AL|\d+)$", na=False)]
        dem = pd.to_numeric(df[dcol], errors="coerce")
        rep = pd.to_numeric(df[rcol], errors="coerce")
        st_d = df[0].map(parse_district)
        out = pd.DataFrame({"year": year, "state_po": st_d.str[0], "district": st_d.str[1],
                            "pres_year": pres_year, "pres_dem2": dem / (dem + rep)})
        out["lean"] = out.pres_dem2 - nat[pres_year]
        frames.append(out)
    return pd.concat(frames, ignore_index=True)


def clean_wiki(s):
    s = re.sub(r"<ref[^>]*/>", "", s)
    s = re.sub(r"<ref.*?</ref>", "", s, flags=re.S)
    return s


def link_text(s):
    s = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", s)
    return re.sub(r"\{\{.*?\}\}|'''", "", s).strip()


def same_name(a, b):
    """First and last names both match. Wikipedia writes names first-last, and
    last name alone is not enough: TX-22's Troy Nehls is succeeded on the
    ballot by his twin Trever."""
    a, b = name_tokens(a), name_tokens(b)
    return bool(a and b) and a[0] == b[0] and a[-1] == b[-1]


def races_2026():
    path = RAW / "house2026.wiki"
    if not path.exists():
        r = requests.get(WIKI, headers=UA, timeout=120)
        r.raise_for_status()
        path.write_text(r.json()["parse"]["wikitext"])
    t = path.read_text()
    body = clean_wiki(t[t.index("\n==Alabama=="):t.index("\n==Non-voting delegates==")])
    rows = []
    for block in re.split(r"\n!(?:rowspan=\d+ *\|)?\s*\{\{ushr\|", body)[1:]:
        st, dist = re.match(r"([A-Z]{2})\|([^|}]+)\|", block).groups()
        block = block.split("\n|}")[0]
        cands = []
        for stripe, rest in re.findall(r"^\*\s*\{\{Party stripe\|([^}]*)\}\}\s*(.+?)\s*$", block,
                                       flags=re.M | re.I):
            text = link_text(rest)
            # Side comes from the party-colour template, not the ballot label:
            # CA-6's Kevin Kiley is on the ballot as "Independent" but runs as
            # an Independent Republican.
            side = ("DEM" if re.search(r"Democratic|DFL", stripe) else
                    "REP" if "Republican" in stripe else "OTHER")
            cands.append((re.sub(r"\s*\([^)]*\)\s*$", "", text), side))
        # The incumbent cell is {{sortname|First|Last}} or, for some, a plain [[link]]
        incs = [((f"{first} {last}" if first else link_text(f"[[{link}]]")),
                 "DEM" if party.startswith(("Democratic", "DFL")) else "REP")
                for first, last, link, party in re.findall(
                    r"^\|\s*(?:\{\{sortname\|([^|}]+)\|([^|}]+)[^}]*\}\}|\[\[(?!\d)([^\]]+)\]\])[^\n]*"
                    r"\n\|\s*(?:style=\"[^\"]*\"|\{\{Party shading/[^}]*\}\})\s*\|\s*([\w-]+)",
                    block, flags=re.M)]
        inc = 0
        for name, side in incs:
            if any(s == side and same_name(c, name) for c, s in cands):
                inc += 1 if side == "DEM" else -1
        rows.append({
            "state_po": st, "district": 0 if dist == "AL" else int(dist),
            "dem": "; ".join(c for c, s in cands if s == "DEM"),
            "rep": "; ".join(c for c, s in cands if s == "REP"),
            "has_dem": any(s == "DEM" for _, s in cands),
            "has_rep": any(s == "REP" for _, s in cands),
            "inc": max(-1, min(1, inc)),
            "incumbents": "; ".join(f"{n} ({s[0]})" for n, s in incs),
        })
    return pd.DataFrame(rows)


def main():
    lean = lean_table()
    lean.to_csv(DATA / "house_lean.csv", index=False)
    print(f"lean: {len(lean):,} rows; per year:", lean.groupby("year").size().to_dict())
    races = races_2026()
    races.to_csv(DATA / "house_2026_races.csv", index=False)
    print(f"2026 races: {len(races)}; D incumbents running {(races.inc == 1).sum()}, "
          f"R {(races.inc == -1).sum()}, open {(races.inc == 0).sum()}; "
          f"no R candidate {(~races.has_rep).sum()}, no D candidate {(~races.has_dem).sum()}")


if __name__ == "__main__":
    main()
