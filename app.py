"""Census Data Explorer — interactive demographic visualization app."""

import os
from functools import lru_cache

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from dash import Dash, html, dcc, Input, Output, State, callback_context

# Static config: metric lists, presets, colors, CPI table, labels.
from config import (
    _DECIMAL_FMT,
    _MAX_TREND_SERIES,
    _PCT_METRICS,
    ACS_YEAR,
    AGE_BRACKETS,
    AGE_PROFILE_HOUSEHOLDER_METRICS,
    AGE_PROFILE_METRICS,
    AGE_PROFILE_SEXES,
    CORR_METRIC_GROUPS,
    CPI,
    CENSUS_2000_YEAR,
    CPI_COLS,
    DEFAULT_VAR,
    BACKTEST_CHAMBERS,
    BACKTEST_VIEWS,
    DEM_COLOR,
    ELECTION_MAJORITY,
    ELECTION_VIEWS,
    MISS_COLOR,
    REP_COLOR,
    US_LABEL,
    FEMALE_COLOR,
    HOUSING_METRICS,
    HOUSING_RATE_Y,
    HOUSING_VIEWS,
    INCOME_COLOR,
    INCOME_YEAR,
    MALE_COLOR,
    METRIC_LABELS,
    MF_COLOR,
    NAEP_METRICS,
    NAEP_VIEWS,
    RACE_DEFAULTS,
    RACE_GROUPS,
    RATIO_BASELINES,
    SUGGESTED_AGE_PROFILES,
    SUGGESTED_ANIM_SCATTERS,
    SUGGESTED_HOUSING,
    SUGGESTED_SCATTERS,
    SUGGESTED_TRENDS,
    TIMESERIES_METRICS,
    TIMESERIES_AGE_METRICS,
    TIMESERIES_RACE_METRICS,
    TRENDS_VIEWS,
)

pio.templates.default = "plotly_white"

DEV_MODE = os.environ.get("DEV_MODE") == "true"


# Load files ###################################################################################
state_geom_raw = gpd.read_file("state_geom.shp")
county_geom_raw = gpd.read_file("county_geom.shp")
zcta_geom_raw = gpd.read_file("zcta_geom.shp")
congressional_district_geom_raw = gpd.read_file("congressional_district_geom.shp")
if DEV_MODE:
    tract_geom_raw = gpd.read_file("tract_geom.shp")
    block_group_geom_raw = gpd.read_file("block_group_geom.shp")

zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={"zcta": object})

dma_polygons_raw = gpd.read_file("dma_polygons.geojson")
dma_polygons_raw["cartodb_id"] = dma_polygons_raw["cartodb_id"].astype(str)
dma_polygons_raw["dma_code"] = dma_polygons_raw["dma_code"].astype(str)

dma_polygon_map = pd.read_csv("dma_polygon_map.csv")

def _read_acs(path, **kwargs):
    """Read an ACS CSV with float columns as float32.

    These are survey estimates with margins of error in the thousands, so
    float32 (~7 significant digits) is far more precision than the data
    carries. Halves the memory of the wide tables — c_zcta_dma alone drops
    from 134 MB to 70 MB. Dtypes are resolved from a small sample and applied
    during parsing, so the float64 copy is never materialised.
    """
    explicit = kwargs.pop("dtype", {})
    sample = pd.read_csv(path, nrows=1000, dtype=explicit or None)
    dtype = {
        c: "float32"
        for c in sample.select_dtypes("float64").columns
        if c not in explicit
    }
    dtype.update(explicit)
    return pd.read_csv(path, dtype=dtype, **kwargs)


c_state = _read_acs(f"c_state_{ACS_YEAR}.csv")
c_dma = _read_acs(f"c_dma_{ACS_YEAR}.csv")


def _read_ts(name, sort, dtype=None):
    """ACS 5-year series (2009+) with the decennial censuses prepended: 2000 from
    the long form, 1790-1990 from NHGIS.

    Categoricals are applied after the concat, since concatenating two
    categoricals with different categories falls back to object dtype. Sorted
    because px.line draws points in row order.
    """
    dtype = dtype or {}
    plain = {c: t for c, t in dtype.items() if t != "category"}
    sources = ["nhgis", "census2000", "timeseries"]
    df = pd.concat(
        [_read_acs(f"c_{src}_{name}.csv", dtype=plain) for src in sources],
        ignore_index=True,
    )
    cats = {c: "category" for c, t in dtype.items() if t == "category"}
    return df.astype(cats).sort_values(sort, ignore_index=True)


ts_state = _read_ts("state", ["state", "year"])
ts_county = _read_ts("county", ["NAME", "year"], dtype={"GEOID": object})
# Categorical keys keep the long-format race frames small (463k county rows) and
# make the per-callback `isin` filters ~4x faster than object dtype.
_RACE_DTYPES = {"race": "category", "year": "int16"}
ts_state_race = _read_ts(
    "state_race", ["state", "race", "year"], dtype={**_RACE_DTYPES, "state": "category"}
)
ts_county_race = _read_ts(
    "county_race", ["NAME", "race", "year"], dtype={**_RACE_DTYPES, "NAME": "category"}
)
# NAEP scores join the state frames as extra columns. Its series starts in 1998,
# so the outer join adds rows that are empty for every census metric.
_naep_state = _read_acs("c_naep_state.csv")
ts_state = ts_state.merge(_naep_state, on=["state", "year"], how="outer").sort_values(
    ["state", "year"], ignore_index=True
)
# The cross-sectional state table (US map, scatter, correlation) takes the latest
# NAEP cycle at or before ACS_YEAR — NAEP is biennial, so it may lag a year.
_naep_year = _naep_state.loc[_naep_state["year"] <= ACS_YEAR, "year"].max()
c_state = c_state.merge(
    _naep_state[_naep_state["year"] == _naep_year].drop(columns="year"),
    on="state",
    how="left",
)
ts_state_race = (
    ts_state_race.merge(
        _read_acs("c_naep_state_race.csv", dtype={"year": "int16"}),
        on=["state", "year", "race"],
        how="outer",
    )
    .astype({"state": "category", "race": "category"})
    .sort_values(["state", "race", "year"], ignore_index=True)
)
_AGE_DTYPES = {"age": "category", "year": "int16"}
ts_state_age = _read_ts(
    "state_age", ["state", "age", "year"], dtype={**_AGE_DTYPES, "state": "category"}
)
ts_county_age = _read_ts(
    "county_age", ["NAME", "age", "year"], dtype={**_AGE_DTYPES, "NAME": "category"}
)
# Age Profile: long format (level x NAME x metric x sex x age -> value).
age_profile = _read_acs(
    f"c_age_profile_{ACS_YEAR}.csv",
    dtype={c: "category" for c in ["level", "NAME", "metric", "sex", "age"]},
)
AGE_PROFILE_LEVELS = ["State", "County", "Congressional District"]
# Housing: US + states by year, and metros (2020-24) with the regulation index.
housing_state = _read_acs("c_housing_state.csv", dtype={"state": "category", "year": "int16"})
housing_metro = _read_acs("c_housing_metro.csv", dtype={"cbsa": object})
# ACS 1-year 2005-2025 with that year's mortgage rate, for the payment view.
housing_payment = _read_acs("c_housing_payment.csv", dtype={"year": "int16"})
HOUSING_STATES = [US_LABEL] + sorted(
    s for s in housing_state["state"].astype(str).unique() if s != US_LABEL
)
# Elections: 2026 forecasts, one row per race and the seat-count distribution.
forecast_races = {c: pd.read_csv(f"c_forecast_{c}_2026.csv") for c in ELECTION_MAJORITY}
forecast_seats = {c: pd.read_csv(f"c_forecast_{c}_seats_2026.csv") for c in ELECTION_MAJORITY}
# Backtest 2010-2024: per race (forecast and actual) and per-year seat distributions.
backtest_races = {c: pd.read_csv(f"c_backtest_{c}_races.csv") for c in BACKTEST_CHAMBERS}
backtest_seats = {c: pd.read_csv(f"c_backtest_{c}_seats.csv") for c in BACKTEST_CHAMBERS}
_POLLED = ("senate", "president")  # backtests with a poll blend and a fundamentals-only version


def _backtest_years(chamber):
    return sorted(int(y) for y in backtest_seats[chamber]["year"].unique())


def _backtest_year_options(chamber):
    return [{"label": "All years", "value": "all"}] + [
        {"label": str(y), "value": y} for y in reversed(_backtest_years(chamber))]
c_county_state = _read_acs(f"c_county_state_{ACS_YEAR}.csv", dtype={"GEOID": object})
c_zcta_dma = _read_acs(f"c_zcta_dma_{ACS_YEAR}.csv", dtype={"zcta": object})
if DEV_MODE:
    c_tract = _read_acs(f"c_tract_{ACS_YEAR}.csv", dtype={"GEOID": object})
    c_block_group = _read_acs(f"c_block_group_{ACS_YEAR}.csv", dtype={"GEOID": object})
c_congressional_district = _read_acs(
    f"c_congressional_district_{ACS_YEAR}.csv", dtype={"GEOID": object}
)

state_name = pd.read_csv(f"state_name_{ACS_YEAR}.csv", dtype={"state": object})


def _add_price_to_rent(df):
    if "Median Home Value" in df.columns and "Median Gross Rent" in df.columns:
        df["price_to_rent_ratio"] = df["Median Home Value"] / (
            df["Median Gross Rent"] * 12
        )


_price_to_rent_dfs = [c_state, c_dma, c_county_state, c_zcta_dma, c_congressional_district, ts_state, ts_county]
if DEV_MODE:
    _price_to_rent_dfs += [c_tract, c_block_group]
for _df in _price_to_rent_dfs:
    _add_price_to_rent(_df)

# Set up the geographic geometry files #########################################################
state_geom = state_geom_raw[["NAME", "geometry"]].set_index("NAME")
state_geom_json = state_geom.to_json()

dma_geom = dma_polygons_raw.merge(
    dma_polygon_map, left_on="dma_name", right_on="DMA Polygons"
)
dma_geom = dma_geom[["DMA", "geometry"]].set_index("DMA")
dma_geom_json = dma_geom.to_json()

county_geom = county_geom_raw[["GEOID", "geometry"]].set_index("GEOID")

zcta_geom = zcta_geom_raw.merge(
    zcta_to_dma[["zcta", "dma"]], how="left", left_on="ZCTA5CE20", right_on="zcta"
)

if DEV_MODE:
    tract_geom = tract_geom_raw[["GEOID", "geometry"]]
    block_group_geom = block_group_geom_raw[["GEOID", "geometry"]]
congressional_district_geom = congressional_district_geom_raw[["GEOID", "geometry"]].set_index("GEOID")
congressional_district_geom_json = congressional_district_geom.to_json()

# Pre-compute GeoJSON per state/city at startup to avoid re-serializing on every callback
county_geom_by_state = {
    fips: county_geom[county_geom.index.str[:2] == fips].to_json()
    for fips in state_name["state"].unique()
}

_city_fips = {}
cities = []
tract_geom_by_state = {}
block_group_geom_by_city = {}
if DEV_MODE:
    tract_geom_by_state = {
        fips: tract_geom[tract_geom["GEOID"].str[:2] == fips].set_index("GEOID").to_json()
        for fips in state_name["state"].unique()
    }
    _city_fips = {
        "New York": ["36005", "36047", "36061", "36081", "36085"],
        "Los Angeles": ["06037"],
        "San Francisco": ["06075"],
    }
    block_group_geom_by_city = {
        city: block_group_geom[block_group_geom["GEOID"].str[:5].isin(fips)]
        .set_index("GEOID")
        .to_json()
        for city, fips in _city_fips.items()
    }
    cities = list(_city_fips.keys())

# Metric column lists ##########################################################################
state_metric_cols = sorted(col for col in c_state.columns if col != "state")
dma_metric_cols = sorted(col for col in c_dma.columns if col != "dma")
county_metric_cols = sorted(
    col
    for col in c_county_state.columns
    if col not in ["state", "county", "state_NAME", "GEOID", "NAME"]
)
zcta_metric_cols = sorted(
    col for col in c_zcta_dma.columns if col not in ["dma", "zcta"]
)
tract_metric_cols = []
block_group_metric_cols = []
if DEV_MODE:
    tract_metric_cols = sorted(
        col
        for col in c_tract.columns
        if col not in ["state", "county", "state_NAME", "GEOID", "NAME", "tract"]
    )
    block_group_metric_cols = sorted(
        col
        for col in c_block_group.columns
        if col not in ["state", "county", "state_NAME", "GEOID", "NAME", "tract", "block group"]
    )
congressional_district_metric_cols = sorted(
    col
    for col in c_congressional_district.columns
    if col not in ["state", "state_NAME", "GEOID", "NAME", "congressional district"]
)

dmas = c_dma["dma"].unique()
states = state_name["state_NAME"].unique()


TIMESERIES_GEOS = {
    "State": (ts_state, "state"),
    "County": (ts_county, "NAME"),
}

# Race-segmented series (long format: one row per geography x year x race)
TIMESERIES_RACE_GEOS = {
    "State": (ts_state_race, "state"),
    "County": (ts_county_race, "NAME"),
}

TIMESERIES_AGE_GEOS = {
    "State": (ts_state_age, "state"),
    "County": (ts_county_age, "NAME"),
}

# Segment modes beyond plain geography. Each is a long frame with one extra key
# column, so they share a single code path in the chart callback.
SEGMENTS = {
    "race": (TIMESERIES_RACE_GEOS, "race", "Race / Ethnicity"),
    "age": (TIMESERIES_AGE_GEOS, "age", "Householder Age"),
}


# Largest by the latest year — a mean over 1790-2024 would favour old-population states.
_latest = ts_state[ts_state["year"] == ts_state["year"].max()]
_ts_state_defaults = _latest.nlargest(4, "Pop")["state"].tolist()


# Scatter geography config: name → (dataframe, label_col, metric_cols)
SCATTER_GEOS = {
    "State": (c_state, "state", state_metric_cols),
    "DMA": (c_dma, "dma", dma_metric_cols),
    "County": (c_county_state, "NAME", county_metric_cols),
    "ZCTA": (c_zcta_dma, "zcta", zcta_metric_cols),
    "Congressional District": (
        c_congressional_district,
        "NAME",
        congressional_district_metric_cols,
    ),
}

CORR_GEOS = {
    "State": (c_state, state_metric_cols),
    "DMA": (c_dma, dma_metric_cols),
    "County": (c_county_state, county_metric_cols),
    "ZCTA": (c_zcta_dma, zcta_metric_cols),
    "Congressional District": (c_congressional_district, congressional_district_metric_cols),
}


_btn_style = {
    "padding": "5px 12px",
    "cursor": "pointer",
    "fontFamily": "Arial",
    "fontSize": "13px",
    "borderRadius": "4px",
    "border": "1px solid #ccc",
    "background": "#f8f8f8",
}


def _apply_cpi(df, year_col="year"):
    df = df.copy()
    for col in CPI_COLS:
        if col in df.columns:
            years = df[year_col]
            if col == "Median Household Income":
                years = years.replace(INCOME_YEAR)
            df[col] = df[col] * years.map(lambda y: CPI[2024] / CPI.get(y, CPI[2024]))
    return df


def _inflate_checkbox(component_id):
    return dcc.Checklist(
        id=component_id,
        options=[{"label": "  Adjust for inflation (2024 $)", "value": "inflate"}],
        value=[],
        inline=True,
        style={"fontFamily": "Arial", "marginTop": "10px", "fontSize": "13px"},
    )


# Helpers ######################################################################################
def _get_color(metric):
    if "Female" in metric:
        return FEMALE_COLOR
    if "pct_male" in metric:
        return MF_COLOR
    if "Income" in metric or "_ratio" in metric or "Median" in metric:
        return INCOME_COLOR
    if "Poverty" in metric or "pct_poverty" in metric or "pct_unemployed" in metric:
        return FEMALE_COLOR
    if "Education" in metric or "pct_bachelors" in metric:
        return "Purples"
    if "Housing" in metric or "pct_owner" in metric or "pct_renter" in metric:
        return "YlOrBr"
    if "Male" in metric:
        return MALE_COLOR
    if metric.startswith("pct_"):
        return "Oranges"
    return MALE_COLOR


def _metric_label(col):
    if col in METRIC_LABELS:
        return METRIC_LABELS[col]
    if col.startswith("pct_male_"):
        return f"% Male {col[len('pct_male_'):]}"
    if col.startswith("pct_female_"):
        return f"% Female {col[len('pct_female_'):]}"
    if col.startswith("pct_"):
        return f"% {col[4:].replace('_', ' ').title()}"
    return col


def _make_options(cols):
    return [{"label": _metric_label(c), "value": c} for c in cols]


def _age_profile_geos(level):
    names = age_profile.loc[age_profile["level"] == level, "NAME"].unique()
    names = sorted(map(str, names))
    # Keep the national row first at State level.
    if US_LABEL in names:
        names.remove(US_LABEL)
        names.insert(0, US_LABEL)
    return names


def _age_profile_sex_options(metric):
    """Householder metrics have no sex split, so disable everything but Both."""
    householder = metric in AGE_PROFILE_HOUSEHOLDER_METRICS
    options = []
    for k, v in AGE_PROFILE_SEXES.items():
        disabled = householder and k != "Both"
        # Browsers grey out a disabled radio but not its label text.
        label = html.Span(f"  {v}", style={"color": "#bbb"} if disabled else {})
        options.append({"label": label, "value": k, "disabled": disabled})
    return options


def _with_naep(metrics, geo_level):
    """NAEP only reports states, so its scores are offered at State level alone."""
    return metrics + NAEP_METRICS if geo_level == "State" else metrics


def _compute_trendline(df, x_metric, y_metric):
    """OLS trendline via numpy. Returns (x_line, y_line, slope, intercept, r2) or None."""
    clean = df[[x_metric, y_metric]].dropna()
    if len(clean) < 3:
        return None
    x = clean[x_metric].values.astype(float)
    y = clean[y_metric].values.astype(float)
    slope, intercept = np.polyfit(x, y, 1)
    r2 = float(np.corrcoef(x, y)[0, 1] ** 2)
    x_line = np.linspace(x.min(), x.max(), 100)
    return x_line, slope * x_line + intercept, slope, intercept, r2


def _fmt_coef(v):
    if abs(v) >= 10000:
        return f"{v:,.0f}"
    if abs(v) >= 100:
        return f"{v:.1f}"
    if abs(v) >= 1:
        return f"{v:.3f}"
    return f"{v:.4f}"


def _axis_fmt(metric):
    label = {"title": _metric_label(metric)}
    if metric in CPI_COLS:
        return {**label, "tickprefix": "$", "tickformat": ",.0f"}
    if metric.startswith("pct_") or metric in _PCT_METRICS:
        return {**label, "tickformat": ".0%"}
    if metric in _DECIMAL_FMT:
        return {**label, "tickformat": _DECIMAL_FMT[metric]}
    return label


def _apply_trends_view(plot_df, metric, series_col, view, baseline_map=None):
    """Rescale `metric` for the chosen view. Returns (df, y_title, y_axis_fmt).

    indexed — each series divided by its own earliest non-null value x 100
    ratio   — each row divided by the all-races baseline for its geography/year
    diff    — each row minus that same baseline
    """
    if view == "indexed":
        plot_df = plot_df.sort_values("year")
        first = plot_df.groupby(series_col)[metric].transform("first")
        plot_df = plot_df.assign(**{metric: plot_df[metric] / first * 100})
        return (
            plot_df,
            f"{_metric_label(metric)} (indexed, first year = 100)",
            {"tickformat": ",.0f"},
        )
    if view == "diff":
        plot_df = plot_df.assign(**{metric: plot_df[metric] - baseline_map})
        return (
            plot_df,
            f"{_metric_label(metric)} (difference from overall)",
            {"tickformat": "+" + _hover_fmt(metric)},
        )
    if view == "ratio":
        plot_df = plot_df.assign(**{metric: plot_df[metric] / baseline_map})
        return (
            plot_df,
            f"{_metric_label(metric)} (ratio to overall)",
            {"tickformat": ".2f"},
        )
    fmt = _axis_fmt(metric)
    return plot_df, fmt["title"], {**fmt, "tickformat": fmt.get("tickformat", _hover_fmt(metric))}


@lru_cache(maxsize=None)
def _keyed_baseline(geo_level, metric):
    """All-races value keyed by (geography, year). Cached — the frames never change."""
    all_df, all_name_col = TIMESERIES_GEOS[geo_level]
    return all_df.set_index([all_name_col, "year"])[metric]


@lru_cache(maxsize=None)
def _us_baseline(metric):
    """National all-races value by year, for the 'United States' ratio baseline."""
    return ts_state[ts_state["state"] == US_LABEL].set_index("year")[metric].to_dict()


def _source_label(year):
    """Hover label for a trends point: a decennial census, else the pooled ACS window."""
    if year in INCOME_YEAR:
        return f"Census {year} (income for {INCOME_YEAR[year]})"
    if year <= CENSUS_2000_YEAR:
        return f"Census {year}"
    return f"ACS {_acs5_window(year)} (5-yr)"


def _acs5_window(year):
    """ACS 5-year estimates are pooled: 2024 covers 2020-2024."""
    return f"{year - 4}–{str(year)[2:]}"


def _hover_fmt(metric):
    if metric in CPI_COLS:
        return "$,.0f"
    if metric.startswith("pct_") or metric in _PCT_METRICS:
        return ".1%"
    return _DECIMAL_FMT.get(metric, ",.0f")


def _hovertemplate(x_metric, y_metric, color_metric=None):
    lines = ["<b>%{hovertext}</b>"]
    lines.append(f"{_metric_label(x_metric)}: %{{x:{_hover_fmt(x_metric)}}}")
    lines.append(f"{_metric_label(y_metric)}: %{{y:{_hover_fmt(y_metric)}}}")
    if color_metric:
        lines.append(
            f"{_metric_label(color_metric)}: %{{marker.color:{_hover_fmt(color_metric)}}}"
        )
    lines.append("<extra></extra>")
    return "<br>".join(lines)


def _trunc_colorscale(name, low=0.30):
    """Drop the light end of sequential scales so low values stay visible.
    Diverging scales (RdBu) are already saturated at both ends — leave them alone."""
    if name == "RdBu":
        return name
    fracs = [low + (1 - low) * i / 9 for i in range(10)]
    sampled = px.colors.sample_colorscale(name, fracs)
    return [[i / 9, c] for i, c in enumerate(sampled)]


def _is_ratio(metric):
    """True for metrics that aren't counts, so '% of population' leaves them alone."""
    return (
        metric.startswith("pct_")
        or "_ratio" in metric
        or "Median" in metric
        or metric in NAEP_METRICS
    )


def _normalize_df(df, metrics):
    """Return copy of df with raw-count metrics expressed as % of Pop."""
    df = df.copy()
    for m in metrics:
        if not _is_ratio(m) and m in df.columns and "Pop" in df.columns:
            df[m] = (df[m] / df["Pop"] * 100).round(4)
    return df


def _normalize_checkbox(component_id):
    return dcc.Checklist(
        id=component_id,
        options=[{"label": "  % of population", "value": "normalize"}],
        value=[],
        inline=True,
        style={"fontFamily": "Arial", "marginTop": "6px", "marginBottom": "12px"},
    )


# App layout ###################################################################################
app = Dash(__name__, title="Census Data Explorer")
server = app.server  # for gunicorn
# Dash links assets/favicon.ico on its own; add the SVG after it so browsers
# that support SVG icons get the one that follows light/dark mode.
app.index_string = app.index_string.replace(
    "{%favicon%}",
    '{%favicon%}\n        <link rel="icon" type="image/svg+xml" href="'
    + app.get_asset_url("favicon.svg")
    + '">',
)

_tab_style = {"fontFamily": "Arial"}
_sidebar_style = {"fontFamily": "Arial", "width": "300px", "padding": "20px", "flexShrink": 0}
_chart_style = {"flexGrow": 1, "padding": "20px"}
_flex_row = {"display": "flex", "alignItems": "flex-start"}
_bold = {"fontWeight": "bold"}
_bold_mt = {"fontWeight": "bold", "marginTop": "12px"}

app.layout = html.Div(
    [
        html.H1("Census Data Explorer", style={"fontFamily": "Arial"}),
        dcc.Store(id="trends-active-preset", data=None),
        dcc.Store(id="scatter-active-preset", data=None),
        dcc.Store(id="anim-active-preset", data=None),
        dcc.Store(id="corr-active-group", data=None),
        dcc.Store(id="ap-active-preset", data=None),
        dcc.Store(id="housing-active-preset", data=None),
        dcc.Tabs(
            [
                dcc.Tab(
                    label="US Map",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="us-map-geo-selector",
                                            options=[
                                                {"label": "State", "value": "State"},
                                                {"label": "DMA", "value": "DMA"},
                                                {"label": "Congressional District", "value": "Congressional District"},
                                            ],
                                            value="State",
                                            multi=False,
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Select Metrics",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="us-map-metric-selector",
                                            options=_make_options(state_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                        _normalize_checkbox("us-map-normalize"),
                                        dcc.Checklist(
                                            id="us-map-exclude-pr",
                                            options=[{"label": "  Exclude Puerto Rico", "value": "exclude"}],
                                            value=[],
                                            inline=True,
                                            style={"fontFamily": "Arial", "marginTop": "8px"},
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="us_map", width="100%", height="700"
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        )
                    ],
                ),
                dcc.Tab(
                    label="State Map",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="state-map-geo",
                                            options=[
                                                {"label": "County", "value": "County"},
                                                {"label": "Congressional District", "value": "Congressional District"},
                                                *([{"label": "Tract", "value": "Tract"}] if DEV_MODE else []),
                                            ],
                                            value="County",
                                            multi=False,
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Select Metrics",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="state-map-metric",
                                            options=_make_options(county_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                        _normalize_checkbox("state-map-normalize"),
                                        html.Label(
                                            "Select State", style={"fontWeight": "bold"}
                                        ),
                                        dcc.Dropdown(
                                            id="state-map-state",
                                            options=states,
                                            value="New York",
                                            multi=False,
                                            placeholder="Select State...",
                                        ),
                                        html.Div(
                                            id="state-map-tract-filters",
                                            children=[
                                                html.Label(
                                                    "Exclude GEOIDs",
                                                    style=_bold,
                                                ),
                                                dcc.Dropdown(
                                                    id="state-map-exclude",
                                                    options=[],
                                                    multi=True,
                                                    placeholder="Select GEOIDs to exclude...",
                                                ),
                                                html.Label(
                                                    "Minimum Population",
                                                    style=_bold,
                                                ),
                                                dcc.Input(
                                                    id="state-map-pop-min",
                                                    type="number",
                                                    value=0,
                                                    min=0,
                                                    step=1,
                                                ),
                                            ],
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="state_map", width="100%", height="700"
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        )
                    ],
                ),
                dcc.Tab(
                    label="ZCTAs",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="zcta-metric-selector",
                                            options=_make_options(zcta_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                        _normalize_checkbox("zcta-normalize"),
                                        html.Label(
                                            "Select DMA", style={"fontWeight": "bold"}
                                        ),
                                        dcc.Dropdown(
                                            id="dma-selector",
                                            options=dmas,
                                            value="New York",
                                            multi=False,
                                            placeholder="Select DMA...",
                                        ),
                                        html.Label(
                                            "Minimum Population",
                                            style=_bold,
                                        ),
                                        dcc.Input(
                                            id="zcta-pop-min",
                                            type="number",
                                            value=0,
                                            min=0,
                                            step=1,
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="zcta_map", width="100%", height="700"
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        )
                    ],
                ),
                *(
                    [dcc.Tab(
                        label="Block Groups",
                        style=_tab_style,
                        selected_style=_tab_style,
                        children=[
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Label("Select Metrics", style=_bold),
                                            dcc.Dropdown(
                                                id="block-group-metric-selector",
                                                options=_make_options(block_group_metric_cols),
                                                value=[DEFAULT_VAR],
                                                multi=True,
                                                placeholder="Select metrics...",
                                            ),
                                            _normalize_checkbox("block-group-normalize"),
                                            html.Label("Select City", style={"fontWeight": "bold"}),
                                            dcc.Dropdown(
                                                id="city-selector",
                                                options=cities,
                                                value="New York",
                                                multi=False,
                                                placeholder="Select City...",
                                            ),
                                            html.Label("Exclude GEOIDs", style=_bold),
                                            dcc.Dropdown(
                                                id="block-group-exclude",
                                                options=[],
                                                multi=True,
                                                placeholder="Select GEOIDs to exclude...",
                                            ),
                                            html.Label("Minimum Population", style=_bold),
                                            dcc.Input(
                                                id="block-group-pop-min",
                                                type="number",
                                                value=0,
                                                min=0,
                                                step=1,
                                            ),
                                        ],
                                        style=_sidebar_style,
                                    ),
                                    html.Div(
                                        [html.Iframe(id="block_group_map", width="100%", height="700")],
                                        style=_chart_style,
                                    ),
                                ],
                                style=_flex_row,
                            )
                        ],
                    )]
                    if DEV_MODE else []
                ),
                dcc.Tab(
                    label="Trends",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                        "whiteSpace": "nowrap",
                                    },
                                ),
                                *[
                                    html.Button(
                                        s["label"],
                                        id=f"trends-preset-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, s in enumerate(SUGGESTED_TRENDS)
                                ],
                            ],
                            style={
                                "padding": "8px 20px",
                                "borderBottom": "1px solid #eee",
                                "display": "flex",
                                "flexWrap": "wrap",
                                "alignItems": "center",
                                "gap": "4px",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="trends-geo-level",
                                            options=list(TIMESERIES_GEOS.keys()),
                                            value="State",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Select Geography",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="trends-geo",
                                            options=sorted(ts_state["state"].unique()),
                                            value=_ts_state_defaults,
                                            multi=True,
                                            placeholder="Select geographies to compare "
                                            "(incl. United States)...",
                                        ),
                                        html.Label(
                                            "Segment By",
                                            style=_bold_mt,
                                        ),
                                        dcc.RadioItems(
                                            id="trends-segment",
                                            options=[
                                                {"label": "  Geography", "value": "geo"},
                                                {"label": "  Race", "value": "race"},
                                                {"label": "  Householder Age", "value": "age"},
                                            ],
                                            value="geo",
                                            labelStyle={
                                                "display": "block",
                                                "marginTop": "4px",
                                            },
                                        ),
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Groups",
                                                    id="trends-race-label",
                                                    style=_bold_mt,
                                                ),
                                                dcc.Dropdown(
                                                    id="trends-race",
                                                    options=RACE_GROUPS,
                                                    value=RACE_DEFAULTS,
                                                    multi=True,
                                                    placeholder="Select groups...",
                                                ),
                                            ],
                                            id="trends-race-wrap",
                                            style={"display": "none"},
                                        ),
                                        html.Label(
                                            "Metric",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="trends-metric",
                                            options=_make_options(
                                                _with_naep(TIMESERIES_METRICS, "State")
                                            ),
                                            value="Median Household Income",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "View",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="trends-view",
                                            options=[
                                                {"label": v, "value": k}
                                                for k, v in TRENDS_VIEWS.items()
                                            ],
                                            value="level",
                                            clearable=False,
                                        ),
                                        html.Div(
                                            [
                                                html.Label(
                                                    "Baseline",
                                                    style=_bold_mt,
                                                ),
                                                dcc.Dropdown(
                                                    id="trends-baseline",
                                                    options=[
                                                        {"label": v, "value": k}
                                                        for k, v in RATIO_BASELINES.items()
                                                    ],
                                                    value="self",
                                                    clearable=False,
                                                ),
                                            ],
                                            id="trends-baseline-wrap",
                                            style={"display": "none"},
                                        ),
                                        _inflate_checkbox("trends-inflate"),
                                        html.P(
                                            "ACS 5-Year Estimates. Income-share metrics use "
                                            "nominal dollars, so rising shares reflect "
                                            "inflation as well as real gains. Each point pools the "
                                            "prior 5 years — 2024 covers 2020–2024 — so "
                                            "trends lag real turning points. Before 2009, "
                                            "points are decennial censuses (2000 long form; "
                                            "1790–1990 via NHGIS, so coverage varies by "
                                            "metric); no data for 2001–08. "
                                            "Historical data: IPUMS NHGIS, University of "
                                            "Minnesota, www.nhgis.org.",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                        html.P(
                                            "NAEP scores are public-school averages; "
                                            "United States = national public. NAEP's "
                                            "Asian group includes Pacific Islanders.",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                            },
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="trends-chart", style={"height": "700px"}
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Animated Scatter",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                    },
                                ),
                                *[
                                    html.Button(
                                        s["label"],
                                        id=f"anim-preset-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, s in enumerate(SUGGESTED_ANIM_SCATTERS)
                                ],
                            ],
                            style={
                                "padding": "12px 20px",
                                "borderBottom": "1px solid #eee",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="anim-geo-level",
                                            options=list(TIMESERIES_GEOS.keys()),
                                            value="State",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "X Axis",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="anim-x",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="pct_poverty",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Y Axis",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="anim-y",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="Median Household Income",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Color by (optional)",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="anim-color",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="pct_black",
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                        html.Label(
                                            "Size by (optional)",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="anim-size",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="Pop",
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                        _inflate_checkbox("anim-inflate"),
                                        html.P(
                                            "ACS 5-Year Estimates. Income-share metrics use "
                                            "nominal dollars, so rising shares reflect "
                                            "inflation as well as real gains. Each point pools the "
                                            "prior 5 years — 2024 covers 2020–2024 — so "
                                            "trends lag real turning points. Before 2009, "
                                            "points are decennial censuses (2000 long form; "
                                            "1790–1990 via NHGIS, so coverage varies by "
                                            "metric); no data for 2001–08. "
                                            "Historical data: IPUMS NHGIS, University of "
                                            "Minnesota, www.nhgis.org.",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="anim-scatter-plot",
                                            style={"height": "700px"},
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Scatter",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                        "whiteSpace": "nowrap",
                                    },
                                ),
                                *[
                                    html.Button(
                                        s["label"],
                                        id=f"scatter-preset-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, s in enumerate(SUGGESTED_SCATTERS)
                                ],
                            ],
                            style={
                                "padding": "8px 20px",
                                "borderBottom": "1px solid #eee",
                                "display": "flex",
                                "flexWrap": "wrap",
                                "alignItems": "center",
                                "gap": "4px",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-geo",
                                            options=list(SCATTER_GEOS.keys()),
                                            value="County",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "X Axis",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-x",
                                            options=_make_options(county_metric_cols),
                                            value="Pop",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Y Axis",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-y",
                                            options=_make_options(county_metric_cols),
                                            value="pct_male",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Color by (optional)",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-color",
                                            options=_make_options(county_metric_cols),
                                            value=None,
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                        html.Label(
                                            "Size by (optional)",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-size",
                                            options=_make_options(county_metric_cols),
                                            value=None,
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                        dcc.Checklist(
                                            id="scatter-trendline",
                                            options=[
                                                {
                                                    "label": "  Show trend line",
                                                    "value": "show",
                                                }
                                            ],
                                            value=[],
                                            inline=True,
                                            style={
                                                "fontFamily": "Arial",
                                                "marginTop": "14px",
                                                "fontSize": "13px",
                                            },
                                        ),
                                        html.Label(
                                            id="scatter-filter-label",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "16px",
                                                "display": "block",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-filter",
                                            options=[],
                                            value=[],
                                            multi=True,
                                            placeholder="All",
                                            disabled=True,
                                            style={"marginTop": "4px"},
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="scatter-plot",
                                            style={"height": "700px"},
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style={
                                "display": "flex",
                                "alignItems": "flex-start",
                            },
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Correlation",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                    },
                                ),
                                *[
                                    html.Button(
                                        label,
                                        id=f"corr-group-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, label in enumerate(CORR_METRIC_GROUPS.keys())
                                ],
                            ],
                            style={
                                "padding": "12px 20px",
                                "borderBottom": "1px solid #eee",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Geography Level",
                                            style=_bold,
                                        ),
                                        dcc.Dropdown(
                                            id="corr-geo-level",
                                            options=list(CORR_GEOS.keys()),
                                            value="County",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Metrics",
                                            style=_bold_mt,
                                        ),
                                        dcc.Dropdown(
                                            id="corr-metrics",
                                            options=_make_options(county_metric_cols),
                                            value=CORR_METRIC_GROUPS[next(iter(CORR_METRIC_GROUPS))],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="corr-matrix", style={"height": "700px"}
                                        )
                                    ],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Age Profile",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                    },
                                ),
                                *[
                                    html.Button(
                                        s["label"],
                                        id=f"ap-preset-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, s in enumerate(SUGGESTED_AGE_PROFILES)
                                ],
                            ],
                            style={
                                "padding": "12px 20px",
                                "borderBottom": "1px solid #eee",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label("Geography Level", style=_bold),
                                        dcc.Dropdown(
                                            id="ap-geo-level",
                                            options=AGE_PROFILE_LEVELS,
                                            value="State",
                                            clearable=False,
                                        ),
                                        html.Label("Select Geography", style=_bold_mt),
                                        dcc.Dropdown(
                                            id="ap-geo",
                                            options=_age_profile_geos("State"),
                                            value=[US_LABEL],
                                            multi=True,
                                            placeholder="Select geographies to compare...",
                                        ),
                                        html.Label("Metric", style=_bold_mt),
                                        dcc.Dropdown(
                                            id="ap-metric",
                                            options=_make_options(AGE_PROFILE_METRICS),
                                            value="pct_poverty",
                                            clearable=False,
                                        ),
                                        html.Label("Sex", style=_bold_mt),
                                        dcc.RadioItems(
                                            id="ap-sex",
                                            options=_age_profile_sex_options("pct_poverty"),
                                            value="Both",
                                            labelStyle={
                                                "display": "block",
                                                "marginTop": "4px",
                                            },
                                        ),
                                        html.P(
                                            "ACS 5-Year Estimates, 2020–2024. Age groups "
                                            "follow each Census table, so they differ by "
                                            "metric. Homeownership and income are by age "
                                            "of householder (who owns or rents the home), "
                                            "so they have no sex split. A snapshot across "
                                            "ages at one time, not people followed as they "
                                            "age.",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [dcc.Graph(id="ap-chart", style={"height": "700px"})],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Housing",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Span(
                                    "Suggested: ",
                                    style={
                                        "fontFamily": "Arial",
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                    },
                                ),
                                *[
                                    html.Button(
                                        s["label"],
                                        id=f"housing-preset-{i}",
                                        n_clicks=0,
                                        style=_btn_style,
                                    )
                                    for i, s in enumerate(SUGGESTED_HOUSING)
                                ],
                            ],
                            style={
                                "padding": "12px 20px",
                                "borderBottom": "1px solid #eee",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label("View", style=_bold),
                                        dcc.RadioItems(
                                            id="housing-view",
                                            options=[
                                                {"label": f"  {v}", "value": k}
                                                for k, v in HOUSING_VIEWS.items()
                                            ],
                                            value="trends",
                                            labelStyle={
                                                "display": "block",
                                                "marginTop": "4px",
                                            },
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Metric", style=_bold_mt),
                                                dcc.Dropdown(
                                                    id="housing-metric",
                                                    options=_make_options(HOUSING_METRICS),
                                                    value="pct_rental_vacancy",
                                                    clearable=False,
                                                ),
                                            ],
                                            id="housing-metric-controls",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Y Axis", style=_bold_mt),
                                                dcc.RadioItems(
                                                    id="housing-rate-y",
                                                    options=[
                                                        {"label": f"  {v}", "value": k}
                                                        for k, v in HOUSING_RATE_Y.items()
                                                    ],
                                                    value="real_home_value",
                                                    labelStyle={
                                                        "display": "block",
                                                        "marginTop": "4px",
                                                    },
                                                ),
                                            ],
                                            id="housing-rate-y-controls",
                                            style={"display": "none"},
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Select Geography", style=_bold_mt),
                                                dcc.Dropdown(
                                                    id="housing-geo",
                                                    options=HOUSING_STATES,
                                                    value=[US_LABEL],
                                                    multi=True,
                                                    placeholder="Select states to compare...",
                                                ),
                                            ],
                                            id="housing-geo-controls",
                                        ),
                                        html.Div(
                                            id="housing-note",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [dcc.Graph(id="housing-chart", style={"height": "700px"})],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
                dcc.Tab(
                    label="Elections",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        dcc.RadioItems(
                                            id="election-mode",
                                            options=[
                                                {"label": "  2026 forecast", "value": "forecast"},
                                                {"label": "  Backtest, 2010–2024", "value": "backtest"},
                                            ],
                                            value="forecast",
                                            labelStyle={"display": "block", "marginTop": "4px"},
                                            style={"marginBottom": "12px"},
                                        ),
                                        html.Div(
                                            [
                                                html.Label("View", style=_bold),
                                                dcc.RadioItems(
                                                    id="election-view",
                                                    options=[
                                                        {"label": f"  {v}", "value": k}
                                                        for k, v in ELECTION_VIEWS.items()
                                                    ],
                                                    value="senate_seats",
                                                    labelStyle={
                                                        "display": "block",
                                                        "marginTop": "4px",
                                                    },
                                                ),
                                            ],
                                            id="election-forecast-controls",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Chamber", style=_bold),
                                                dcc.RadioItems(
                                                    id="backtest-chamber",
                                                    options=[
                                                        {"label": f"  {v[0]}", "value": k}
                                                        for k, v in BACKTEST_CHAMBERS.items()
                                                    ],
                                                    value="senate",
                                                    inline=True,
                                                    labelStyle={"marginRight": "12px"},
                                                ),
                                                html.Label("View", style=_bold_mt),
                                                dcc.RadioItems(
                                                    id="backtest-view",
                                                    options=[
                                                        {"label": f"  {v}", "value": k}
                                                        for k, v in BACKTEST_VIEWS.items()
                                                    ],
                                                    value="seats",
                                                    labelStyle={
                                                        "display": "block",
                                                        "marginTop": "4px",
                                                    },
                                                ),
                                                html.Label("Election", style=_bold_mt),
                                                dcc.Dropdown(
                                                    id="backtest-year",
                                                    options=_backtest_year_options("senate"),
                                                    value="all",
                                                    clearable=False,
                                                ),
                                            ],
                                            id="election-backtest-controls",
                                            style={"display": "none"},
                                        ),
                                        html.Div(
                                            id="election-summary",
                                            style={"marginTop": "16px", "fontSize": "14px"},
                                        ),
                                        html.Div(
                                            id="election-note",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                    ],
                                    style=_sidebar_style,
                                ),
                                html.Div(
                                    [dcc.Graph(id="election-chart", style={"height": "700px"})],
                                    style=_chart_style,
                                ),
                            ],
                            style=_flex_row,
                        ),
                    ],
                ),
            ],
        ),
    ]
)


# Map generation ###############################################################################


def _build_choropleth_map(
    geo_json, data_df, id_col, label_key, selected_metrics, name_col=None
):
    """Build a folium map with one choropleth layer per selected metric."""
    m = folium.Map(tiles=None)
    data_df_indexed = data_df.set_index(id_col)
    id_set = set(data_df[id_col])

    for i in selected_metrics:
        my_chp = folium.Choropleth(
            tiles="Esri.WorldGrayCanvas",
            geo_data=geo_json,
            data=data_df,
            columns=[id_col, i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=_get_color(i),
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False,
        ).add_to(m)

        for s in my_chp.geojson.data["features"]:
            if s["id"] in id_set:
                label = (
                    str(data_df_indexed.loc[s["id"], name_col]) if name_col else s["id"]
                )
                val = data_df_indexed.loc[s["id"], i]
            else:
                label = s["id"]
                val = 0
            s["properties"][label_key] = label
            # numpy scalars (float32 since the ACS loads downcast) aren't JSON
            # serializable, and folium renders these properties straight to JSON.
            s["properties"][i] = val.item() if hasattr(val, "item") else val
        folium.GeoJsonTooltip([label_key, i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles="Esri.WorldGrayCanvas", control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()


def generate_state_map(selected_metrics, normalize=False, exclude_pr=False):
    """Render state choropleth map."""
    df = _normalize_df(c_state, selected_metrics) if normalize else c_state
    if exclude_pr:
        df = df[df["state"] != "Puerto Rico"]
    return _build_choropleth_map(
        state_geom_json, df, "state", "State", selected_metrics
    )


def generate_dma_map(selected_metrics, normalize=False, exclude_pr=False):
    _ = exclude_pr  # DMAs never include PR; parameter exists for dispatch compatibility
    df = _normalize_df(c_dma, selected_metrics) if normalize else c_dma
    return _build_choropleth_map(dma_geom_json, df, "dma", "DMA", selected_metrics)


def generate_county_map(selected_metrics, selected_state, normalize=False):
    """Render county choropleth map for a single state."""
    state_fips = state_name.loc[
        state_name["state_NAME"] == selected_state, "state"
    ].values[0]
    df = c_county_state.loc[c_county_state["GEOID"].str[:2] == state_fips].reset_index(drop=True)
    if normalize:
        df = _normalize_df(df, selected_metrics)
    return _build_choropleth_map(
        county_geom_by_state[state_fips], df, "GEOID", "County", selected_metrics, name_col="NAME"
    )


def generate_zcta_map(selected_metrics, selected_dma, pop_min=None, normalize=False):
    """Render ZCTA choropleth map filtered to a single DMA."""
    zcta_geom_select = zcta_geom[zcta_geom["dma"] == selected_dma].reset_index()
    zcta_geom_select = zcta_geom_select[["ZCTA5CE20", "geometry"]].set_index(
        "ZCTA5CE20"
    )
    zcta_json_select = zcta_geom_select.to_json()

    df = c_zcta_dma[c_zcta_dma["dma"] == selected_dma].reset_index(drop=True)
    df = df.rename(columns={"zcta": "ZCTA5CE20"})
    df = df.loc[df[DEFAULT_VAR] >= (pop_min or 0)]
    if normalize:
        df = _normalize_df(df, selected_metrics)

    return _build_choropleth_map(
        zcta_json_select, df, "ZCTA5CE20", "ZCTA", selected_metrics
    )


def generate_tract_map(
    selected_metrics, selected_state, pop_min=None, exclude=None, normalize=False
):
    """Render census tract choropleth map for a single state."""
    state_fips = state_name.loc[
        state_name["state_NAME"] == selected_state, "state"
    ].values[0]

    df = c_tract.loc[c_tract["GEOID"].str[:2] == state_fips].reset_index(drop=True)
    if pop_min is not None:
        df = df.loc[df[DEFAULT_VAR] >= pop_min]
    if exclude:
        df = df.loc[~df["GEOID"].isin(exclude)]
    if normalize:
        df = _normalize_df(df, selected_metrics)

    return _build_choropleth_map(
        tract_geom_by_state[state_fips], df, "GEOID", "Tract", selected_metrics
    )


def generate_block_group_map(
    selected_metrics, selected_city, pop_min=None, exclude=None, normalize=False
):
    """Render block group choropleth map for NYC, LA, or SF."""
    county_fips = _city_fips[selected_city]
    df = c_block_group.loc[
        c_block_group["GEOID"].str[:5].isin(county_fips)
    ].reset_index(drop=True)
    if pop_min is not None:
        df = df.loc[df[DEFAULT_VAR] >= pop_min]
    if exclude:
        df = df.loc[~df["GEOID"].isin(exclude)]
    if normalize:
        df = _normalize_df(df, selected_metrics)

    return _build_choropleth_map(
        block_group_geom_by_city[selected_city],
        df,
        "GEOID",
        "Block Group",
        selected_metrics,
    )


def generate_state_cd_map(selected_metrics, selected_state, normalize=False):
    """Render congressional district map filtered to a single state."""
    state_fips = state_name.loc[state_name["state_NAME"] == selected_state, "state"].values[0]
    df = c_congressional_district.loc[
        c_congressional_district["GEOID"].str[:2] == state_fips
    ].reset_index(drop=True)
    if normalize:
        df = _normalize_df(df, selected_metrics)
    geo = congressional_district_geom[
        congressional_district_geom.index.str[:2] == state_fips
    ].to_json()
    return _build_choropleth_map(geo, df, "GEOID", "Congressional District", selected_metrics, name_col="NAME")


def generate_congressional_district_map(selected_metrics, normalize=False, exclude_pr=False):
    """Render congressional district choropleth map for the full US."""
    df = _normalize_df(c_congressional_district, selected_metrics) if normalize else c_congressional_district
    if exclude_pr:
        df = df[df["GEOID"].str[:2] != "72"]
    return _build_choropleth_map(
        congressional_district_geom_json,
        df,
        "GEOID",
        "Congressional District",
        selected_metrics,
        name_col="NAME",
    )


# Callbacks ########################################################################################


_US_MAP_GEOS = {
    "State": (state_metric_cols, generate_state_map),
    "DMA": (dma_metric_cols, generate_dma_map),
    "Congressional District": (congressional_district_metric_cols, generate_congressional_district_map),
}


@app.callback(
    Output("us-map-metric-selector", "options"),
    Output("us-map-metric-selector", "value"),
    Input("us-map-geo-selector", "value"),
)
def update_us_map_metric_options(geo):
    cols, _ = _US_MAP_GEOS[geo]
    return _make_options(cols), [DEFAULT_VAR]


@app.callback(
    Output("us_map", "srcDoc"),
    Input("us-map-metric-selector", "value"),
    Input("us-map-geo-selector", "value"),
    Input("us-map-normalize", "value"),
    Input("us-map-exclude-pr", "value"),
)
def update_us_map(metrics, geo, normalize, exclude_pr):
    _, generate_fn = _US_MAP_GEOS[geo]
    return generate_fn(metrics, bool(normalize), exclude_pr=bool(exclude_pr))


_STATE_MAP_GEOS = {
    "County": (county_metric_cols, generate_county_map),
    "Congressional District": (congressional_district_metric_cols, generate_state_cd_map),
}
if DEV_MODE:
    _STATE_MAP_GEOS["Tract"] = (tract_metric_cols, generate_tract_map)


@app.callback(
    Output("state-map-metric", "options"),
    Output("state-map-metric", "value"),
    Input("state-map-geo", "value"),
)
def update_state_map_metric_options(geo):
    cols, _ = _STATE_MAP_GEOS[geo]
    return _make_options(cols), [DEFAULT_VAR]


@app.callback(
    Output("state-map-tract-filters", "style"),
    Input("state-map-geo", "value"),
)
def toggle_state_map_tract_filters(geo):
    return {"display": "block"} if geo == "Tract" else {"display": "none"}


@app.callback(
    Output("state-map-exclude", "options"),
    Output("state-map-exclude", "value"),
    Input("state-map-geo", "value"),
    Input("state-map-state", "value"),
)
def update_state_map_exclude_options(geo, selected_state):
    state_fips = state_name.loc[
        state_name["state_NAME"] == selected_state, "state"
    ].values[0]
    if geo == "Tract":
        options = sorted(c_tract.loc[c_tract["GEOID"].str[:2] == state_fips, "GEOID"].unique())
    else:
        options = sorted(c_county_state.loc[c_county_state["GEOID"].str[:2] == state_fips, "GEOID"].unique())
    return options, []


@app.callback(
    Output("state_map", "srcDoc"),
    Input("state-map-metric", "value"),
    Input("state-map-geo", "value"),
    Input("state-map-state", "value"),
    Input("state-map-pop-min", "value"),
    Input("state-map-exclude", "value"),
    Input("state-map-normalize", "value"),
)
def update_state_map(metrics, geo, selected_state, pop_min, exclude, normalize):
    if geo == "Tract":
        return generate_tract_map(metrics, selected_state, pop_min, exclude, bool(normalize))
    if geo == "Congressional District":
        return generate_state_cd_map(metrics, selected_state, bool(normalize))
    return generate_county_map(metrics, selected_state, bool(normalize))


@app.callback(
    Output("zcta_map", "srcDoc"),
    Input("zcta-metric-selector", "value"),
    Input("dma-selector", "value"),
    Input("zcta-pop-min", "value"),
    Input("zcta-normalize", "value"),
)
def update_zcta_map(metrics, dma, pop_min, normalize):
    return generate_zcta_map(metrics, dma, pop_min, bool(normalize))


if DEV_MODE:
    @app.callback(
        Output("block_group_map", "srcDoc"),
        Input("block-group-metric-selector", "value"),
        Input("city-selector", "value"),
        Input("block-group-pop-min", "value"),
        Input("block-group-exclude", "value"),
        Input("block-group-normalize", "value"),
    )
    def update_block_group_map(metrics, city, pop_min, exclude, normalize):
        return generate_block_group_map(metrics, city, pop_min, exclude, bool(normalize))

    @app.callback(Output("block-group-exclude", "options"), Input("city-selector", "value"))
    def update_block_group_exclude_options(selected_city):
        return sorted(
            c_block_group.loc[
                c_block_group["GEOID"].str[:5].isin(_city_fips[selected_city]), "GEOID"
            ].unique()
        )


@app.callback(
    Output("scatter-geo", "value"),
    Output("scatter-x", "value"),
    Output("scatter-y", "value"),
    Output("scatter-color", "value"),
    Output("scatter-size", "value"),
    [Input(f"scatter-preset-{i}", "n_clicks") for i in range(len(SUGGESTED_SCATTERS))],
    prevent_initial_call=True,
)
def load_scatter_preset(*_):
    s = SUGGESTED_SCATTERS[_triggered_idx("scatter-preset")]
    return s["geo"], s["x"], s["y"], s["color"], s.get("size")


@app.callback(
    Output("scatter-x", "options"),
    Output("scatter-y", "options"),
    Output("scatter-color", "options"),
    Output("scatter-size", "options"),
    Input("scatter-geo", "value"),
)
def update_scatter_options(geo):
    _, _, cols = SCATTER_GEOS[geo]
    opts = _make_options(cols)
    return opts, opts, opts, opts


@app.callback(
    Output("scatter-filter-label", "children"),
    Output("scatter-filter", "options"),
    Output("scatter-filter", "value"),
    Output("scatter-filter", "disabled"),
    Input("scatter-geo", "value"),
)
def update_scatter_filter_options(geo):
    if geo == "County":
        opts = sorted(c_county_state["state_NAME"].dropna().unique())
        return "Filter by State", [{"label": o, "value": o} for o in opts], [], False
    if geo == "ZCTA":
        opts = sorted(c_zcta_dma["dma"].dropna().unique())
        return "Filter by DMA", [{"label": o, "value": o} for o in opts], [], False
    return "Filter", [], [], True


@app.callback(
    Output("scatter-plot", "figure"),
    Input("scatter-geo", "value"),
    Input("scatter-x", "value"),
    Input("scatter-y", "value"),
    Input("scatter-color", "value"),
    Input("scatter-size", "value"),
    Input("scatter-trendline", "value"),
    Input("scatter-filter", "value"),
)
def update_scatter(
    geo, x_metric, y_metric, color_metric, size_metric, show_trendline, filter_vals
):
    if not x_metric or not y_metric:
        return px.scatter()

    df, label_col, _ = SCATTER_GEOS[geo]

    if filter_vals:
        if geo == "County":
            df = df[df["state_NAME"].isin(filter_vals)]
        elif geo == "ZCTA":
            df = df[df["dma"].isin(filter_vals)]

    extra = [m for m in [color_metric, size_metric] if m]
    cols = list(dict.fromkeys([label_col, x_metric, y_metric] + extra))
    plot_df = df[cols].dropna(subset=[x_metric, y_metric])

    if size_metric:
        plot_df = plot_df[plot_df[size_metric] > 0].dropna(subset=[size_metric])

    fig = px.scatter(
        plot_df,
        x=x_metric,
        y=y_metric,
        color=color_metric or None,
        size=size_metric or None,
        size_max=40,
        hover_name=label_col,
        color_continuous_scale=(
            _trunc_colorscale(_get_color(color_metric))
            if color_metric
            else _trunc_colorscale("Viridis")
        ),
    )
    fig.update_traces(
        marker={"opacity": 0.65},
        hovertemplate=_hovertemplate(x_metric, y_metric, color_metric),
    )
    if color_metric:
        fmt = _axis_fmt(color_metric)
        fig.update_coloraxes(
            colorbar_tickprefix=fmt.get("tickprefix", ""),
            colorbar_tickformat=fmt.get("tickformat", ""),
        )
    if show_trendline:
        result = _compute_trendline(plot_df, x_metric, y_metric)
        if result:
            x_line, y_line, slope, intercept, r2 = result
            fig.add_shape(
                type="line",
                x0=x_line[0],
                y0=y_line[0],
                x1=x_line[-1],
                y1=y_line[-1],
                line={"color": "crimson", "width": 2.5},
                layer="above",
            )
            sign = "+" if intercept >= 0 else "-"
            fig.add_annotation(
                x=0.02,
                y=0.98,
                xref="paper",
                yref="paper",
                text=f"y = {_fmt_coef(slope)}x {sign} {_fmt_coef(abs(intercept))}<br>R² = {r2:.3f}",
                showarrow=False,
                align="left",
                xanchor="left",
                yanchor="top",
                font={"size": 12, "family": "monospace"},
                bgcolor="rgba(255,255,255,0.85)",
                bordercolor="#ccc",
                borderwidth=1,
            )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis=_axis_fmt(x_metric),
        yaxis=_axis_fmt(y_metric),
    )
    return fig


@app.callback(
    Output("anim-geo-level", "value"),
    Output("anim-x", "value"),
    Output("anim-y", "value"),
    Output("anim-color", "value"),
    Output("anim-size", "value"),
    [
        Input(f"anim-preset-{i}", "n_clicks")
        for i in range(len(SUGGESTED_ANIM_SCATTERS))
    ],
    prevent_initial_call=True,
)
def load_anim_preset(*_):
    s = SUGGESTED_ANIM_SCATTERS[_triggered_idx("anim-preset")]
    return s["geo_level"], s["x"], s["y"], s["color"], s["size"]


@app.callback(
    Output("anim-scatter-plot", "figure"),
    Input("anim-geo-level", "value"),
    Input("anim-x", "value"),
    Input("anim-y", "value"),
    Input("anim-color", "value"),
    Input("anim-size", "value"),
    Input("anim-inflate", "value"),
)
def update_anim_scatter(
    geo_level, x_metric, y_metric, color_metric, size_metric, inflate
):
    if not x_metric or not y_metric:
        return px.scatter()

    df, name_col = TIMESERIES_GEOS[geo_level]

    extra = [m for m in [color_metric, size_metric] if m]
    cols = list(dict.fromkeys(["year", name_col, x_metric, y_metric] + extra))
    # px orders animation frames by first appearance, and geographies now start
    # in different census years, so sort by year or frames come out of order.
    plot_df = df[cols].dropna(subset=[x_metric, y_metric]).sort_values(["year", name_col])
    # The national row isn't a state, and sized by population it hides the states.
    plot_df = plot_df[plot_df[name_col] != US_LABEL]

    if inflate:
        plot_df = _apply_cpi(plot_df)

    if size_metric:
        plot_df = plot_df[plot_df[size_metric] > 0].dropna(subset=[size_metric])

    x_pad = (plot_df[x_metric].max() - plot_df[x_metric].min()) * 0.05
    y_pad = (plot_df[y_metric].max() - plot_df[y_metric].min()) * 0.05

    fig = px.scatter(
        plot_df,
        x=x_metric,
        y=y_metric,
        color=color_metric or None,
        size=size_metric or None,
        size_max=60,
        hover_name=name_col,
        animation_frame="year",
        animation_group=name_col,
        range_x=[plot_df[x_metric].min() - x_pad, plot_df[x_metric].max() + x_pad],
        range_y=[plot_df[y_metric].min() - y_pad, plot_df[y_metric].max() + y_pad],
        # One color range for all years; px otherwise rescales it every frame.
        range_color=(
            [plot_df[color_metric].min(), plot_df[color_metric].max()] if color_metric else None
        ),
        color_continuous_scale=(
            _trunc_colorscale(_get_color(color_metric))
            if color_metric
            else _trunc_colorscale("Viridis")
        ),
    )
    tmpl = _hovertemplate(x_metric, y_metric, color_metric)
    fig.update_traces(marker={"opacity": 0.7}, hovertemplate=tmpl)
    for frame in fig.frames:
        for trace in frame.data:
            trace.hovertemplate = tmpl
    if color_metric:
        fmt = _axis_fmt(color_metric)
        fig.update_coloraxes(
            colorbar_tickprefix=fmt.get("tickprefix", ""),
            colorbar_tickformat=fmt.get("tickformat", ""),
        )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis=_axis_fmt(x_metric),
        yaxis=_axis_fmt(y_metric),
    )
    return fig


@app.callback(Output("trends-geo", "options"), Input("trends-geo-level", "value"))
def update_trends_geo_options(geo_level):
    df, name_col = TIMESERIES_GEOS[geo_level]
    return sorted(df[name_col].unique())


@app.callback(
    Output("trends-race-wrap", "style"),
    Output("trends-race-label", "children"),
    Output("trends-race", "options"),
    Output("trends-race", "value", allow_duplicate=True),
    Output("trends-metric", "options"),
    Output("trends-metric", "value", allow_duplicate=True),
    Input("trends-segment", "value"),
    Input("trends-geo-level", "value"),
    State("trends-race", "value"),
    State("trends-metric", "value"),
    prevent_initial_call=True,
)
def toggle_trends_segment(segment, geo_level, values, metric):
    """Swap the group picker and metric list to match the segment mode.

    Options and values move together: each mode supports a different set of
    metrics (home value and rent have no race iteration; age only covers
    median income; NAEP is state-only), so a metric that just left the list
    is replaced.
    """
    if segment == "race":
        metrics = _with_naep(TIMESERIES_RACE_METRICS, geo_level)
        if metric not in metrics:
            metric = "Median Household Income"
        if not set(values or []) & set(RACE_GROUPS):
            values = RACE_DEFAULTS
        return (
            {"display": "block"},
            "Race / Ethnicity",
            RACE_GROUPS,
            values,
            _make_options(metrics),
            metric,
        )
    if segment == "age":
        # B19049 is the only age-of-householder table, so income is the sole metric.
        return (
            {"display": "block"},
            "Householder Age",
            AGE_BRACKETS,
            AGE_BRACKETS,
            _make_options(TIMESERIES_AGE_METRICS),
            TIMESERIES_AGE_METRICS[0],
        )
    metrics = _with_naep(TIMESERIES_METRICS, geo_level)
    if metric not in metrics:
        metric = "Median Household Income"
    return (
        {"display": "none"},
        "Groups",
        RACE_GROUPS,
        values,
        _make_options(metrics),
        metric,
    )


@app.callback(
    Output("trends-baseline-wrap", "style"), Input("trends-view", "value")
)
def toggle_trends_baseline(view):
    show = view in ("ratio", "diff")
    return {"display": "block"} if show else {"display": "none"}


@app.callback(
    Output("trends-view", "options"),
    Output("trends-view", "value", allow_duplicate=True),
    Input("trends-metric", "value"),
    State("trends-view", "value"),
    prevent_initial_call=True,
)
def toggle_trends_views(metric, view):
    views = NAEP_VIEWS if metric in NAEP_METRICS else TRENDS_VIEWS
    return (
        [{"label": v, "value": k} for k, v in views.items()],
        view if view in views else "level",
    )


@app.callback(
    Output("trends-geo-level", "value"),
    Output("trends-geo", "value"),
    Output("trends-metric", "value"),
    Output("trends-inflate", "value"),
    Output("trends-segment", "value"),
    Output("trends-race", "value"),
    Output("trends-view", "value"),
    Output("trends-baseline", "value"),
    [Input(f"trends-preset-{i}", "n_clicks") for i in range(len(SUGGESTED_TRENDS))],
    prevent_initial_call=True,
)
def load_trends_preset(*_):
    s = SUGGESTED_TRENDS[_triggered_idx("trends-preset")]
    return (
        s["geo_level"],
        s["geo"],
        s["metric"],
        s["inflate"],
        s.get("segment", "geo"),
        s.get("race", RACE_DEFAULTS),
        s.get("view", "level"),
        s.get("baseline", "self"),
    )


@app.callback(
    Output("trends-chart", "figure"),
    Input("trends-geo-level", "value"),
    Input("trends-geo", "value"),
    Input("trends-metric", "value"),
    Input("trends-inflate", "value"),
    Input("trends-segment", "value"),
    Input("trends-race", "value"),
    Input("trends-view", "value"),
    Input("trends-baseline", "value"),
)
def update_trends_chart(
    geo_level, geo_names, metric, inflate, segment, segment_values, view, baseline
):
    if not geo_names or not metric:
        return px.line()

    spec = SEGMENTS.get(segment)
    if spec:
        if not segment_values:
            return px.line()
        geos, key, label = spec
        df, name_col = geos[geo_level]
        plot_df = df[df[name_col].isin(geo_names) & df[key].isin(segment_values)][
            ["year", name_col, key, metric]
        ].dropna(subset=[metric])
        # One line per geography x segment value. With a single geography the
        # prefix is redundant, so label by the segment alone. astype(str) because
        # the key columns are categorical and don't support concatenation.
        values_txt = plot_df[key].astype(str)
        if len(geo_names) > 1:
            series = plot_df[name_col].astype(str) + " — " + values_txt
            legend_title = f"{geo_level} / {label}"
        else:
            series = values_txt
            legend_title = label
        plot_df = plot_df.assign(_series=series)
        color_col = "_series"
    else:
        df, name_col = TIMESERIES_GEOS[geo_level]
        plot_df = df[df[name_col].isin(geo_names)][["year", name_col, metric]].dropna(
            subset=[metric]
        )
        color_col, legend_title = name_col, geo_level

    if inflate:
        plot_df = _apply_cpi(plot_df)

    # The view list swaps on metric change, but this callback can fire first
    # with the old view; never draw a ratio or index of NAEP scores.
    views = NAEP_VIEWS if metric in NAEP_METRICS else TRENDS_VIEWS
    if view not in views:
        view = "level"

    baseline_map = None
    if view in ("ratio", "diff"):
        # All-races value for the same year, from the geography itself or the US
        if baseline == "us":
            baseline_map = plot_df["year"].map(_us_baseline(metric))
        else:
            keyed = _keyed_baseline(geo_level, metric)
            baseline_map = pd.Series(
                pd.MultiIndex.from_arrays(
                    [plot_df[name_col].astype(str), plot_df["year"]]
                ).map(keyed),
                index=plot_df.index,
            )
        baseline_map = pd.to_numeric(baseline_map, errors="coerce")
        if view == "ratio":
            baseline_map = baseline_map.replace(0, np.nan)

    plot_df, y_title, y_fmt = _apply_trends_view(
        plot_df, metric, color_col, view, baseline_map
    )
    plot_df = plot_df.dropna(subset=[metric])
    if plot_df.empty:
        return px.line()

    # Each ACS 5-year point pools the prior 5 years; surface that in the hover
    # so "2024" isn't misread as a single calendar year. NAEP is a single-year
    # assessment, ticked only on the years it ran.
    if metric in NAEP_METRICS:
        source = plot_df["year"].map(lambda y: f"NAEP {y}")
        xaxis = {"tickvals": sorted(plot_df["year"].unique()), "title": "Assessment year"}
    else:
        source = plot_df["year"].map(_source_label)
        # Every other year up to a 30-year span; past that (back to 1790) let
        # plotly pick, or the labels pile up.
        span = plot_df["year"].max() - plot_df["year"].min()
        xaxis = {"title": "Year (ACS: end of 5-year window)"}
        if span <= 30:
            xaxis["dtick"] = 2
    plot_df = plot_df.assign(_window=source)
    fig = px.line(
        plot_df,
        x="year",
        y=metric,
        color=color_col,
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
        custom_data=["_window"],
    )
    fig.update_traces(
        hovertemplate=(
            f"%{{fullData.name}}<br>%{{customdata[0]}}<br>"
            f"%{{y:{y_fmt['tickformat']}}}"
            "<extra></extra>"
        )
    )
    yaxis = {**y_fmt, "title": y_title}
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis=xaxis,
        yaxis=yaxis,
        legend_title=legend_title,
    )
    # The qualitative palette only has 8 colours, so beyond that lines start
    # sharing colours and the chart stops being readable.
    n_series = plot_df[color_col].nunique()
    if n_series > _MAX_TREND_SERIES:
        fig.add_annotation(
            text=(
                f"⚠ {n_series} series — colours repeat above "
                f"{_MAX_TREND_SERIES}. Narrow the selection to compare reliably."
            ),
            xref="paper",
            yref="paper",
            x=0,
            y=1.06,
            showarrow=False,
            font={"size": 11, "color": "#b26a00"},
            align="left",
        )
    if view == "ratio":
        fig.add_hline(y=1, line_dash="dot", line_color="#999")
    elif view == "diff":
        fig.add_hline(y=0, line_dash="dot", line_color="#999")
    elif view == "indexed":
        fig.add_hline(y=100, line_dash="dot", line_color="#999")
    return fig


@app.callback(
    Output("corr-metrics", "options"),
    Output("corr-metrics", "value"),
    Input("corr-geo-level", "value"),
    [Input(f"corr-group-{i}", "n_clicks") for i in range(len(CORR_METRIC_GROUPS))],
)
def update_corr_options(geo_level, *_group_clicks):
    _, cols = CORR_GEOS[geo_level]
    opts = _make_options(cols)
    triggered = callback_context.triggered[0]["prop_id"]
    if "corr-group" in triggered:
        idx = _triggered_idx("corr-group")
        group_key = list(CORR_METRIC_GROUPS.keys())[idx]
        value = [m for m in CORR_METRIC_GROUPS[group_key] if m in cols]
    else:
        value = [m for m in CORR_METRIC_GROUPS[next(iter(CORR_METRIC_GROUPS))] if m in cols]
    return opts, value


@app.callback(
    Output("corr-matrix", "figure"),
    Input("corr-geo-level", "value"),
    Input("corr-metrics", "value"),
)
def update_corr_matrix(geo_level, selected_metrics):
    if not selected_metrics or len(selected_metrics) < 2:
        return px.imshow([[]], title="Select at least 2 metrics")
    df, _ = CORR_GEOS[geo_level]
    available = [m for m in selected_metrics if m in df.columns]
    corr = df[available].corr()
    labels = [_metric_label(c) for c in corr.columns]
    corr.index = labels
    corr.columns = labels
    n = len(labels)
    fig = px.imshow(
        corr,
        color_continuous_scale="RdBu",
        zmin=-1,
        zmax=1,
        text_auto=".2f",
        aspect="auto",
    )
    fig.update_traces(textfont_size=max(7, min(12, 120 // n)))
    tick_size = max(8, min(12, 120 // n))
    fig.update_layout(
        margin={"l": 20, "r": 20, "t": 30, "b": 20},
        coloraxis_colorbar={"title": "r", "tickformat": ".1f"},
        xaxis={"tickangle": -35, "tickfont_size": tick_size},
        yaxis={"tickfont_size": tick_size},
    )
    return fig


_btn_active_style = {
    **_btn_style,
    "background": "#d0e4f7",
    "borderColor": "#4a90d9",
    "fontWeight": "bold",
}

def _triggered_idx(btn_prefix):
    """Index of the preset button that fired the current callback."""
    triggered = callback_context.triggered[0]["prop_id"]
    return int(triggered.replace(f"{btn_prefix}-", "").split(".")[0])


def _register_preset_highlight(btn_prefix, store_id, n):
    """Wire a row of preset buttons to a click-to-toggle highlight.

    btn_prefix — button ids are f"{btn_prefix}-{i}"
    store_id   — dcc.Store holding the active index (None when nothing is active)
    """

    @app.callback(
        Output(store_id, "data"),
        [Input(f"{btn_prefix}-{i}", "n_clicks") for i in range(n)],
        State(store_id, "data"),
        prevent_initial_call=True,
    )
    def _update_active(*args):
        *_, current = args
        idx = _triggered_idx(btn_prefix)
        return None if current == idx else idx

    @app.callback(
        [Output(f"{btn_prefix}-{i}", "style") for i in range(n)],
        Input(store_id, "data"),
    )
    def _highlight(active):
        return [_btn_active_style if i == active else _btn_style for i in range(n)]


@app.callback(Output("ap-geo", "options"), Input("ap-geo-level", "value"))
def update_ap_geo_options(level):
    return _age_profile_geos(level)


@app.callback(
    Output("ap-sex", "options"),
    Output("ap-sex", "value", allow_duplicate=True),
    Input("ap-metric", "value"),
    State("ap-sex", "value"),
    prevent_initial_call=True,
)
def toggle_ap_sex(metric, sex):
    if metric in AGE_PROFILE_HOUSEHOLDER_METRICS:
        sex = "Both"
    return _age_profile_sex_options(metric), sex


@app.callback(
    Output("ap-geo-level", "value"),
    Output("ap-geo", "value"),
    Output("ap-metric", "value"),
    Output("ap-sex", "value"),
    [Input(f"ap-preset-{i}", "n_clicks") for i in range(len(SUGGESTED_AGE_PROFILES))],
    prevent_initial_call=True,
)
def load_ap_preset(*_):
    s = SUGGESTED_AGE_PROFILES[_triggered_idx("ap-preset")]
    return s["geo_level"], s["geo"], s["metric"], s["sex"]


@app.callback(
    Output("ap-chart", "figure"),
    Input("ap-geo-level", "value"),
    Input("ap-geo", "value"),
    Input("ap-metric", "value"),
    Input("ap-sex", "value"),
)
def update_ap_chart(level, geo_names, metric, sex):
    if not geo_names or not metric:
        return px.line()
    # The sex picker can lag a metric change by one callback; householder
    # metrics only exist for Both.
    if metric in AGE_PROFILE_HOUSEHOLDER_METRICS:
        sex = "Both"
    sexes = ["Male", "Female"] if sex == "compare" else [sex]
    df = age_profile[
        (age_profile["level"] == level)
        & age_profile["NAME"].isin(geo_names)
        & (age_profile["metric"] == metric)
        & age_profile["sex"].isin(sexes)
    ]
    if df.empty:
        return px.line()
    names, sex_txt = df["NAME"].astype(str), df["sex"].astype(str)
    if sex == "compare":
        series = names + " — " + sex_txt if len(geo_names) > 1 else sex_txt
    else:
        series = names
    df = df.assign(_series=series, age=df["age"].astype(str))
    ages = df.sort_values("age_order")["age"].unique().tolist()
    # Legend in a stable order; lines drawn youngest to oldest.
    df = df.sort_values(["_series", "age_order"])
    fmt = _axis_fmt(metric)
    fig = px.line(
        df,
        x="age",
        y="value",
        color="_series",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
        category_orders={"age": ages},
    )
    fig.update_traces(
        hovertemplate=(
            f"%{{fullData.name}}<br>Age %{{x}}<br>"
            f"%{{y:{_hover_fmt(metric)}}}<extra></extra>"
        )
    )
    is_householder = metric in AGE_PROFILE_HOUSEHOLDER_METRICS
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Age of householder" if is_householder else "Age", "type": "category"},
        yaxis=fmt,
        legend_title="Sex" if sex == "compare" and len(geo_names) == 1 else level,
    )
    return fig


_HOUSING_NOTES = {
    "trends": (
        "ACS 5-Year Estimates; each point pools five years (2024 = 2020–24). "
        "Headship and ownership count householders aged 25–34 per person aged "
        "25–34, so they fall when young adults live with parents or roommates. "
        "Price-to-income is median home value over median household income; the "
        "earliest windows include the 2005–07 housing bubble."
    ),
    "metro": (
        "Each dot is a metro area (ACS 2020–24), sized by population. The "
        "regulation index is the Wharton Residential Land Use Regulatory Index, "
        "from a 2018 survey of local governments (Gyourko, Hartley & Krimmel, "
        "NBER w26573), averaged over each metro's responding jurisdictions; only "
        "the {n} metros with at least 5 responses are shown. Higher = more "
        "restrictive. This is an association, not a measured effect: heavily "
        "regulated metros also tend to be coastal and short on buildable land, "
        "and expensive places may adopt more rules."
    ),
    "payment": (
        "Monthly principal and interest on the median home value, with 20% down "
        "on a 30-year fixed loan at that year's average rate (Freddie Mac, via "
        "FRED), as a share of median household income. Leaves out taxes and "
        "insurance, and owners who locked in a lower rate pay less. ACS 1-Year "
        "Estimates, so single years are noisier than the 5-year trends; there "
        "is no 2020 release. Home values are owner-reported. Rising rates and "
        "rising prices often go together, so this doesn't show rates causing "
        "price changes."
    ),
    "ratescatter": (
        "Each dot is one state (plus DC and Puerto Rico) in one year, ACS 1-Year "
        "Estimates 2005–2025 (no 2020 release). The rate is the national 30-year "
        "fixed average (Freddie Mac, via FRED), so each year's states line up in "
        "a column at that year's rate. Home values are owner-reported and in 2025 "
        "dollars. Most of the vertical spread is between states, not years. "
        "There's no trendline on purpose: across years, higher rates have gone "
        "with higher prices, "
        "because rates and prices both follow the economy. During the 2006–12 "
        "bust, the price crash pulled rates down, and in 2022–25 rates jumped "
        "while prices kept rising."
    ),
}


@app.callback(
    Output("housing-view", "value"),
    Output("housing-metric", "value"),
    Output("housing-geo", "value"),
    [Input(f"housing-preset-{i}", "n_clicks") for i in range(len(SUGGESTED_HOUSING))],
    State("housing-metric", "value"),
    State("housing-geo", "value"),
    prevent_initial_call=True,
)
def load_housing_preset(*args):
    *_, metric, geo = args
    s = SUGGESTED_HOUSING[_triggered_idx("housing-preset")]
    # Metro presets leave the state picker as it was; the payment preset, the metric.
    return s["view"], s["metric"] or metric, s["geo"] or geo


@app.callback(
    Output("housing-geo-controls", "style"),
    Output("housing-metric-controls", "style"),
    Output("housing-rate-y-controls", "style"),
    Input("housing-view", "value"),
)
def toggle_housing_controls(view):
    hide = {"display": "none"}
    return (
        hide if view in ("metro", "ratescatter") else {},
        hide if view in ("payment", "ratescatter") else {},
        {} if view == "ratescatter" else hide,
    )


@app.callback(
    Output("housing-chart", "figure"),
    Output("housing-note", "children"),
    Input("housing-view", "value"),
    Input("housing-metric", "value"),
    Input("housing-geo", "value"),
    Input("housing-rate-y", "value"),
)
def update_housing_chart(view, metric, geo_names, rate_y):
    # Several of these move within a few points, where ".0%" ticks repeat.
    yaxis = {**_axis_fmt(metric), "tickformat": _hover_fmt(metric)}
    if view == "metro":
        return _housing_metro_fig(metric, yaxis)
    # All states regardless of the (hidden) picker, so check before the empty-picker case.
    if view == "ratescatter":
        return _housing_rate_scatter_fig(rate_y), _HOUSING_NOTES["ratescatter"]
    if not geo_names:
        return px.line(), _HOUSING_NOTES[view]
    if view == "payment":
        return _housing_payment_fig(geo_names), _HOUSING_NOTES["payment"]
    df = housing_state[housing_state["state"].isin(geo_names)].copy()
    df["state"] = df["state"].astype(str)
    df["window"] = df["year"].map(_acs5_window)
    fig = px.line(
        df.sort_values(["state", "year"]),
        x="year",
        y=metric,
        color="state",
        markers=True,
        custom_data=["window"],
        color_discrete_sequence=px.colors.qualitative.Set2,
        category_orders={"state": [g for g in HOUSING_STATES if g in geo_names]},
    )
    fig.update_traces(
        hovertemplate=(
            f"%{{fullData.name}}<br>ACS %{{customdata[0]}}<br>"
            f"%{{y:{_hover_fmt(metric)}}}<extra></extra>"
        )
    )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "ACS 5-year estimate (end year)", "dtick": 1},
        yaxis=yaxis,
        legend_title="State",
    )
    return fig, _HOUSING_NOTES["trends"]


def _housing_payment_fig(geo_names):
    metric = "pct_payment_to_income"
    df = housing_payment[housing_payment["state"].isin(geo_names)]
    fig = px.line(
        df.sort_values(["state", "year"]),
        x="year",
        y=metric,
        color="state",
        markers=True,
        custom_data=["monthly_payment", "Median Home Value", "mortgage_rate"],
        color_discrete_sequence=px.colors.qualitative.Set2,
        category_orders={"state": [g for g in HOUSING_STATES if g in geo_names]},
    )
    fig.update_traces(
        hovertemplate=(
            "%{fullData.name}, %{x}<br>"
            f"%{{y:{_hover_fmt(metric)}}} of income<br>"
            "$%{customdata[0]:,.0f}/mo on a $%{customdata[1]:,.0f} home "
            "at %{customdata[2]:.2f}%<extra></extra>"
        )
    )
    # The rate is national, so one line from any state's rows.
    rates = housing_payment.drop_duplicates("year").sort_values("year")
    fig.add_scatter(
        x=rates["year"],
        y=rates["mortgage_rate"],
        name=_metric_label("mortgage_rate"),
        yaxis="y2",
        mode="lines",
        line={"color": "#999", "dash": "dash"},
        hovertemplate="30-yr rate, %{x}: %{y:.2f}%<extra></extra>",
    )
    fig.update_layout(
        margin={"l": 40, "r": 40, "t": 20, "b": 40},
        xaxis={"title": "ACS 1-year estimate", "dtick": 1, "range": [2004.5, 2025.5]},
        yaxis={**_axis_fmt(metric), "rangemode": "tozero"},
        yaxis2={
            "title": _metric_label("mortgage_rate"),
            "overlaying": "y",
            "side": "right",
            "ticksuffix": "%",
            "rangemode": "tozero",
            "showgrid": False,
        },
        # Above the plot: a side legend would cover the rate axis.
        legend={"orientation": "h", "y": 1.02, "x": 0, "yanchor": "bottom", "title": ""},
    )
    return fig


def _housing_rate_scatter_fig(y):
    """Every state-year as a dot. The rate is national, so each
    year's states stack in a column at that year's rate. No trendline — the
    year-to-year correlation is positive and would read as causal."""
    df = housing_payment[housing_payment["state"] != US_LABEL].sort_values("year")
    is_dollar = y == "real_home_value"
    y_fmt = "$,.0f" if is_dollar else ".1f"
    fig = px.scatter(
        df,
        x="mortgage_rate",
        y=y,
        hover_name="state",
        custom_data=["year"],
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b>, %{customdata[0]}<br>Rate: %{x:.2f}%<br>"
            f"{HOUSING_RATE_Y[y]}: %{{y:{y_fmt}}}<extra></extra>"
        ),
        marker={"size": 7, "opacity": 0.7, "line": {"width": 0.5, "color": "white"}},
    )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"title": _metric_label("mortgage_rate"), "ticksuffix": "%"},
        yaxis={"title": HOUSING_RATE_Y[y], "tickformat": y_fmt},
    )
    return fig


def _housing_metro_fig(metric, yaxis):
    df = housing_metro.dropna(subset=["wrluri", metric])
    df = df.assign(name=df["NAME"].str.replace(" Metro Area", "", regex=False))
    fig = px.scatter(
        df,
        x="wrluri",
        y=metric,
        size="Pop",
        size_max=40,
        hover_name="name",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b><br>"
            f"{_metric_label('wrluri')}: %{{x:.2f}}<br>"
            f"{_metric_label(metric)}: %{{y:{_hover_fmt(metric)}}}<extra></extra>"
        ),
        marker={"opacity": 0.7, "line": {"width": 0.5, "color": "white"}},
    )
    trend = _compute_trendline(df, "wrluri", metric)
    if trend is not None:
        x_line, y_line, _, _, r2 = trend
        r = np.sign(np.corrcoef(df["wrluri"], df[metric])[0, 1]) * np.sqrt(r2)
        fig.add_scatter(
            x=x_line,
            y=y_line,
            mode="lines",
            line={"color": "#555", "dash": "dash"},
            name=f"Trend (r = {r:+.2f}, unweighted)",
            hoverinfo="skip",
        )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={**_axis_fmt("wrluri"), "title": "← less restrictive    "
               f"{_metric_label('wrluri')}    more restrictive →"},
        yaxis=yaxis,
        legend={"orientation": "h", "y": 1.02, "x": 0},
    )
    return fig, _HOUSING_NOTES["metro"].format(n=len(df))


_ELECTION_NOTES = {
    "senate": (
        "Each race starts from fundamentals (the state's presidential lean, whether "
        "an incumbent is running, and the national generic ballot, averaged over 12 "
        "versions of the model) and moves toward the average of nonpartisan polls "
        "from the last 6–30 days. How far depends on how polled the race is: "
        "1998–2022 races put 85% of the weight on polls when there were two or "
        "more, 39% when there was one. Polls: Wikipedia race pages; history: "
        "FiveThirtyEight. Independents in Idaho, Montana, Nebraska and South "
        "Dakota count as the anti-Republican candidate but not toward a Democratic "
        "majority. Backtest on the 2010–2022 midterms: 124 of 134 races called "
        "(118 on fundamentals alone)."
    ),
    "house": (
        "Each district: its lean in the 2024 presidential election on the 2026 lines "
        "(nine states redrew; Missouri's and Virginia's new maps were blocked), "
        "whether an incumbent is running, and the national mood from the generic "
        "ballot. District leans: The Downballot. Seats with only one major party on "
        "the ballot are counted for that party. Louisiana's House general election "
        "is December 12. Backtest on the 2010–2022 midterms: 1,452 of 1,557 "
        "contested races called."
    ),
}


def _seat_summary(seats, need):
    """Chance of reaching need, and the median and 80% range of a seat distribution."""
    seats = seats.sort_values("seats")
    cdf = seats["prob"].cumsum()
    lo, med, hi = (int(seats["seats"][cdf >= q].iloc[0]) for q in (0.1, 0.5, 0.9))
    p = seats.loc[seats["seats"] >= need, "prob"].sum()
    return p, med, lo, hi


_SHOW, _HIDE = {"display": "block"}, {"display": "none"}


@app.callback(
    Output("election-chart", "figure"),
    Output("election-summary", "children"),
    Output("election-note", "children"),
    Output("election-forecast-controls", "style"),
    Output("election-backtest-controls", "style"),
    Input("election-mode", "value"),
    Input("election-view", "value"),
    Input("backtest-chamber", "value"),
    Input("backtest-view", "value"),
    Input("backtest-year", "value"),
)
def update_election_chart(mode, view, bt_chamber, bt_view, bt_year):
    if mode == "backtest":
        fig, summary = _backtest_fig(bt_chamber, bt_view, bt_year)
        return fig, summary, _BACKTEST_NOTES[bt_chamber], _HIDE, _SHOW
    chamber, kind = view.split("_")
    need = ELECTION_MAJORITY[chamber]
    p, med, lo, hi = _seat_summary(forecast_seats[chamber], need)
    meta = forecast_seats[chamber].iloc[0]
    gb = 2 * meta["generic_ballot_dem2"] - 1
    summary = [
        html.Div(f"Democrats win {chamber.title()}: {p:.0%}", style=_bold),
        html.Div(f"Democratic seats: {med} (80% range {lo}–{hi}); {need} needed"),
        html.Div(
            f"As of {meta['as_of']}; generic ballot D+{gb * 100:.1f} (two-party)",
            style={"marginTop": "8px", "color": "#555"},
        ),
    ]
    fig = _election_seats_fig(chamber) if kind == "seats" else _election_races_fig(chamber)
    return fig, summary, _ELECTION_NOTES[chamber], _SHOW, _HIDE


def _election_seats_fig(chamber):
    seats = forecast_seats[chamber]
    need = ELECTION_MAJORITY[chamber]
    fig = go.Figure(
        go.Bar(
            x=seats["seats"],
            y=seats["prob"],
            marker_color=[DEM_COLOR if s >= need else REP_COLOR for s in seats["seats"]],
            hovertemplate="%{x} Democratic seats: %{y:.1%}<extra></extra>",
        )
    )
    fig.add_vline(x=need - 0.5, line_dash="dash", line_color="#555",
                  annotation_text=f"{need} for a majority", annotation_position="top")
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        xaxis={"title": "Democratic seats (simulated)"},
        yaxis={"title": "Probability", "tickformat": ".0%"},
        bargap=0.05,
    )
    return fig


def _election_races_fig(chamber):
    df = forecast_races[chamber].copy()
    if chamber == "house":
        df = df[df["p_dem"].between(0.2, 0.8)]
        df["race"] = df["state_po"] + "-" + df["district"].astype(str).str.zfill(2)
    else:
        df["race"] = df["state_po"] + np.where(df["special"], " (special)", "")
    df = df.sort_values("p_dem")
    inc = df["inc"].map({1: "Democratic incumbent", -1: "Republican incumbent", 0: "Open seat"})
    dem = df["dem_side"] if chamber == "senate" else df["dem"]
    if chamber == "senate":
        polls = df["n_polls"].astype(int)
        inc = inc + np.where(
            polls > 0,
            "<br>Poll average: " + (100 * df["poll"]).round(1).astype(str)
            + "% D two-party (" + polls.astype(str) + np.where(polls == 1, " poll)", " polls)"),
            "<br>No recent nonpartisan polls",
        )
    fig = go.Figure(
        go.Bar(
            x=df["p_dem"],
            y=df["race"],
            orientation="h",
            marker_color=np.where(df["p_dem"] >= 0.5, DEM_COLOR, REP_COLOR),
            customdata=np.column_stack([dem, df["rep"], inc, df["pred"], df["lean"]]),
            hovertemplate=(
                "<b>%{y}</b>: %{customdata[0]} (D) vs %{customdata[1]} (R)<br>"
                "%{customdata[2]}<br>P(Democrat wins): %{x:.0%}<br>"
                "Expected Democratic two-party share: %{customdata[3]:.1%}<br>"
                "Presidential lean vs nation: %{customdata[4]:+.1%}<extra></extra>"
            ),
        )
    )
    fig.add_vline(x=0.5, line_dash="dash", line_color="#555")
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"title": "Probability the Democrat (or independent) wins", "tickformat": ".0%",
               "range": [0, 1]},
        yaxis={"title": None, "tickfont": {"size": 10}},
    )
    return fig


_BACKTEST_NOTES = {
    "senate": (
        "Each election re-forecast with today's model, using only results from "
        "earlier years, that year's final generic-ballot average (RealClearPolitics), "
        "and nonpartisan polls from the last 6–30 days (FiveThirtyEight through 2022, "
        "Wikipedia for 2024). It is not a forecast published at the time. The model "
        "was built while looking at the 2010–2022 midterms; the presidential years "
        "were never used in designing it. Races are scored on the November vote, so "
        "Georgia 2020 counts as a Republican lead though Democrats won both January "
        "runoffs; seat totals use the final result. Control takes 50 seats under a "
        "Democratic Vice President, 51 otherwise. Races with no Democrat or no "
        "Republican, or a third candidate above 15%, aren't forecast and keep their "
        "actual result in the seat totals."
    ),
    "house": (
        "Each election re-forecast with today's model, using only results from "
        "earlier years and that year's final generic-ballot average "
        "(RealClearPolitics). It is not a forecast published at the time. The model "
        "was built while looking at the 2010–2022 midterms; the presidential years "
        "were never used in designing it. House races have no polls in the model. "
        "Races with no Democrat or no Republican, or a third candidate above 15%, "
        "aren't forecast and keep their actual result in the seat totals. District "
        "lines changed over the period, so the map counts misses by state."
    ),
    "president": (
        "Each election re-forecast with today's model, using only results from "
        "earlier years: each state's lean in the previous presidential election, "
        "the final national polling average, and nonpartisan state polls from the "
        "last 6–30 days (FiveThirtyEight through 2020, Wikipedia for 2024), blended "
        "the same way as the Senate. Electoral votes: each state's House seats plus "
        "two (DC three). Maine and Nebraska are scored statewide, so totals can "
        "differ by one from the official count (2008, 2016). Simulated errors share "
        "a national shift and a regional (Census division) shift, both sized from "
        "earlier elections, which had small polling misses that barely clustered by "
        "region. So the electoral-vote ranges are too narrow, and 2016, when the "
        "misses followed education more than geography, is the clearest failure. "
        "No 2028 forecast yet."
    ),
}


@app.callback(
    Output("backtest-year", "options"),
    Output("backtest-year", "value"),
    Input("backtest-chamber", "value"),
    State("backtest-year", "value"),
)
def update_backtest_years(chamber, year):
    """Each chamber has its own election years; keep the chosen one if it has it."""
    keep = year == "all" or year in _backtest_years(chamber)
    return _backtest_year_options(chamber), year if keep else "all"


def _race_label(df, chamber):
    if chamber == "house":
        return df["state_po"] + "-" + df["district"].astype(str).str.zfill(2)
    return df["state_po"] + np.where(df["special"], " (special)", "")


def _backtest_data(chamber, year):
    """The chosen year's races (all years if year is 'all'), with labels and the call."""
    df = backtest_races[chamber]
    if year != "all":
        df = df[df["year"] == year]
    df = df.copy()
    df["race"] = _race_label(df, chamber)
    df["dem_won"] = df["dem2"] > 0.5
    df["called"] = (df["p_dem"] > 0.5) == df["dem_won"]
    return df


def _brier(p, won):
    return float(((p - won) ** 2).mean())


def _backtest_fig(chamber, view, year):
    df = _backtest_data(chamber, year)
    if view == "seats":
        fig = _backtest_seats_fig(chamber, year)
    elif view == "races":
        fig = _backtest_races_fig(df, chamber)
    elif view == "calibration":
        fig = _backtest_calibration_fig(df, chamber)
    else:
        fig = _backtest_map_fig(df, chamber, year)
    return fig, _backtest_summary(df, chamber, year)


def _backtest_summary(df, chamber, year):
    seats = backtest_seats[chamber]
    _, unit, prize = BACKTEST_CHAMBERS[chamber]
    n = len(df)
    lines = [
        html.Div(
            f"{'States' if chamber == 'president' else 'Races'} called: {df['called'].sum()} of {n} "
            f"(Brier {_brier(df['p_dem'], df['dem_won']):.3f})",
            style=_bold,
        )
    ]
    if chamber in _POLLED:
        fund = (df["p_fund"] > 0.5) == df["dem_won"]
        lines.append(html.Div(
            f"Fundamentals alone, without polls: {fund.sum()} of {n} "
            f"(Brier {_brier(df['p_fund'], df['dem_won']):.3f})"
        ))
    years = _backtest_years(chamber) if year == "all" else [year]
    inside = control_right = 0
    for y in years:
        s = seats[seats["year"] == y]
        actual, need = int(s["actual"].iloc[0]), int(s["need"].iloc[0])
        p, med, lo, hi = _seat_summary(s, need)
        inside += lo <= actual <= hi
        control_right += (p > 0.5) == (actual >= need)
        if year != "all":
            won = "Democrats" if actual >= need else "Republicans"
            lines += [
                html.Div(f"Democratic {unit}: forecast {med} (80% range {lo}–{hi}), "
                         f"actual {actual}", style={"marginTop": "8px"}),
                html.Div(f"Chance Democrats win {prize}: {p:.0%}; {won} won it "
                         f"({need} {unit} needed)"),
            ]
    if year == "all":
        lines += [
            html.Div(f"Actual {unit} inside the 80% range: {inside} of {len(years)} elections",
                     style={"marginTop": "8px"}),
            html.Div(f"Winner of {prize} called right: {control_right} of {len(years)}"),
        ]
    return lines


def _backtest_seats_fig(chamber, year):
    seats = backtest_seats[chamber]
    _, unit, prize = BACKTEST_CHAMBERS[chamber]
    win = "to win" if chamber == "president" else "for control"
    if year != "all":
        s = seats[seats["year"] == year]
        need, actual = int(s["need"].iloc[0]), int(s["actual"].iloc[0])
        fig = go.Figure(
            go.Bar(
                x=s["seats"],
                y=s["prob"],
                marker_color=[DEM_COLOR if v >= need else REP_COLOR for v in s["seats"]],
                hovertemplate=f"%{{x}} Democratic {unit}: %{{y:.1%}}<extra></extra>",
            )
        )
        fig.add_vline(x=need - 0.5, line_dash="dash", line_color="#555",
                      annotation_text=f"{need} {win}", annotation_position="top left")
        fig.add_vline(x=actual, line_color="#222", line_width=3,
                      annotation_text=f"Actual: {actual}", annotation_position="top right")
        fig.update_layout(
            margin={"l": 40, "r": 20, "t": 30, "b": 40},
            xaxis={"title": f"Democratic {unit} in {year} (simulated)"},
            yaxis={"title": "Probability", "tickformat": ".0%"},
            bargap=0.05,
        )
        return fig
    rows = []
    for y in _backtest_years(chamber):
        s = seats[seats["year"] == y]
        need, actual = int(s["need"].iloc[0]), int(s["actual"].iloc[0])
        p, med, lo, hi = _seat_summary(s, need)
        rows.append({"year": y, "med": med, "lo": lo, "hi": hi, "actual": actual, "need": need,
                     "p": p})
    r = pd.DataFrame(rows)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=r["year"], y=r["need"], mode="markers", name=f"{unit.capitalize()} {win}",
        marker={"symbol": "line-ew", "size": 28, "line": {"width": 2, "color": "#999"}},
        hovertemplate=f"%{{x}}: %{{y}} {unit} {win}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=r["year"], y=r["med"], mode="markers", name="Forecast (median, 80% range)",
        marker={"size": 10, "color": DEM_COLOR},
        error_y={"type": "data", "symmetric": False, "array": r["hi"] - r["med"],
                 "arrayminus": r["med"] - r["lo"], "color": DEM_COLOR, "thickness": 2},
        customdata=np.column_stack([r["lo"], r["hi"], r["p"]]),
        hovertemplate=(f"%{{x}}: forecast %{{y}} {unit} (80% range %{{customdata[0]}}–"
                       f"%{{customdata[1]}})<br>Chance Democrats win {prize}: "
                       "%{customdata[2]:.0%}<extra></extra>"),
    ))
    fig.add_trace(go.Scatter(
        x=r["year"], y=r["actual"], mode="markers", name="Actual",
        marker={"symbol": "x", "size": 11, "color": "#222"},
        hovertemplate=f"%{{x}}: actual %{{y}} {unit}<extra></extra>",
    ))
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        xaxis={"title": None, "tickvals": _backtest_years(chamber)},
        yaxis={"title": f"Democratic {unit}"},
        legend={"orientation": "h", "y": 1.05, "x": 0},
    )
    return fig


def _backtest_hover(df, chamber):
    """customdata and hovertemplate shared by the race-level backtest charts."""
    extra = ""
    cols = [df["year"], df["race"], df["dem_cand"].fillna("").str.title(),
            df["rep_cand"].fillna("").str.title(), df["p_dem"], df["pred"], df["dem2"]]
    if chamber in _POLLED:
        polls = df["n_polls"].fillna(0).astype(int)
        cols.append(np.where(
            polls > 0,
            "Poll average: " + (100 * df["poll"]).round(1).astype(str) + "% ("
            + polls.astype(str) + np.where(polls == 1, " poll)", " polls)"),
            "No recent nonpartisan polls",
        ))
        extra = "%{customdata[7]}<br>"
    template = (
        "<b>%{customdata[0]} %{customdata[1]}</b>: %{customdata[2]} (D) vs "
        "%{customdata[3]} (R)<br>P(Democrat wins): %{customdata[4]:.0%}<br>"
        "Forecast Democratic share: %{customdata[5]:.1%}<br>" + extra
        + "Actual: %{customdata[6]:.1%}<extra></extra>"
    )
    return np.column_stack(cols), template


def _backtest_races_fig(df, chamber):
    fig = go.Figure()
    for called, name, color in ((True, "Called right", "#999"), (False, "Missed", MISS_COLOR)):
        d = df[df["called"] == called]
        custom, template = _backtest_hover(d, chamber)
        fig.add_trace(go.Scattergl(
            x=d["pred"], y=d["dem2"], mode="markers", name=f"{name} ({len(d)})",
            marker={"color": color, "size": 5 if chamber == "house" else 7, "opacity": 0.8},
            customdata=custom, hovertemplate=template,
        ))
    lo = min(df["pred"].min(), df["dem2"].min()) - 0.02
    hi = max(df["pred"].max(), df["dem2"].max()) + 0.02
    fig.add_shape(type="line", x0=lo, y0=lo, x1=hi, y1=hi, line={"color": "#555", "dash": "dot"})
    fig.add_hline(y=0.5, line_color="#ccc")
    fig.add_vline(x=0.5, line_color="#ccc")
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        xaxis={"title": "Forecast Democratic two-party share", "tickformat": ".0%",
               "range": [lo, hi]},
        yaxis={"title": "Actual Democratic two-party share", "tickformat": ".0%",
               "range": [lo, hi]},
        legend={"orientation": "h", "y": 1.05, "x": 0},
    )
    return fig


def _backtest_calibration_fig(df, chamber):
    """Races grouped into 10-point bins by forecast probability; each dot is a
    bin, placed at its average forecast and the share Democrats won, with the
    90% range a perfectly calibrated forecast would land in given its count."""
    bins = np.linspace(0, 1, 11)
    series = [("p_dem", "With polls" if chamber in _POLLED else "Forecast", DEM_COLOR)]
    if chamber in _POLLED:
        series.append(("p_fund", "Fundamentals only", "#999"))
    fig = go.Figure()
    for lo in bins[:-1:2]:  # shade every other bin so the bin edges show
        fig.add_vrect(x0=lo, x1=lo + 0.1, fillcolor="#f0f0f0", line_width=0, layer="below")
    fig.add_shape(type="line", x0=0, y0=0, x1=1, y1=1, line={"color": "#555", "dash": "dot"})
    for col, name, color in series:
        g = df.groupby(pd.cut(df[col], bins, include_lowest=True), observed=True).agg(
            p=(col, "mean"), won=("dem_won", "mean"), n=(col, "size"))
        edges = [f"{iv.left * 100:.0f}–{iv.right * 100:.0f}%".replace("-0–", "0–")
                 for iv in g.index]
        se = 1.645 * np.sqrt(g["p"] * (1 - g["p"]) / g["n"])
        fig.add_trace(go.Scatter(
            x=g["p"], y=g["won"], mode="markers+lines", name=name,
            marker={"size": 6 + 16 * np.sqrt(g["n"] / g["n"].max()), "color": color},
            line={"color": color, "width": 1},
            error_y={"type": "data", "array": se, "color": color, "thickness": 1},
            customdata=np.column_stack([g["n"], edges]),
            hovertemplate=(f"<b>{name}</b>, forecasts %{{customdata[1]}}:<br>Democrats won "
                           "%{y:.0%} of %{customdata[0]} races (average forecast "
                           "%{x:.0%})<extra></extra>"),
        ))
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
        xaxis={"title": "Forecast chance the Democrat wins (10-point bins; each dot sits at "
                        "its bin's average forecast)", "tickformat": ".0%",
               "tickvals": list(bins), "range": [-0.02, 1.02]},
        yaxis={"title": "Share of those races Democrats won", "tickformat": ".0%",
               "range": [-0.05, 1.05]},
        legend={"orientation": "h", "y": 1.05, "x": 0},
    )
    return fig


def _backtest_map_fig(df, chamber, year):
    if chamber in _POLLED and year != "all":
        # One election: each state's outcome, a miss outranking a second race
        rank = {"Missed": 0, "Democrat won, called": 1, "Republican won, called": 2}
        df = df.assign(outcome=np.where(~df["called"], "Missed", np.where(
            df["dem_won"], "Democrat won, called", "Republican won, called")))
        df["text"] = (df["race"] + ": " + df["dem_cand"].str.title() + " (D) "
                      + (100 * df["dem2"]).round(1).astype(str) + "%, forecast "
                      + (100 * df["pred"]).round(1).astype(str) + "%, P(D) "
                      + (100 * df["p_dem"]).round(0).astype(int).astype(str) + "%")
        st = df.groupby("state_po").agg(
            outcome=("outcome", lambda x: min(x, key=rank.get)),
            text=("text", "<br>".join)).reset_index()
        fig = px.choropleth(
            st, locations="state_po", locationmode="USA-states", scope="usa",
            color="outcome", hover_name="state_po", custom_data=["text"],
            category_orders={"outcome": list(rank)},
            color_discrete_map={"Missed": MISS_COLOR, "Democrat won, called": "#92c5de",
                                "Republican won, called": "#f4a582"},
        )
        fig.update_traces(hovertemplate="<b>%{hovertext}</b><br>%{customdata[0]}<extra></extra>")
    else:
        miss = df[~df["called"]]
        miss_text = (miss["year"].astype(str) + " " + miss["race"] + ": forecast "
                     + (100 * miss["p_dem"]).round(0).astype(int).astype(str) + "% D, "
                     + np.where(miss["dem_won"], "Democrat", "Republican") + " won")
        # Share of each state's races missed, against the misses the model
        # itself expected (each race's chance of going the other way)
        st = df.assign(expected=np.minimum(df["p_dem"], 1 - df["p_dem"])).groupby(
            "state_po").agg(races=("race", "size"), expected=("expected", "sum"))
        st["misses"] = miss.groupby("state_po").size()
        st["misses"] = st["misses"].fillna(0).astype(int)
        st["share"] = st["misses"] / st["races"]
        st["text"] = miss_text.groupby(miss["state_po"]).agg("<br>".join)
        st["text"] = st["text"].fillna("No misses")
        st = st.reset_index()
        fig = px.choropleth(
            st, locations="state_po", locationmode="USA-states", scope="usa",
            color="share", hover_name="state_po",
            custom_data=["misses", "races", "expected", "text"],
            color_continuous_scale="Oranges", range_color=[0, max(0.05, st["share"].max())],
        )
        fig.update_traces(hovertemplate=("<b>%{hovertext}</b>: %{customdata[0]} missed of "
                                         "%{customdata[1]} races (%{z:.0%}); the model "
                                         "expected %{customdata[2]:.1f}<br>%{customdata[3]}"
                                         "<extra></extra>"))
        fig.update_layout(coloraxis_colorbar={"title": "Share missed", "tickformat": ".0%"})
    fig.update_layout(margin={"l": 0, "r": 0, "t": 30, "b": 0},
                      legend={"orientation": "h", "y": 1.02, "x": 0, "title": None})
    return fig


_register_preset_highlight("trends-preset", "trends-active-preset", len(SUGGESTED_TRENDS))
_register_preset_highlight("housing-preset", "housing-active-preset", len(SUGGESTED_HOUSING))
_register_preset_highlight(
    "scatter-preset", "scatter-active-preset", len(SUGGESTED_SCATTERS)
)
_register_preset_highlight(
    "anim-preset", "anim-active-preset", len(SUGGESTED_ANIM_SCATTERS)
)
_register_preset_highlight("corr-group", "corr-active-group", len(CORR_METRIC_GROUPS))
_register_preset_highlight("ap-preset", "ap-active-preset", len(SUGGESTED_AGE_PROFILES))


if __name__ == "__main__":
    app.run_server(debug=True)
