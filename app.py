"""Census Data Explorer — interactive demographic visualization app."""

import os
from functools import lru_cache

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, html, dcc, Input, Output, State, callback_context

# Static config: metric lists, presets, colors, CPI table, labels.
from config import (
    _MAX_TREND_SERIES,
    _PCT_METRICS,
    ACS_YEAR,
    AGE_BRACKETS,
    CORR_METRIC_GROUPS,
    CPI,
    CPI_COLS,
    DEFAULT_VAR,
    US_LABEL,
    FEMALE_COLOR,
    INCOME_COLOR,
    MALE_COLOR,
    METRIC_LABELS,
    MF_COLOR,
    RACE_DEFAULTS,
    RACE_GROUPS,
    RATIO_BASELINES,
    SUGGESTED_ANIM_SCATTERS,
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
ts_state = _read_acs("c_timeseries_state.csv")
ts_county = _read_acs("c_timeseries_county.csv", dtype={"GEOID": object})
# Categorical keys keep the long-format race frames small (463k county rows) and
# make the per-callback `isin` filters ~4x faster than object dtype.
_RACE_DTYPES = {"race": "category", "year": "int16"}
ts_state_race = _read_acs(
    "c_timeseries_state_race.csv", dtype={**_RACE_DTYPES, "state": "category"}
)
ts_county_race = _read_acs(
    "c_timeseries_county_race.csv", dtype={**_RACE_DTYPES, "NAME": "category"}
)
_AGE_DTYPES = {"age": "category", "year": "int16"}
ts_state_age = _read_acs(
    "c_timeseries_state_age.csv", dtype={**_AGE_DTYPES, "state": "category"}
)
ts_county_age = _read_acs(
    "c_timeseries_county_age.csv", dtype={**_AGE_DTYPES, "NAME": "category"}
)
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


_ts_state_defaults = ts_state.groupby("state")["Pop"].mean().nlargest(4).index.tolist()


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
            df[col] = df[col] * df[year_col].map(
                lambda y: CPI[2024] / CPI.get(y, CPI[2024])
            )
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
    return label


def _apply_trends_view(plot_df, metric, series_col, view, baseline_map=None):
    """Rescale `metric` for the chosen view. Returns (df, y_title, y_axis_fmt).

    indexed — each series divided by its own earliest non-null value x 100
    ratio   — each row divided by the all-races baseline for its geography/year
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


def _acs5_window(year):
    """ACS 5-year estimates are pooled: 2024 covers 2020-2024."""
    return f"{year - 4}–{str(year)[2:]}"


def _hover_fmt(metric):
    if metric in CPI_COLS:
        return "$,.0f"
    if metric.startswith("pct_") or metric in _PCT_METRICS:
        return ".1%"
    return ",.0f"


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
    return metric.startswith("pct_") or "_ratio" in metric or "Median" in metric


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
app = Dash(__name__)
server = app.server  # for gunicorn

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
                                            options=_make_options(TIMESERIES_METRICS),
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
                                            "ACS 5-Year Estimates. Each point pools the "
                                            "prior 5 years — 2024 covers 2020–2024 — so "
                                            "trends lag real turning points.",
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
                                            "ACS 5-Year Estimates. Each point pools the "
                                            "prior 5 years — 2024 covers 2020–2024 — so "
                                            "trends lag real turning points.",
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
            tiles="cartodb positron",
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

    folium.TileLayer(tiles="cartodb positron", control=False).add_to(m)
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
    plot_df = df[cols].dropna(subset=[x_metric, y_metric])

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
    State("trends-race", "value"),
    State("trends-metric", "value"),
    prevent_initial_call=True,
)
def toggle_trends_segment(segment, values, metric):
    """Swap the group picker and metric list to match the segment mode.

    Options and values move together: each mode supports a different set of
    metrics (home value and rent have no race iteration; age only covers
    median income), so a metric that just left the list is replaced.
    """
    if segment == "race":
        if metric not in TIMESERIES_RACE_METRICS:
            metric = "Median Household Income"
        if not set(values or []) & set(RACE_GROUPS):
            values = RACE_DEFAULTS
        return (
            {"display": "block"},
            "Race / Ethnicity",
            RACE_GROUPS,
            values,
            _make_options(TIMESERIES_RACE_METRICS),
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
    return (
        {"display": "none"},
        "Groups",
        RACE_GROUPS,
        values,
        _make_options(TIMESERIES_METRICS),
        metric,
    )


@app.callback(
    Output("trends-baseline-wrap", "style"), Input("trends-view", "value")
)
def toggle_trends_baseline(view):
    return {"display": "block"} if view == "ratio" else {"display": "none"}


@app.callback(
    Output("trends-geo-level", "value"),
    Output("trends-geo", "value"),
    Output("trends-metric", "value"),
    Output("trends-inflate", "value"),
    Output("trends-segment", "value"),
    Output("trends-race", "value"),
    Output("trends-view", "value"),
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

    baseline_map = None
    if view == "ratio":
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
        baseline_map = pd.to_numeric(baseline_map, errors="coerce").replace(0, np.nan)

    plot_df, y_title, y_fmt = _apply_trends_view(
        plot_df, metric, color_col, view, baseline_map
    )
    plot_df = plot_df.dropna(subset=[metric])
    if plot_df.empty:
        return px.line()

    # Each ACS 5-year point pools the prior 5 years; surface that in the hover
    # so "2024" isn't misread as a single calendar year.
    plot_df = plot_df.assign(_window=plot_df["year"].map(_acs5_window))
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
            f"%{{fullData.name}}<br>ACS %{{customdata[0]}} (5-yr)<br>"
            f"%{{y:{y_fmt['tickformat']}}}"
            "<extra></extra>"
        )
    )
    yaxis = {**y_fmt, "title": y_title}
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"dtick": 1, "title": "Year (end of 5-year ACS window)"},
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


_register_preset_highlight("trends-preset", "trends-active-preset", len(SUGGESTED_TRENDS))
_register_preset_highlight(
    "scatter-preset", "scatter-active-preset", len(SUGGESTED_SCATTERS)
)
_register_preset_highlight(
    "anim-preset", "anim-active-preset", len(SUGGESTED_ANIM_SCATTERS)
)
_register_preset_highlight("corr-group", "corr-active-group", len(CORR_METRIC_GROUPS))


if __name__ == "__main__":
    app.run_server(debug=True)
