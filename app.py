"""Create an interactive plot in a browser window"""

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, html, dcc, Input, Output, State, callback_context

pio.templates.default = "plotly_white"

DEFAULT_VAR = "Pop"

MALE_COLOR = "Blues"
FEMALE_COLOR = "Reds"
MF_COLOR = "RdBu"
INCOME_COLOR = "Greens"

# Load files ###################################################################################
state_geom_raw = gpd.read_file("state_geom.shp")
county_geom_raw = gpd.read_file("county_geom.shp")
zcta_geom_raw = gpd.read_file("zcta_geom.shp")
tract_geom_raw = gpd.read_file("tract_geom.shp")
block_group_geom_raw = gpd.read_file("block_group_geom.shp")

zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={"zcta": object})

dma_polygons_raw = gpd.read_file("dma_polygons.geojson")
dma_polygons_raw["cartodb_id"] = dma_polygons_raw["cartodb_id"].astype(str)
dma_polygons_raw["dma_code"] = dma_polygons_raw["dma_code"].astype(str)

dma_polygon_map = pd.read_csv("dma_polygon_map.csv")

c_state = pd.read_csv("c_state.csv")
c_dma = pd.read_csv("c_dma.csv")
ts_state = pd.read_csv("c_timeseries_state.csv")
ts_county = pd.read_csv("c_timeseries_county.csv", dtype={"GEOID": object})
c_county_state = pd.read_csv("c_county_state.csv", dtype={"GEOID": object})
c_zcta_dma = pd.read_csv("c_zcta_dma.csv", dtype={"zcta": object})
c_tract = pd.read_csv("c_tract.csv", dtype={"GEOID": object})
c_block_group = pd.read_csv("c_block_group.csv", dtype={"GEOID": object})

state_name = pd.read_csv("state_name.csv", dtype={"state": object})


def _add_price_to_rent(df):
    if "Median Home Value" in df.columns and "Median Gross Rent" in df.columns:
        df["price_to_rent_ratio"] = df["Median Home Value"] / (
            df["Median Gross Rent"] * 12
        )


for _df in [
    c_state,
    c_dma,
    c_county_state,
    c_zcta_dma,
    c_tract,
    c_block_group,
    ts_state,
    ts_county,
]:
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
county_geom_json = county_geom.to_json()

zcta_geom = zcta_geom_raw.merge(
    zcta_to_dma[["zcta", "dma"]], how="left", left_on="ZCTA5CE20", right_on="zcta"
)

tract_geom = tract_geom_raw[["GEOID", "geometry"]]
block_group_geom = block_group_geom_raw[["GEOID", "geometry"]]

# Pre-compute GeoJSON per state/city at startup to avoid re-serializing on every callback
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
tract_metric_cols = sorted(
    col
    for col in c_tract.columns
    if col not in ["state", "county", "state_NAME", "GEOID", "NAME", "tract"]
)
block_group_metric_cols = sorted(
    col
    for col in c_block_group.columns
    if col
    not in ["state", "county", "state_NAME", "GEOID", "NAME", "tract", "block group"]
)

dmas = c_dma["dma"].unique()
states = state_name["state_NAME"].unique()
cities = list(_city_fips.keys())

TIMESERIES_METRICS = [
    "Pop",
    "Median Household Income",
    "Median Home Value",
    "Median Gross Rent",
    "pct_male",
    "pct_white_nh",
    "pct_black",
    "pct_hispanic",
    "pct_asian",
    "pct_poverty",
    "pct_unemployed",
    "pct_bachelors_plus",
    "pct_owner_occupied",
    "pct_renter_occupied",
    "price_to_rent_ratio",
]

TIMESERIES_GEOS = {
    "State": (ts_state, "state"),
    "County": (ts_county, "NAME"),
}

_ts_state_defaults = ts_state.groupby("state")["Pop"].mean().nlargest(4).index.tolist()

SUGGESTED_TRENDS = [
    {
        "label": "Income Growth",
        "geo_level": "State",
        "geo": ["California", "New York", "Texas", "Florida"],
        "metric": "Median Household Income",
        "inflate": ["inflate"],
    },
    {
        "label": "Home Values",
        "geo_level": "State",
        "geo": ["California", "New York", "Texas", "Florida"],
        "metric": "Median Home Value",
        "inflate": ["inflate"],
    },
    {
        "label": "Rent Pressure",
        "geo_level": "State",
        "geo": ["California", "New York", "Texas", "Florida"],
        "metric": "Median Gross Rent",
        "inflate": ["inflate"],
    },
    {
        "label": "Poverty Trends",
        "geo_level": "State",
        "geo": ["California", "New York", "Texas", "Mississippi"],
        "metric": "pct_poverty",
        "inflate": [],
    },
    {
        "label": "Education Gains",
        "geo_level": "State",
        "geo": ["California", "New York", "Texas", "Florida"],
        "metric": "pct_bachelors_plus",
        "inflate": [],
    },
    {
        "label": "Latino Growth",
        "geo_level": "State",
        "geo": ["California", "Texas", "Florida", "Arizona"],
        "metric": "pct_hispanic",
        "inflate": [],
    },
    {
        "label": "County Home Values",
        "geo_level": "County",
        "geo": [
            "Los Angeles County, California",
            "King County, Washington",
            "Travis County, Texas",
            "Mecklenburg County, North Carolina",
            "Maricopa County, Arizona",
        ],
        "metric": "Median Home Value",
        "inflate": ["inflate"],
    },
    {
        "label": "County Income Growth",
        "geo_level": "County",
        "geo": [
            "Los Angeles County, California",
            "King County, Washington",
            "Travis County, Texas",
            "Mecklenburg County, North Carolina",
            "Maricopa County, Arizona",
        ],
        "metric": "Median Household Income",
        "inflate": ["inflate"],
    },
    {
        "label": "County Rent Pressure",
        "geo_level": "County",
        "geo": [
            "Los Angeles County, California",
            "Miami-Dade County, Florida",
            "Cook County, Illinois",
            "King County, Washington",
            "Denver County, Colorado",
        ],
        "metric": "Median Gross Rent",
        "inflate": ["inflate"],
    },
    {
        "label": "County Education Trend",
        "geo_level": "County",
        "geo": [
            "Travis County, Texas",
            "Fulton County, Georgia",
            "Wake County, North Carolina",
            "Mecklenburg County, North Carolina",
            "Denver County, Colorado",
        ],
        "metric": "pct_bachelors_plus",
        "inflate": [],
    },
]

SUGGESTED_ANIM_SCATTERS = [
    {
        "label": "Poverty vs Income",
        "geo_level": "State",
        "x": "pct_poverty",
        "y": "Median Household Income",
        "color": "pct_black",
        "size": "Pop",
    },
    {
        "label": "Home Affordability",
        "geo_level": "State",
        "x": "Median Household Income",
        "y": "Median Home Value",
        "color": "pct_owner_occupied",
        "size": "Pop",
    },
    {
        "label": "Education vs Poverty",
        "geo_level": "State",
        "x": "pct_bachelors_plus",
        "y": "pct_poverty",
        "color": "pct_black",
        "size": "Pop",
    },
    {
        "label": "Rent vs Income",
        "geo_level": "County",
        "x": "Median Household Income",
        "y": "Median Gross Rent",
        "color": "pct_renter_occupied",
        "size": "Pop",
    },
    {
        "label": "Diversity Shift",
        "geo_level": "State",
        "x": "pct_white_nh",
        "y": "pct_hispanic",
        "color": "Median Household Income",
        "size": "Pop",
    },
]

# Scatter geography config: name → (dataframe, label_col, metric_cols)
SCATTER_GEOS = {
    "State": (c_state, "state", state_metric_cols),
    "DMA": (c_dma, "dma", dma_metric_cols),
    "County": (c_county_state, "NAME", county_metric_cols),
    "ZCTA": (c_zcta_dma, "zcta", zcta_metric_cols),
}

CORR_GEOS = {
    "State": (c_state, state_metric_cols),
    "DMA": (c_dma, dma_metric_cols),
    "County": (c_county_state, county_metric_cols),
    "ZCTA": (c_zcta_dma, zcta_metric_cols),
}

CORR_METRIC_GROUPS = {
    "Key Metrics": [
        "pct_white_nh",
        "pct_black",
        "pct_hispanic",
        "pct_asian",
        "pct_poverty",
        "pct_unemployed",
        "pct_bachelors_plus",
        "pct_owner_occupied",
        "Household Income 200+_ratio",
        "pct_male",
    ],
    "Demographics": [
        "pct_white_nh",
        "pct_black",
        "pct_hispanic",
        "pct_asian",
        "pct_aian",
        "pct_nhpi",
        "pct_other_race",
        "pct_two_or_more",
    ],
    "Economics": [
        "pct_poverty",
        "pct_unemployed",
        "pct_bachelors_plus",
        "Household Income 200+_ratio",
        "pct_male",
        "pct_male_20 to 29 years",
        "pct_male_30 to 39 years",
    ],
    "Housing": [
        "pct_owner_occupied",
        "pct_renter_occupied",
        "Household Income 200+_ratio",
        "pct_poverty",
        "pct_white_nh",
        "pct_black",
    ],
}

# Suggested scatter presets
SUGGESTED_SCATTERS = [
    {
        "label": "Race & Income",
        "geo": "County",
        "x": "pct_black",
        "y": "Household Income 200+_ratio",
        "color": "pct_hispanic",
        "size": "Pop",
    },
    {
        "label": "DMA Overview",
        "geo": "DMA",
        "x": "pct_white_nh",
        "y": "Household Income 200+_ratio",
        "color": "pct_black",
        "size": "Pop",
    },
    {
        "label": "Young Adult Hubs",
        "geo": "County",
        "x": "pct_male_20 to 29 years",
        "y": "Household Income 200+_ratio",
        "color": "pct_hispanic",
        "size": "Pop",
    },
    {
        "label": "Gentrification",
        "geo": "ZCTA",
        "x": "pct_renter_occupied",
        "y": "Median Gross Rent",
        "color": "pct_bachelors_plus",
        "size": "Pop",
    },
    {
        "label": "Home Affordability",
        "geo": "County",
        "x": "Median Household Income",
        "y": "Median Home Value",
        "color": "pct_owner_occupied",
        "size": "Pop",
    },
    {
        "label": "Education & Poverty",
        "geo": "County",
        "x": "pct_bachelors_plus",
        "y": "pct_poverty",
        "color": "pct_black",
        "size": "Pop",
    },
    {
        "label": "Brain Drain",
        "geo": "State",
        "x": "pct_bachelors_plus",
        "y": "pct_unemployed",
        "color": "pct_poverty",
        "size": "Pop",
    },
    {
        "label": "Homeownership Gap",
        "geo": "County",
        "x": "pct_black",
        "y": "pct_owner_occupied",
        "color": "Median Home Value",
        "size": "Pop",
    },
    {
        "label": "Value vs Income",
        "geo": "ZCTA",
        "x": "Median Household Income",
        "y": "Median Home Value",
        "color": "pct_bachelors_plus",
        "size": "Pop",
    },
    {
        "label": "Rent vs Value",
        "geo": "ZCTA",
        "x": "Median Home Value",
        "y": "Median Gross Rent",
        "color": "pct_owner_occupied",
        "size": "Pop",
    },
    {
        "label": "Upside Markets",
        "geo": "County",
        "x": "Median Household Income",
        "y": "Median Home Value",
        "color": "pct_bachelors_plus",
        "size": "Pop",
    },
    {
        "label": "Gentrification Risk",
        "geo": "ZCTA",
        "x": "pct_poverty",
        "y": "pct_bachelors_plus",
        "color": "Median Gross Rent",
        "size": "Pop",
    },
]

_btn_style = {
    "padding": "5px 12px",
    "cursor": "pointer",
    "fontFamily": "Arial",
    "fontSize": "13px",
    "borderRadius": "4px",
    "border": "1px solid #ccc",
    "background": "#f8f8f8",
}

# CPI-U annual averages (BLS, all items) — used to express dollar metrics in 2022 dollars
CPI = {
    2009: 214.537,
    2010: 218.056,
    2011: 224.939,
    2012: 229.594,
    2013: 232.957,
    2014: 236.736,
    2015: 237.017,
    2016: 240.007,
    2017: 245.120,
    2018: 251.107,
    2019: 255.657,
    2020: 258.811,
    2021: 270.970,
    2022: 292.655,
    2023: 304.702,
    2024: 314.175,
}
CPI_COLS = ["Median Household Income", "Median Home Value", "Median Gross Rent"]


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


METRIC_LABELS = {
    "pct_male": "% Male",
    "pct_white_alone": "% White (Alone)",
    "pct_white_nh": "% White Non-Hispanic",
    "pct_black": "% Black or African American",
    "pct_hispanic": "% Hispanic or Latino",
    "pct_asian": "% Asian",
    "pct_aian": "% American Indian / Alaska Native",
    "pct_nhpi": "% Native Hawaiian / Pacific Islander",
    "pct_other_race": "% Some Other Race",
    "pct_two_or_more": "% Two or More Races",
    "pct_poverty": "% Below Poverty Line",
    "pct_unemployed": "% Unemployed (of Labor Force)",
    "pct_bachelors_plus": "% Bachelor's Degree or Higher",
    "pct_owner_occupied": "% Owner-Occupied Housing",
    "pct_renter_occupied": "% Renter-Occupied Housing",
    "Household Income 200+_ratio": "% Households Income $200k+",
    "price_to_rent_ratio": "Price-to-Rent Ratio",
}


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


_PCT_METRICS = {"Household Income 200+_ratio"}


def _axis_fmt(metric):
    if metric in CPI_COLS:
        return {"tickprefix": "$", "tickformat": ",.0f"}
    if metric.startswith("pct_") or metric in _PCT_METRICS:
        return {"tickformat": ".0%"}
    return {}


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
    return (
        metric.startswith("pct_")
        or "_ratio" in metric
        or "Median" in metric
        or metric in ("Median Home Value", "Median Gross Rent")
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
app = Dash(__name__)
server = app.server  # for gunicorn

_tab_style = {"fontFamily": "Arial"}

app.layout = html.Div(
    [
        html.H1("Census Data Explorer", style={"fontFamily": "Arial"}),
        dcc.Store(id="trends-active-preset", data=None),
        dcc.Store(id="scatter-active-preset", data=None),
        dcc.Store(id="anim-active-preset", data=None),
        dcc.Tabs(
            [
                dcc.Tab(
                    label="States",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style={
                                                "fontFamily": "Arial",
                                                "fontWeight": "bold",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="state-metric-selector",
                                            options=_make_options(state_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                            style={"fontFamily": "Arial"},
                                        ),
                                        _normalize_checkbox("state-normalize"),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="state_map", width="100%", height="700"
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
                        )
                    ],
                ),
                dcc.Tab(
                    label="DMAs",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style={
                                                "fontFamily": "Arial",
                                                "fontWeight": "bold",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="dma-metric-selector",
                                            options=_make_options(dma_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                            style={"fontFamily": "Arial"},
                                        ),
                                        _normalize_checkbox("dma-normalize"),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="dma_map", width="100%", height="700"
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
                        )
                    ],
                ),
                dcc.Tab(
                    label="Counties",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style={
                                                "fontFamily": "Arial",
                                                "fontWeight": "bold",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="county-metric-selector",
                                            options=_make_options(county_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                            style={"fontFamily": "Arial"},
                                        ),
                                        _normalize_checkbox("county-normalize"),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="county_map", width="100%", height="700"
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
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
                                            style={"fontWeight": "bold"},
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
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Input(
                                            id="zcta-pop-min",
                                            type="number",
                                            value=0,
                                            min=0,
                                            step=1,
                                        ),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="zcta_map", width="100%", height="700"
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
                        )
                    ],
                ),
                dcc.Tab(
                    label="Tracts",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="tract-metric-selector",
                                            options=_make_options(tract_metric_cols),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                        _normalize_checkbox("tract-normalize"),
                                        html.Label(
                                            "Select State", style={"fontWeight": "bold"}
                                        ),
                                        dcc.Dropdown(
                                            id="state-selector",
                                            options=states,
                                            value="New York",
                                            multi=False,
                                            placeholder="Select State...",
                                        ),
                                        html.Label(
                                            "Exclude GEOIDs",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="tract-exclude",
                                            options=[],
                                            multi=True,
                                            placeholder="Select GEOIDs to exclude...",
                                        ),
                                        html.Label(
                                            "Minimum Population",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Input(
                                            id="tract-pop-min",
                                            type="number",
                                            value=0,
                                            min=0,
                                            step=1,
                                        ),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="tract_map", width="100%", height="700"
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
                        )
                    ],
                ),
                dcc.Tab(
                    label="Block Groups",
                    style=_tab_style,
                    selected_style=_tab_style,
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label(
                                            "Select Metrics",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="block-group-metric-selector",
                                            options=_make_options(
                                                block_group_metric_cols
                                            ),
                                            value=[DEFAULT_VAR],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                        _normalize_checkbox("block-group-normalize"),
                                        html.Label(
                                            "Select City", style={"fontWeight": "bold"}
                                        ),
                                        dcc.Dropdown(
                                            id="city-selector",
                                            options=cities,
                                            value="New York",
                                            multi=False,
                                            placeholder="Select City...",
                                        ),
                                        html.Label(
                                            "Exclude GEOIDs",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="block-group-exclude",
                                            options=[],
                                            multi=True,
                                            placeholder="Select GEOIDs to exclude...",
                                        ),
                                        html.Label(
                                            "Minimum Population",
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Input(
                                            id="block-group-pop-min",
                                            type="number",
                                            value=0,
                                            min=0,
                                            step=1,
                                        ),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Iframe(
                                            id="block_group_map",
                                            width="100%",
                                            height="700",
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
                        )
                    ],
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
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="trends-geo-level",
                                            options=list(TIMESERIES_GEOS.keys()),
                                            value="State",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Select Geography",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="trends-geo",
                                            options=sorted(ts_state["state"].unique()),
                                            value=_ts_state_defaults,
                                            multi=True,
                                            placeholder="Select geographies to compare...",
                                        ),
                                        html.Label(
                                            "Metric",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="trends-metric",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="Median Household Income",
                                            clearable=False,
                                        ),
                                        _inflate_checkbox("trends-inflate"),
                                        html.P(
                                            "ACS 5-Year Estimates (rolling average). "
                                            "Each point represents a 5-year window.",
                                            style={
                                                "fontSize": "11px",
                                                "color": "#888",
                                                "marginTop": "16px",
                                            },
                                        ),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="trends-chart", style={"height": "700px"}
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
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
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="anim-geo-level",
                                            options=list(TIMESERIES_GEOS.keys()),
                                            value="State",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "X Axis",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="anim-x",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="pct_poverty",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Y Axis",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="anim-y",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="Median Household Income",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Color by (optional)",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
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
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="anim-size",
                                            options=_make_options(TIMESERIES_METRICS),
                                            value="Pop",
                                            clearable=True,
                                            placeholder="None",
                                        ),
                                        _inflate_checkbox("anim-inflate"),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="anim-scatter-plot",
                                            style={"height": "700px"},
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
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
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-geo",
                                            options=list(SCATTER_GEOS.keys()),
                                            value="County",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "X Axis",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-x",
                                            options=_make_options(county_metric_cols),
                                            value="Pop",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Y Axis",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="scatter-y",
                                            options=_make_options(county_metric_cols),
                                            value="pct_male",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Color by (optional)",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
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
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
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
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="scatter-plot",
                                            style={"height": "700px"},
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
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
                                    "Metric Groups: ",
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
                                            style={"fontWeight": "bold"},
                                        ),
                                        dcc.Dropdown(
                                            id="corr-geo-level",
                                            options=list(CORR_GEOS.keys()),
                                            value="County",
                                            clearable=False,
                                        ),
                                        html.Label(
                                            "Metrics",
                                            style={
                                                "fontWeight": "bold",
                                                "marginTop": "12px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="corr-metrics",
                                            options=_make_options(county_metric_cols),
                                            value=CORR_METRIC_GROUPS["Key Metrics"],
                                            multi=True,
                                            placeholder="Select metrics...",
                                        ),
                                    ],
                                    style={
                                        "fontFamily": "Arial",
                                        "width": "300px",
                                        "padding": "20px",
                                        "flexShrink": 0,
                                    },
                                ),
                                html.Div(
                                    [
                                        dcc.Graph(
                                            id="corr-matrix", style={"height": "700px"}
                                        )
                                    ],
                                    style={"flexGrow": 1, "padding": "20px"},
                                ),
                            ],
                            style={"display": "flex", "alignItems": "flex-start"},
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
            s["properties"][i] = val
        folium.GeoJsonTooltip([label_key, i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles="cartodb positron", control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()


def generate_state_map(selected_metrics, normalize=False):
    """Render state choropleth map."""
    df = _normalize_df(c_state, selected_metrics) if normalize else c_state
    return _build_choropleth_map(
        state_geom_json, df, "state", "State", selected_metrics
    )


def generate_dma_map(selected_metrics, normalize=False):
    """Render DMA choropleth map."""
    df = _normalize_df(c_dma, selected_metrics) if normalize else c_dma
    return _build_choropleth_map(dma_geom_json, df, "dma", "DMA", selected_metrics)


def generate_county_map(selected_metrics, normalize=False):
    """Render county choropleth map."""
    df = (
        _normalize_df(c_county_state, selected_metrics) if normalize else c_county_state
    )
    return _build_choropleth_map(
        county_geom_json, df, "GEOID", "County", selected_metrics, name_col="NAME"
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


# Callbacks ########################################################################################


@app.callback(
    Output("state_map", "srcDoc"),
    Input("state-metric-selector", "value"),
    Input("state-normalize", "value"),
)
def update_state_map(metrics, normalize):
    """Callback: update state map."""
    return generate_state_map(metrics, bool(normalize))


@app.callback(
    Output("dma_map", "srcDoc"),
    Input("dma-metric-selector", "value"),
    Input("dma-normalize", "value"),
)
def update_dma_map(metrics, normalize):
    """Callback: update DMA map."""
    return generate_dma_map(metrics, bool(normalize))


@app.callback(
    Output("county_map", "srcDoc"),
    Input("county-metric-selector", "value"),
    Input("county-normalize", "value"),
)
def update_county_map(metrics, normalize):
    """Callback: update county map."""
    return generate_county_map(metrics, bool(normalize))


@app.callback(
    Output("zcta_map", "srcDoc"),
    Input("zcta-metric-selector", "value"),
    Input("dma-selector", "value"),
    Input("zcta-pop-min", "value"),
    Input("zcta-normalize", "value"),
)
def update_zcta_map(metrics, dma, pop_min, normalize):
    """Callback: update ZCTA map."""
    return generate_zcta_map(metrics, dma, pop_min, bool(normalize))


@app.callback(
    Output("tract_map", "srcDoc"),
    Input("tract-metric-selector", "value"),
    Input("state-selector", "value"),
    Input("tract-pop-min", "value"),
    Input("tract-exclude", "value"),
    Input("tract-normalize", "value"),
)
def update_tract_map(metrics, state, pop_min, exclude, normalize):
    """Callback: update tract map."""
    return generate_tract_map(metrics, state, pop_min, exclude, bool(normalize))


@app.callback(
    Output("block_group_map", "srcDoc"),
    Input("block-group-metric-selector", "value"),
    Input("city-selector", "value"),
    Input("block-group-pop-min", "value"),
    Input("block-group-exclude", "value"),
    Input("block-group-normalize", "value"),
)
def update_block_group_map(metrics, city, pop_min, exclude, normalize):
    """Callback: update block group map."""
    return generate_block_group_map(metrics, city, pop_min, exclude, bool(normalize))


@app.callback(Output("tract-exclude", "options"), Input("state-selector", "value"))
def update_tract_exclude_options(selected_state):
    """Callback: populate tract exclude dropdown."""
    state_fips = state_name.loc[
        state_name["state_NAME"] == selected_state, "state"
    ].values[0]
    return sorted(c_tract.loc[c_tract["GEOID"].str[:2] == state_fips, "GEOID"].unique())


@app.callback(Output("block-group-exclude", "options"), Input("city-selector", "value"))
def update_block_group_exclude_options(selected_city):
    """Callback: populate block group exclude dropdown."""
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
    """Callback: load scatter preset values."""
    triggered_id = callback_context.triggered[0]["prop_id"]
    idx = int(triggered_id.split("-")[2].split(".")[0])
    s = SUGGESTED_SCATTERS[idx]
    return s["geo"], s["x"], s["y"], s["color"], s.get("size")


@app.callback(
    Output("scatter-x", "options"),
    Output("scatter-y", "options"),
    Output("scatter-color", "options"),
    Output("scatter-size", "options"),
    Input("scatter-geo", "value"),
)
def update_scatter_options(geo):
    """Callback: update scatter dropdown options when geo changes."""
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
    """Callback: update scatter filter dropdown based on geo level."""
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
    """Callback: render scatter plot."""
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
    """Callback: load animated scatter preset values."""
    triggered_id = callback_context.triggered[0]["prop_id"]
    idx = int(triggered_id.split("-")[2].split(".")[0])
    s = SUGGESTED_ANIM_SCATTERS[idx]
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
    """Callback: render animated scatter plot."""
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
    """Callback: populate geography dropdown for trends tab."""
    df, name_col = TIMESERIES_GEOS[geo_level]
    return sorted(df[name_col].unique())


@app.callback(
    Output("trends-geo-level", "value"),
    Output("trends-geo", "value"),
    Output("trends-metric", "value"),
    Output("trends-inflate", "value"),
    [Input(f"trends-preset-{i}", "n_clicks") for i in range(len(SUGGESTED_TRENDS))],
    prevent_initial_call=True,
)
def load_trends_preset(*_):
    """Callback: load trends preset values."""
    triggered_id = callback_context.triggered[0]["prop_id"]
    idx = int(triggered_id.split("-")[2].split(".")[0])
    s = SUGGESTED_TRENDS[idx]
    return s["geo_level"], s["geo"], s["metric"], s["inflate"]


@app.callback(
    Output("trends-chart", "figure"),
    Input("trends-geo-level", "value"),
    Input("trends-geo", "value"),
    Input("trends-metric", "value"),
    Input("trends-inflate", "value"),
)
def update_trends_chart(geo_level, geo_names, metric, inflate):
    """Callback: render trends line chart."""
    if not geo_names or not metric:
        return px.line()
    df, name_col = TIMESERIES_GEOS[geo_level]
    plot_df = df[df[name_col].isin(geo_names)][["year", name_col, metric]].dropna(
        subset=[metric]
    )
    if inflate:
        plot_df = _apply_cpi(plot_df)
    fig = px.line(
        plot_df,
        x="year",
        y=metric,
        color=name_col,
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 20, "b": 40},
        xaxis={"dtick": 1, "title": "Year"},
        yaxis=_axis_fmt(metric),
        legend_title=geo_level,
    )
    return fig


@app.callback(
    Output("corr-metrics", "options"),
    Output("corr-metrics", "value"),
    Input("corr-geo-level", "value"),
    [Input(f"corr-group-{i}", "n_clicks") for i in range(len(CORR_METRIC_GROUPS))],
)
def update_corr_options(geo_level, *_group_clicks):
    """Callback: update correlation metric options and apply preset groups."""
    _, cols = CORR_GEOS[geo_level]
    opts = _make_options(cols)
    triggered = callback_context.triggered[0]["prop_id"]
    if "corr-group" in triggered:
        idx = int(triggered.split("-")[2].split(".")[0])
        group_key = list(CORR_METRIC_GROUPS.keys())[idx]
        value = [m for m in CORR_METRIC_GROUPS[group_key] if m in cols]
    else:
        value = [m for m in CORR_METRIC_GROUPS["Key Metrics"] if m in cols]
    return opts, value


@app.callback(
    Output("corr-matrix", "figure"),
    Input("corr-geo-level", "value"),
    Input("corr-metrics", "value"),
)
def update_corr_matrix(geo_level, selected_metrics):
    """Callback: render correlation heatmap."""
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

_N_TRENDS = len(SUGGESTED_TRENDS)
_N_SCATTER = len(SUGGESTED_SCATTERS)
_N_ANIM = len(SUGGESTED_ANIM_SCATTERS)


def _parse_preset_idx(triggered_id, prefix):
    part = triggered_id.replace(f"{prefix}-preset-", "").split(".")[0]
    return int(part)


@app.callback(
    Output("trends-active-preset", "data"),
    [Input(f"trends-preset-{i}", "n_clicks") for i in range(_N_TRENDS)],
    State("trends-active-preset", "data"),
    prevent_initial_call=True,
)
def _update_trends_active(*args):
    """Callback: track active trends preset index."""
    *_, current = args
    idx = _parse_preset_idx(callback_context.triggered[0]["prop_id"], "trends")
    return None if current == idx else idx


@app.callback(
    [Output(f"trends-preset-{i}", "style") for i in range(_N_TRENDS)],
    Input("trends-active-preset", "data"),
)
def _highlight_trends_presets(active):
    """Callback: highlight active trends preset button."""
    return [_btn_active_style if i == active else _btn_style for i in range(_N_TRENDS)]


@app.callback(
    Output("scatter-active-preset", "data"),
    [Input(f"scatter-preset-{i}", "n_clicks") for i in range(_N_SCATTER)],
    State("scatter-active-preset", "data"),
    prevent_initial_call=True,
)
def _update_scatter_active(*args):
    """Callback: track active scatter preset index."""
    *_, current = args
    idx = _parse_preset_idx(callback_context.triggered[0]["prop_id"], "scatter")
    return None if current == idx else idx


@app.callback(
    [Output(f"scatter-preset-{i}", "style") for i in range(_N_SCATTER)],
    Input("scatter-active-preset", "data"),
)
def _highlight_scatter_presets(active):
    """Callback: highlight active scatter preset button."""
    return [_btn_active_style if i == active else _btn_style for i in range(_N_SCATTER)]


@app.callback(
    Output("anim-active-preset", "data"),
    [Input(f"anim-preset-{i}", "n_clicks") for i in range(_N_ANIM)],
    State("anim-active-preset", "data"),
    prevent_initial_call=True,
)
def _update_anim_active(*args):
    """Callback: track active animated scatter preset index."""
    *_, current = args
    idx = _parse_preset_idx(callback_context.triggered[0]["prop_id"], "anim")
    return None if current == idx else idx


@app.callback(
    [Output(f"anim-preset-{i}", "style") for i in range(_N_ANIM)],
    Input("anim-active-preset", "data"),
)
def _highlight_anim_presets(active):
    """Callback: highlight active animated scatter preset button."""
    return [_btn_active_style if i == active else _btn_style for i in range(_N_ANIM)]


if __name__ == "__main__":
    app.run_server(debug=True)
