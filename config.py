"""Static configuration for the Census Data Explorer.

Pure data only — no dataframe references, so this imports cleanly on its own.
Anything that binds a loaded dataframe (TIMESERIES_GEOS, CORR_GEOS, ...) stays
in app.py.
"""

# Display constants ###########################################################
ACS_YEAR = 2024
DEFAULT_VAR = "Pop"

# The national row lives inside the state-level frames (see download_timeseries).
US_LABEL = "United States"

MALE_COLOR = "Blues"
FEMALE_COLOR = "Reds"
MF_COLOR = "RdBu"
INCOME_COLOR = "Greens"


# Trends tab #################################################################
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


# Metrics the ACS iterates by race. Home value, gross rent and pct_male have no
# race-iterated table, so they're unavailable when segmenting by race.
TIMESERIES_RACE_METRICS = [
    "Pop",
    "Median Household Income",
    "pct_poverty",
    "pct_unemployed",
    "pct_bachelors_plus",
    "pct_owner_occupied",
    "pct_renter_occupied",
]


# Householder age brackets (B19049). Ordered youngest to oldest so the legend
# reads naturally; the ACS offers no other metric broken out this way.
AGE_BRACKETS = ["Under 25", "25 to 44", "45 to 64", "65 and over"]

TIMESERIES_AGE_METRICS = ["Median Household Income"]


RACE_GROUPS = [
    "White (non-Hispanic)",
    "Black",
    "Hispanic or Latino",
    "Asian",
    "American Indian / Alaska Native",
    "Native Hawaiian / Pacific Islander",
    "Two or more races",
    "Some other race",
    "White alone",
]


# Smaller groups are heavily suppressed at county level, so default to the four
# with reliable coverage; the rest stay selectable.
RACE_DEFAULTS = ["White (non-Hispanic)", "Black", "Hispanic or Latino", "Asian"]


TRENDS_VIEWS = {
    "level": "Level",
    "indexed": "Indexed to first year (=100)",
    "ratio": "Ratio to overall",
}


# Ratio-mode baseline: the geography's own all-races value isolates racial
# disparity from regional cost-of-living; the US value is a common yardstick
# when comparing several geographies.
RATIO_BASELINES = {"self": "Same geography", "us": US_LABEL}


# Length of px.colors.qualitative.Set2 — past this, line colours repeat.
_MAX_TREND_SERIES = 8


# Presets ####################################################################
SUGGESTED_TRENDS = [
    {
        # Same data as ratios: Black income sits ~0.68 of the national median
        # in both 2009 and 2024 — 15 years of growth, no relative movement.
        "label": "Income Gap",
        "geo_level": "State",
        "geo": [US_LABEL],
        "metric": "Median Household Income",
        "inflate": [],
        "segment": "race",
        "race": RACE_DEFAULTS + ["American Indian / Alaska Native"],
        "view": "ratio",
    },
    {
        # The clearest convergence in the data: Hispanic 0.46 -> 0.58 and
        # Black 0.63 -> 0.71 of the national bachelor's rate. Needs the
        # 2009-2011 backfill to show the full run.
        "label": "Education Gap",
        "geo_level": "State",
        "geo": [US_LABEL],
        "metric": "pct_bachelors_plus",
        "inflate": [],
        "segment": "race",
        "race": RACE_DEFAULTS,
        "view": "ratio",
    },
    {
        # Homeownership is the gap that did NOT close: Black ownership slips
        # from 0.687 to 0.676 of the national rate while every other group rises.
        "label": "Ownership Gap",
        "geo_level": "State",
        "geo": [US_LABEL],
        "metric": "pct_owner_occupied",
        "inflate": [],
        "segment": "race",
        "race": RACE_DEFAULTS,
        "view": "ratio",
    },
    {
        # Under-25 households earned $26.4k in 2009 vs $63.1k for 45-64 —
        # and the young-household line barely moved until ~2016.
        "label": "Income by Age",
        "geo_level": "State",
        "geo": [US_LABEL],
        "metric": "Median Household Income",
        "inflate": ["inflate"],
        "segment": "age",
        "race": AGE_BRACKETS,
    },
    {
        # Same race, different states — shows geography matters as much as
        # race for outcomes. Utah/Idaho fell ~10pts while Vermont/Hawaii rose.
        "label": "Black Ownership by State",
        "geo_level": "State",
        "geo": ["Utah", "Idaho", "Vermont", "Delaware", US_LABEL],
        "metric": "pct_owner_occupied",
        "inflate": [],
        "segment": "race",
        "race": ["Black"],
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
        "label": "Market Quality Over Time",
        "geo_level": "State",
        "x": "pct_bachelors_plus",
        "y": "Median Household Income",
        "color": "pct_poverty",
        "size": "Pop",
    },
]


CORR_METRIC_GROUPS = {
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
    "Advertiser": [
        "Median Household Income",
        "Household Income 200+_ratio",
        "pct_bachelors_plus",
        "pct_owner_occupied",
        "pct_renter_occupied",
        "pct_hispanic",
        "pct_asian",
        "pct_black",
        "pct_poverty",
        "pct_male_20 to 29 years",
        "pct_male_30 to 39 years",
    ],
}


# Suggested scatter presets
SUGGESTED_SCATTERS = [
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
        "label": "Gentrification Risk",
        "geo": "ZCTA",
        "x": "pct_poverty",
        "y": "pct_bachelors_plus",
        "color": "Median Gross Rent",
        "size": "Pop",
    },
    {
        "label": "Affluent DMAs",
        "geo": "DMA",
        "x": "pct_bachelors_plus",
        "y": "Household Income 200+_ratio",
        "color": "pct_owner_occupied",
        "size": "Pop",
    },
]


# CPI-U annual averages (BLS, all items) — used to express dollar metrics in 2022 dollars
# Inflation & labels #########################################################
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


_PCT_METRICS = {"Household Income 200+_ratio"}
