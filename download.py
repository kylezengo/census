""" Download census data"""

import os
import requests

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ACS_URL = "https://api.census.gov/data/2022/acs/acs5" # note year 2022

# Authentication
load_dotenv()
census_api_key = os.getenv("census_api_key")

# Load files
zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={'zcta':object})


####################################################################################################
########################################## DOWNLOAD DATA ###########################################
####################################################################################################
# Get variable options
response = requests.get("https://api.census.gov/data/2022/acs/acs5/variables.json", timeout=20)
variables_json = response.json()

variables = pd.DataFrame.from_dict(variables_json["variables"], orient="index")
variables = variables.reset_index(names='variable')

# Get data for variables
var_misc = [
    "B11012_001E", # N Census Households
    "B19001_014E", # N Census Household Income 100-124
    "B19001_015E", # N Census Household Income 125-149
    "B19001_016E", # N Census Household Income 150-199
    "B19001_017E", # N Census Household Income 200+
    "B19049_003E"  # Median Census Household Income 25-44
]

groups = [
    "B01001",  # Sex by Age
    # "B01001A", # Sex by Age (White Alone)
    # 'B01001B', # Sex by Age (Black or African American Alone)
    # 'B01001C', # Sex by Age (American Indian and Alaska Native Alone)
    # 'B01001D', # Sex by Age (Asian Alone)
    # 'B01001E', # Sex by Age (Native Hawaiian and Other Pacific Islander Alone)
    # 'B01001F', # Sex by Age (Some Other Race Alone)
    # 'B01001G', # Sex by Age (Two or More Races)
    # 'B01001H', # Sex by Age (White Alone, Not Hispanic or Latino)
    # 'B01001I', # Sex by Age (Hispanic or Latino)
    "misc"
]







var_groups = {}
metrics = []
for g in groups:
    if g == "misc":
        var_groups[g] = ",".join(var_misc)

        metrics.extend(var_misc)
    else:
        vars_list = variables.loc[variables["group"] == g, "variable"].tolist()
        var_groups[g] = ",".join(vars_list) # convert to comma-separated string for API call

        metrics.extend(vars_list)

# Make API calls for each geography level
geo_level =["state", "county", "zip code tabulation area"]
dfs = {}
for level in geo_level:
    # Calls are seperated because I can only request 50 variables at once
    var_dfs = []
    for var_str in var_groups.values():
        params = {
            "get": f"NAME,{var_str}",
            "for": f"{level}:*",
            "key": census_api_key
        }
        response = requests.get(ACS_URL, params=params, timeout=20)

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
        else:
            data = response.json()
            data_df = pd.DataFrame(data[1:], columns=data[0])
            var_dfs.append(data_df)

    if level == "zip code tabulation area":
        merged_df = var_dfs[0].drop(columns='NAME')
        for df in var_dfs[1:]:
            df = df.drop(columns='NAME')
            merged_df = merged_df.merge(df, how='outer', on=level)
    else:
        merged_df = var_dfs[0]
        for df in var_dfs[1:]:
            df = df.drop(columns=['state','county'], errors='ignore')
            merged_df = merged_df.merge(df, how='outer', on="NAME")

    dfs[level] = merged_df

c_state = dfs['state']
c_state = c_state.drop(columns='state')
c_state = c_state.rename(columns={'NAME': 'state'})

c_county = dfs['county']

c_zcta = dfs['zip code tabulation area']

state_name = dfs['state'][['state','NAME']].rename(columns={'NAME': 'state_NAME'})
c_county_state = c_county.merge(state_name, how='left', on='state')
c_county_state['GEOID'] = c_county_state['state'] + c_county_state['county']

# tract ############################################################################################
states = state_name['state'].unique()
tract_dfs = []
for state_fips in states:
    var_dfs = []
    for var_str in var_groups.values():
        params = {
            "get": f"NAME,{var_str}",
            "for": "tract:*",
            "in": f"state:{state_fips}",
            "key": census_api_key
        }
        response = requests.get(ACS_URL, params=params, timeout=20)

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
        else:
            data = response.json()
            data_df = pd.DataFrame(data[1:], columns=data[0])
            var_dfs.append(data_df)

    merged_df = var_dfs[0]
    for df in var_dfs[1:]:
        merged_df = merged_df.merge(df, how='outer', on=['NAME', 'state', 'county', 'tract'])

    tract_dfs.append(merged_df)

# Combine all tract dataframes
c_tract = pd.concat(tract_dfs, ignore_index=True)

c_tract['GEOID'] = (
    c_tract['state'] +
    c_tract['county'] +
    c_tract['tract'].str.zfill(6) # pad tract to 6 digits if needed
)

# block group ######################################################################################
state_counties = {
    "36": ["005", "047", "061", "081", "085"],  # NY counties (NYC)
    "06": ["037", "075"],                       # CA counties (LA, SF)
}
block_group_dfs = []
for state_fips, counties in state_counties.items():
    for county in counties:
        var_dfs = []
        for var_str in var_groups.values():
            params = {
                "get": f"NAME,{var_str}",
                "for": "block group:*",
                "in": f"state:{state_fips} county:{county}",
                "key": census_api_key
            }
            response = requests.get(ACS_URL, params=params, timeout=20)

            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
            else:
                data = response.json()
                data_df = pd.DataFrame(data[1:], columns=data[0])
                var_dfs.append(data_df)

        merged_df = var_dfs[0]
        for df in var_dfs[1:]:
            merged_df = merged_df.merge(
                df,
                how='outer',
                on=['NAME', 'state', 'county', 'tract', 'block group']
            )

        block_group_dfs.append(merged_df)

# Combine all block group dataframes
c_block_group = pd.concat(block_group_dfs, ignore_index=True)

c_block_group['GEOID'] = (
    c_block_group['state'] +
    c_block_group['county'] +
    c_block_group['tract'].str.zfill(6) +  # pad tract to 6 digits if needed
    c_block_group['block group']
)

####################################################################################################
############################################# CLEAN UP #############################################
####################################################################################################
# Join ZCTA to DMA and create c_dma
c_zcta = c_zcta.rename(columns={'zip code tabulation area': 'zcta'})

c_zcta_dma = c_zcta.merge(zcta_to_dma, how='left', on='zcta')
c_zcta_dma[metrics] = c_zcta_dma[metrics].astype(float)

c_dma = c_zcta_dma.groupby('dma', as_index=False, dropna=False).sum(numeric_only=True)

# Rename columns for clarity
cols_to_rename = {
    # B01001: Sex by Age
    'B01001_001E': 'Pop - Total'
    ,'B01001_002E': 'Pop - Male'
    ,'B01001_003E': 'Pop - Male Under 5 years'
    ,'B01001_004E': 'Pop - Male 5 to 9 years'
    ,'B01001_005E': 'Pop - Male 10 to 14 years'
    ,'B01001_006E': 'Pop - Male 15 to 17 years'
    ,'B01001_007E': 'Pop - Male 18 and 19 years'
    ,'B01001_008E': 'Pop - Male 20 years'
    ,'B01001_009E': 'Pop - Male 21 years'
    ,'B01001_010E': 'Pop - Male 22 to 24 years'
    ,'B01001_011E': 'Pop - Male 25 to 29 years'
    ,'B01001_012E': 'Pop - Male 30 to 34 years'
    ,'B01001_013E': 'Pop - Male 35 to 39 years'
    ,'B01001_014E': 'Pop - Male 40 to 44 years'
    ,'B01001_015E': 'Pop - Male 45 to 49 years'
    ,'B01001_016E': 'Pop - Male 50 to 54 years'
    ,'B01001_017E': 'Pop - Male 55 to 59 years'
    ,'B01001_018E': 'Pop - Male 60 and 61 years'
    ,'B01001_019E': 'Pop - Male 62 to 64 years'
    ,'B01001_020E': 'Pop - Male 65 and 66 years'
    ,'B01001_021E': 'Pop - Male 67 to 69 years'
    ,'B01001_022E': 'Pop - Male 70 to 74 years'
    ,'B01001_023E': 'Pop - Male 75 to 79 years'
    ,'B01001_024E': 'Pop - Male 80 to 84 years'
    ,'B01001_025E': 'Pop - Male 85 years and over'
    ,'B01001_026E': 'Pop - Female'
    ,'B01001_027E': 'Pop - Female Under 5 years'
    ,'B01001_028E': 'Pop - Female 5 to 9 years'
    ,'B01001_029E': 'Pop - Female 10 to 14 years'
    ,'B01001_030E': 'Pop - Female 15 to 17 years'
    ,'B01001_031E': 'Pop - Female 18 and 19 years'
    ,'B01001_032E': 'Pop - Female 20 years'
    ,'B01001_033E': 'Pop - Female 21 years'
    ,'B01001_034E': 'Pop - Female 22 to 24 years'
    ,'B01001_035E': 'Pop - Female 25 to 29 years'
    ,'B01001_036E': 'Pop - Female 30 to 34 years'
    ,'B01001_037E': 'Pop - Female 35 to 39 years'
    ,'B01001_038E': 'Pop - Female 40 to 44 years'
    ,'B01001_039E': 'Pop - Female 45 to 49 years'
    ,'B01001_040E': 'Pop - Female 50 to 54 years'
    ,'B01001_041E': 'Pop - Female 55 to 59 years'
    ,'B01001_042E': 'Pop - Female 60 and 61 years'
    ,'B01001_043E': 'Pop - Female 62 to 64 years'
    ,'B01001_044E': 'Pop - Female 65 and 66 years'
    ,'B01001_045E': 'Pop - Female 67 to 69 years'
    ,'B01001_046E': 'Pop - Female 70 to 74 years'
    ,'B01001_047E': 'Pop - Female 75 to 79 years'
    ,'B01001_048E': 'Pop - Female 80 to 84 years'
    ,'B01001_049E': 'Pop - Female 85 years and over'

    # B01001A: Sex by Age (White Alone)
    ,'B01001A_001E': 'Pop - Total (White Alone)'
    ,'B01001A_002E': 'Pop - Male (White Alone)'
    ,'B01001A_003E': 'Pop - Male Under 5 years (White Alone)'
    ,'B01001A_004E': 'Pop - Male 5 to 9 years (White Alone)'
    ,'B01001A_005E': 'Pop - Male 10 to 14 years (White Alone)'
    ,'B01001A_006E': 'Pop - Male 15 to 17 years (White Alone)'
    ,'B01001A_007E': 'Pop - Male 18 and 19 years (White Alone)'
    ,'B01001A_008E': 'Pop - Male 20 to 24 years (White Alone)'
    ,'B01001A_009E': 'Pop - Male 25 to 29 years (White Alone)'
    ,'B01001A_010E': 'Pop - Male 30 to 34 years (White Alone)'
    ,'B01001A_011E': 'Pop - Male 35 to 44 years (White Alone)'
    ,'B01001A_013E': 'Pop - Male 55 to 64 years (White Alone)'
    ,'B01001A_012E': 'Pop - Male 45 to 54 years (White Alone)'
    ,'B01001A_014E': 'Pop - Male 65 to 74 years (White Alone)'
    ,'B01001A_015E': 'Pop - Male 75 to 84 years (White Alone)'
    ,'B01001A_016E': 'Pop - Male 85 years and over (White Alone)'
    ,'B01001A_017E': 'Pop - Female (White Alone)'
    ,'B01001A_018E': 'Pop - Female Under 5 years (White Alone)'
    ,'B01001A_019E': 'Pop - Female 5 to 9 years (White Alone)'
    ,'B01001A_020E': 'Pop - Female 10 to 14 years (White Alone)'
    ,'B01001A_021E': 'Pop - Female 15 to 17 years (White Alone)'
    ,'B01001A_022E': 'Pop - Female 18 and 19 years (White Alone)'
    ,'B01001A_024E': 'Pop - Female 25 to 29 years (White Alone)'
    ,'B01001A_023E': 'Pop - Female 20 to 24 years (White Alone)'
    ,'B01001A_025E': 'Pop - Female 30 to 34 years (White Alone)'
    ,'B01001A_026E': 'Pop - Female 35 to 44 years (White Alone)'
    ,'B01001A_027E': 'Pop - Female 45 to 54 years (White Alone)'
    ,'B01001A_028E': 'Pop - Female 55 to 64 years (White Alone)'
    ,'B01001A_029E': 'Pop - Female 65 to 74 years (White Alone)'
    ,'B01001A_030E': 'Pop - Female 75 to 84 years (White Alone)'
    ,'B01001A_031E': 'Pop - Female 85 years and over (White Alone)'

    # Miscellaneous variables
    ,'B11012_001E': 'N Census Households'
    ,'B19001_014E': 'N Census Household Income 100-124'
    ,'B19001_015E': 'N Census Household Income 125-149'
    ,'B19001_016E': 'N Census Household Income 150-199'
    ,'B19001_017E': 'N Census Household Income 200+'
    ,'B19049_003E': 'Median Census Household Income 25-44'
}

decade_aggregations = [
    ('Under 10 years', ['Under 5 years', '5 to 9 years']),
    ('10 to 19 years', ['10 to 14 years', '15 to 17 years', '18 and 19 years']),
    ('20 to 29 years', ['20 years', '21 years', '22 to 24 years', '25 to 29 years']),
    ('30 to 39 years', ['30 to 34 years', '35 to 39 years']),
    ('40 to 49 years', ['40 to 44 years', '45 to 49 years']),
    ('50 to 59 years', ['50 to 54 years', '55 to 59 years']),
    ('60 to 69 years', ['60 and 61 years', '62 to 64 years', '65 and 66 years', '67 to 69 years']),
    ('70 to 79 years', ['70 to 74 years', '75 to 79 years']),
    ('80 years and over', ['80 to 84 years', '85 years and over']),
]

decade_labels = [
    'Under 10 years',
    '10 to 19 years',
    '20 to 29 years',
    '30 to 39 years',
    '40 to 49 years',
    '50 to 59 years',
    '60 to 69 years',
    '70 to 79 years',
    '80 years and over'
]

pop_dfs = [c_state, c_dma, c_county_state, c_zcta_dma, c_tract, c_block_group]
for df in pop_dfs:
    df[metrics] = df[metrics].astype(float)
    df.rename(columns=cols_to_rename, inplace=True)

    df['pct_male'] = df['Pop - Male'] / df['Pop - Total']
    df['Household Income 200+_ratio'] = (
        df['N Census Household Income 200+'] / df['N Census Households']
    )
    df['pct_white'] = df['Pop - Total (White Alone)'] / df['Pop - Total']
    for gender in ['Male', 'Female']:
        for decade, cols in decade_aggregations:
            colnames = [f'Pop - {gender} {c}' for c in cols]
            new_col = f'Pop - {gender} {decade}'
            df[new_col] = df[colnames].sum(axis=1)
    for decade in decade_labels:
        male_col = f'Pop - Male {decade}'
        female_col = f'Pop - Female {decade}'
        ratio_col = f'pct_male_{decade}'
        df[ratio_col] = df[male_col] / (df[male_col] + df[female_col])

    df.replace([np.inf, -np.inf, -666666666], np.nan, inplace=True)

# Drop the 'Median Census Household Income 25-44' column from c_dma
# This column has been summed over ZCTAs so it is now invalid
# Could try to do weighted avg or something instead...
c_dma = c_dma.drop(columns='Median Census Household Income 25-44')

####################################################################################################
############################################### SAVE ###############################################
####################################################################################################
# Save csv (mostly static data so just overwrite)
state_name.to_csv("state_name.csv", index=False)

c_state.to_csv("c_state.csv", index=False)
c_dma.to_csv("c_dma.csv", index=False)
c_county_state.to_csv("c_county_state.csv", index=False)
c_zcta_dma.to_csv("c_zcta_dma.csv", index=False)
c_tract.to_csv("c_tract.csv", index=False)
c_block_group.to_csv("c_block_group.csv", index=False)
