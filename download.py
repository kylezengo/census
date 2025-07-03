""" Download census data"""

import os
import requests

import numpy as np
import pandas as pd
import pygris
from dotenv import load_dotenv

ACS_URL = "https://api.census.gov/data/2022/acs/acs5"

# Authentication
load_dotenv()
census_api_key = os.getenv("census_api_key")

# Load files
zip_to_zcta_raw = pd.read_excel(
    "ZIPCodetoZCTACrosswalk2021UDS.xlsx", # https://udsmapper.org/zip-code-to-zcta-crosswalk/
    dtype = {'ZIP_CODE':object,'ZCTA':object}
)
zip_to_dma_raw = pd.read_csv("zip_to_dma.csv", dtype = {'zip_code':object, 'dma_code':object})

# Create zcta_to_dma
zip_to_zcta = zip_to_zcta_raw.copy()
zip_to_zcta.columns = zip_to_zcta.columns.str.lower()

zip_to_dma = zip_to_dma_raw.rename(columns={'dma_description_clean':'dma'})
zip_to_dma = zip_to_dma.drop(columns=['dma_description'])

zip_to_zcta = zip_to_zcta.loc[~zip_to_zcta['state'].isin(["PR","VI"])]

zip_to_zcta_dma = zip_to_zcta.merge(zip_to_dma, how='left', on='zip_code') # might want full outer?
zip_to_zcta_dma = zip_to_zcta_dma.merge(
    zip_to_dma.rename(columns={
        'zip_code':'zip_code_zcta_join'
        ,'dma_code':'dma_code_zcta_join'
        ,'dma':'dma_zcta_join'
    }),
    how='left',
    left_on='zcta',
    right_on='zip_code_zcta_join'
)
zip_to_zcta_dma['dma_code'] = zip_to_zcta_dma['dma_code'].fillna(zip_to_zcta_dma['dma_code_zcta_join'])
zip_to_zcta_dma['dma'] = zip_to_zcta_dma['dma'].fillna(zip_to_zcta_dma['dma_zcta_join'])
zip_to_zcta_dma = zip_to_zcta_dma.drop(columns=['zip_code_zcta_join','dma_code_zcta_join','dma_zcta_join'])

zcta_to_dma = zip_to_zcta_dma[['zcta','dma']].drop_duplicates()



####################################################################################################
########################################## DOWNLOAD DATA ###########################################
####################################################################################################
# Get the cartographic boundary files with cb = True, seems to only work for 2020 right now
# Commenting out since I already have these files downloaded
# state_geom = pygris.states(year=2020, cb = True)
# state_geom.to_file("state_geom.shp", index=False)

# counties_geom = pygris.counties(year=2020, cb = True)
# counties_geom.to_file("counties_geom.shp", index=False)

# zcta_geom = pygris.zctas(year=2020, cb = True)
# zcta_geom.to_file("zcta_geom.shp", index=False)

# block_groups_geom = pygris.block_groups(year=2020, cb = True)
# block_groups_geom.to_file("block_groups_geom.shp", index=False)

# Get variable options
response = requests.get("https://api.census.gov/data/2022/acs/acs5/variables.json", timeout=20)
variables_json = response.json()

variables = pd.DataFrame.from_dict(variables_json["variables"], orient="index")
variables = variables.reset_index(names='variable')

# Get data for variables
variables_to_get = variables.loc[variables["group"] == "B01001",'variable']
var_str = ",".join(variables_to_get) # convert to comma-separated string for API call

more_variables_to_get = [
    "B11012_001E", # N Census Households
    "B19001_014E", # N Census Household Income 100-124
    "B19001_015E", # N Census Household Income 125-149
    "B19001_016E", # N Census Household Income 150-199
    "B19001_017E", # N Census Household Income 200+
    "B19049_003E"  # Median Census Household Income 25-44
]
more_var_str = ",".join(more_variables_to_get)

# Get data for state, county, and zcta
to_get =["state", "county", "zip code tabulation area"]
dfs = {}
for i in to_get:
    params = {
        "get": f"NAME,{var_str}",
        "for": f"{i}:*",
        "key": census_api_key
    }
    response = requests.get(ACS_URL, params=params, timeout=20)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
    else:
        data = response.json()
        data_df = pd.DataFrame(data[1:], columns=data[0])

    # Calls are seperated because I can only request 50 variables at once
    params = {
        "get": f"NAME,{more_var_str}",
        "for": f"{i}:*",
        "key": census_api_key
    }
    response = requests.get(ACS_URL, params=params, timeout=20)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
    else:
        more_data = response.json()
        more_data_df = pd.DataFrame(more_data[1:], columns=more_data[0])
    
    if i == "state":
        # data_df = data_df.drop(columns='state')
        more_data_df = more_data_df.drop(columns='state')

        df = data_df.merge(more_data_df, how='outer', on='NAME')
        # df = df.rename(columns={'NAME': 'state'})
    elif i == "county":
        # data_df = data_df.drop(columns='county')
        more_data_df = more_data_df.drop(columns=['state','county'])

        df = data_df.merge(more_data_df, how='outer', on='NAME')
        # df = df.rename(columns={'NAME': 'county'})
    else:
        data_df = data_df.drop(columns='NAME')
        more_data_df = more_data_df.drop(columns='NAME')

        df = data_df.merge(more_data_df, how='outer', on=i)
    
    dfs[i] = df

c_state = dfs['state']
c_state = c_state.drop(columns='state')
c_state = c_state.rename(columns={'NAME': 'state'})

c_county = dfs['county']

c_zcta = dfs['zip code tabulation area']

state_name = dfs['state'][['state','NAME']].rename(columns={'NAME': 'state_NAME'})
c_county_state = c_county.merge(state_name, how='left', on='state')
c_county_state['GEOID'] = c_county_state['state'] + c_county_state['county']


####################################################################################################
############################################# CLEAN UP #############################################
####################################################################################################
# Convert numbers from string to integer
metrics = list(variables_to_get) + more_variables_to_get

c_state[metrics] = c_state[metrics].astype(float)
c_county_state[metrics] = c_county_state[metrics].astype(float)
c_zcta[metrics] = c_zcta[metrics].astype(float)


# Rename columns for clarity
cols_to_rename = {
    'zip code tabulation area': 'zcta'
    ,'B01001_001E': 'Pop - Total'
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
    ,'B11012_001E': 'N Census Households'
    ,'B19001_014E': 'N Census Household Income 100-124'
    ,'B19001_015E': 'N Census Household Income 125-149'
    ,'B19001_016E': 'N Census Household Income 150-199'
    ,'B19001_017E': 'N Census Household Income 200+'
    ,'B19049_003E': 'Median Census Household Income 25-44'
}

c_state = c_state.rename(columns=cols_to_rename)
c_county_state = c_county_state.rename(columns=cols_to_rename)
c_zcta = c_zcta.rename(columns=cols_to_rename)


# Aggregate decades
c_zcta['Pop - Male Under 10 years'] = c_zcta['Pop - Male Under 5 years'] + c_zcta['Pop - Male 5 to 9 years']
c_zcta['Pop - Male 10 to 19 years'] = c_zcta['Pop - Male 10 to 14 years'] + c_zcta['Pop - Male 15 to 17 years'] +  c_zcta['Pop - Male 18 and 19 years']
c_zcta['Pop - Male 20 to 29 years'] = c_zcta['Pop - Male 20 years'] + c_zcta['Pop - Male 21 years'] + c_zcta['Pop - Male 22 to 24 years'] + c_zcta['Pop - Male 25 to 29 years']
c_zcta['Pop - Male 30 to 39 years'] = c_zcta['Pop - Male 30 to 34 years'] + c_zcta['Pop - Male 35 to 39 years']
c_zcta['Pop - Male 40 to 49 years'] = c_zcta['Pop - Male 40 to 44 years'] + c_zcta['Pop - Male 45 to 49 years']
c_zcta['Pop - Male 50 to 59 years'] = c_zcta['Pop - Male 50 to 54 years'] + c_zcta['Pop - Male 55 to 59 years']
c_zcta['Pop - Male 60 to 69 years'] = c_zcta['Pop - Male 60 and 61 years'] + c_zcta['Pop - Male 62 to 64 years'] + c_zcta['Pop - Male 65 and 66 years'] + c_zcta['Pop - Male 67 to 69 years']
c_zcta['Pop - Male 70 to 79 years'] = c_zcta['Pop - Male 70 to 74 years'] + c_zcta['Pop - Male 75 to 79 years']
c_zcta['Pop - Male 80 years and over'] = c_zcta['Pop - Male 80 to 84 years'] + c_zcta['Pop - Male 85 years and over']

c_zcta['Pop - Female Under 10 years'] = c_zcta['Pop - Female Under 5 years'] + c_zcta['Pop - Female 5 to 9 years']
c_zcta['Pop - Female 10 to 19 years'] = c_zcta['Pop - Female 10 to 14 years'] + c_zcta['Pop - Female 15 to 17 years'] + c_zcta['Pop - Female 18 and 19 years']
c_zcta['Pop - Female 20 to 29 years'] = c_zcta['Pop - Female 20 years'] + c_zcta['Pop - Female 21 years'] + c_zcta['Pop - Female 22 to 24 years'] + c_zcta['Pop - Female 25 to 29 years']
c_zcta['Pop - Female 30 to 39 years'] = c_zcta['Pop - Female 30 to 34 years'] + c_zcta['Pop - Female 35 to 39 years']
c_zcta['Pop - Female 40 to 49 years'] = c_zcta['Pop - Female 40 to 44 years'] + c_zcta['Pop - Female 45 to 49 years']
c_zcta['Pop - Female 50 to 59 years'] = c_zcta['Pop - Female 50 to 54 years'] + c_zcta['Pop - Female 55 to 59 years']
c_zcta['Pop - Female 60 to 69 years'] = c_zcta['Pop - Female 60 and 61 years'] + c_zcta['Pop - Female 62 to 64 years'] + c_zcta['Pop - Female 65 and 66 years'] + c_zcta['Pop - Female 67 to 69 years']
c_zcta['Pop - Female 70 to 79 years'] = c_zcta['Pop - Female 70 to 74 years'] + c_zcta['Pop - Female 75 to 79 years']
c_zcta['Pop - Female 80 years and over'] = c_zcta['Pop - Female 80 to 84 years'] + c_zcta['Pop - Female 85 years and over']

# Sex ratios
c_zcta['mf_ratio'] = c_zcta['Pop - Male'] / c_zcta['Pop - Female']
c_zcta['mf_ratio_Under 10 years'] = c_zcta['Pop - Male Under 10 years'] / c_zcta['Pop - Female Under 10 years']
c_zcta['mf_ratio_10 to 19 years'] = c_zcta['Pop - Male 10 to 19 years'] / c_zcta['Pop - Female 10 to 19 years']
c_zcta['mf_ratio_20 to 29 years'] = c_zcta['Pop - Male 20 to 29 years'] / c_zcta['Pop - Female 20 to 29 years']
c_zcta['mf_ratio_30 to 39 years'] = c_zcta['Pop - Male 30 to 39 years'] / c_zcta['Pop - Female 30 to 39 years']
c_zcta['mf_ratio_40 to 49 years'] = c_zcta['Pop - Male 40 to 49 years'] / c_zcta['Pop - Female 40 to 49 years']
c_zcta['mf_ratio_50 to 59 years'] = c_zcta['Pop - Male 50 to 59 years'] / c_zcta['Pop - Female 50 to 59 years']
c_zcta['mf_ratio_60 to 69 years'] = c_zcta['Pop - Male 60 to 69 years'] / c_zcta['Pop - Female 60 to 69 years']
c_zcta['mf_ratio_70 to 79 years'] = c_zcta['Pop - Male 70 to 79 years'] / c_zcta['Pop - Female 70 to 79 years']
c_zcta['mf_ratio_80 years and over'] = c_zcta['Pop - Male 80 years and over'] / c_zcta['Pop - Female 80 years and over']

c_state['mf_ratio'] = c_state['Pop - Male'] / c_state['Pop - Female']

c_county_state['mf_ratio'] = c_county_state['Pop - Male'] / c_county_state['Pop - Female']

# Income ratios
c_state['Household Income 200+_ratio'] = c_state['N Census Household Income 200+'] / c_state['N Census Households']

c_county_state['Household Income 200+_ratio'] = c_county_state['N Census Household Income 200+'] / c_county_state['N Census Households']

c_zcta['Household Income 200+_ratio'] = c_zcta['N Census Household Income 200+'] / c_zcta['N Census Households']

#
c_state = c_state.replace([np.inf, -np.inf], np.nan)
c_state = c_state.replace(-666666666, np.nan)

c_county_state = c_county_state.replace([np.inf, -np.inf], np.nan)
c_county_state = c_county_state.replace(-666666666, np.nan)

c_zcta = c_zcta.replace([np.inf, -np.inf], np.nan)
c_zcta = c_zcta.replace(-666666666, np.nan)


######################################################
# DMA Stuff ##########################################
c_zcta_dma = c_zcta.merge(zcta_to_dma, how='left', on='zcta')

c_dma = c_zcta_dma.groupby('dma', as_index=False, dropna=False).sum(numeric_only=True)
c_dma = c_dma.drop(columns='Median Census Household Income 25-44') # can do weighted avg or something instead

pop_cols = [col for col in c_dma.columns if "Pop - " in col]
c_dma[pop_cols] = c_dma[pop_cols].astype(float) # need float for map

# Sex ratios
c_dma['mf_ratio'] = c_dma['Pop - Male'] / c_dma['Pop - Female']
c_dma['mf_ratio_Under 10 years'] = c_dma['Pop - Male Under 10 years'] / c_dma['Pop - Female Under 10 years']
c_dma['mf_ratio_10 to 19 years'] = c_dma['Pop - Male 10 to 19 years'] / c_dma['Pop - Female 10 to 19 years']
c_dma['mf_ratio_20 to 29 years'] = c_dma['Pop - Male 20 to 29 years'] / c_dma['Pop - Female 20 to 29 years']
c_dma['mf_ratio_30 to 39 years'] = c_dma['Pop - Male 30 to 39 years'] / c_dma['Pop - Female 30 to 39 years']
c_dma['mf_ratio_40 to 49 years'] = c_dma['Pop - Male 40 to 49 years'] / c_dma['Pop - Female 40 to 49 years']
c_dma['mf_ratio_50 to 59 years'] = c_dma['Pop - Male 50 to 59 years'] / c_dma['Pop - Female 50 to 59 years']
c_dma['mf_ratio_60 to 69 years'] = c_dma['Pop - Male 60 to 69 years'] / c_dma['Pop - Female 60 to 69 years']
c_dma['mf_ratio_70 to 79 years'] = c_dma['Pop - Male 70 to 79 years'] / c_dma['Pop - Female 70 to 79 years']
c_dma['mf_ratio_80 years and over'] = c_dma['Pop - Male 80 years and over'] / c_dma['Pop - Female 80 years and over']

# Income ratios
c_dma['Household Income 200+_ratio'] = c_dma['N Census Household Income 200+'] / c_dma['N Census Households']


####################################################################################################
############################################### SAVE ###############################################
####################################################################################################
 # Save csv (mostly static data so just overwrite)
zcta_to_dma.to_csv("zcta_to_dma.csv", index=False)

c_state.to_csv("c_state.csv", index=False)
c_dma.to_csv("c_dma.csv", index=False)
c_county_state.to_csv("c_county_state.csv", index=False)
c_zcta_dma.to_csv("c_zcta_dma.csv", index=False)