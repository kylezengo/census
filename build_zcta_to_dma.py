"""Build zcta to dma crosswalk."""

import pandas as pd

# Load files
zip_to_zcta_raw = pd.read_excel(
    "ZIPCodetoZCTACrosswalk2021UDS.xlsx",
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

 # Save csv
zcta_to_dma.to_csv("zcta_to_dma.csv", index=False)