"""Create an interactive plot in a browser window"""

import folium
import geopandas as gpd
import pandas as pd
from dash import Dash, html, dcc, Input, Output

MALE_COLOR = "Blues"
FEMALE_COLOR = "Reds"
MF_COLOR = "RdBu"
INCOME_COLOR = "Greens"

# Load files #######################################################################################
state_geom_raw = gpd.read_file("state_geom.shp") # needs zcta_df.cpg, zcta_df.shx, etc
county_geom_raw = gpd.read_file("county_geom.shp")
zcta_geom_raw = gpd.read_file("zcta_geom.shp")
tract_geom_raw = gpd.read_file("tract_geom.shp")
block_group_geom_raw = gpd.read_file("block_group_geom.shp")

zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={'zcta':object})

dma_polygons_raw = gpd.read_file('dma_polygons.geojson')
dma_polygons_raw['cartodb_id'] = dma_polygons_raw['cartodb_id'].astype(str)
dma_polygons_raw['dma_code'] = dma_polygons_raw['dma_code'].astype(str)

dma_polygon_map = pd.read_csv('dma_polygon_map.csv')

c_state = pd.read_csv("c_state.csv")
c_dma = pd.read_csv("c_dma.csv")
c_county_state = pd.read_csv("c_county_state.csv", dtype={'GEOID':object})
c_zcta_dma = pd.read_csv("c_zcta_dma.csv", dtype={'zcta':object})
c_tract = pd.read_csv("c_tract.csv", dtype={'GEOID':object})
c_block_group = pd.read_csv("c_block_group.csv", dtype={'GEOID':object})

state_name = pd.read_csv("state_name.csv", dtype={'state':object})

# Set up the geographic geometry files #############################################################
state_geom = state_geom_raw[['NAME', 'geometry']]
state_geom = state_geom.set_index('NAME')
state_geom_json = state_geom.to_json()

dma_geom = dma_polygons_raw.merge(dma_polygon_map, left_on='dma_name', right_on='DMA Polygons')
dma_geom = dma_geom[['DMA', 'geometry']]
dma_geom = dma_geom.set_index('DMA')
dma_geom_json = dma_geom.to_json()

county_geom = county_geom_raw[['GEOID', 'geometry']]
county_geom = county_geom.set_index('GEOID')
county_geom_json = county_geom.to_json()

zcta_geom = zcta_geom_raw.merge(
    zcta_to_dma[['zcta','dma']],
    how="left",
    left_on='ZCTA5CE20',
    right_on='zcta'
)

tract_geom = tract_geom_raw[['GEOID', 'geometry']]

block_group_geom = block_group_geom_raw[['GEOID', 'geometry']]

# Create indexed versions of the dataframes to lookup values
c_state_indexed = c_state.set_index('state')
c_dma_indexed = c_dma.set_index('dma')
c_county_state_indexed = c_county_state.set_index('GEOID')

# Lists for user
dma_metric_cols = [col for col in c_dma.columns if col != 'dma']
dma_metric_cols.sort()

county_metric_cols = [col for col in c_county_state.columns if col not in  ['state', 'county', 'state_NAME', 'GEOID', 'NAME']]
county_metric_cols.sort()

zcta_metric_cols = [col for col in c_zcta_dma.columns if col not in  ['dma','zcta']]
zcta_metric_cols.sort()

tract_metric_cols = [col for col in c_tract.columns if col not in  ['state', 'county', 'state_NAME', 'GEOID', 'NAME', 'tract']]
tract_metric_cols.sort()

block_group_metric_cols = [col for col in c_block_group.columns if col not in  ['state', 'county', 'state_NAME', 'GEOID', 'NAME', 'tract', 'block group']]
block_group_metric_cols.sort()

tract_geoid = sorted(c_tract['GEOID'].unique())

block_group_geoid = sorted(c_block_group['GEOID'].unique())

dmas = c_dma['dma'].unique()
# dmas.sort()

states = state_name['state_NAME'].unique()

cities = ['New York', 'Los Angeles', 'San Francisco']

# Set up Dash app ##################################################################################
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Census Data Explorer", style={'fontFamily': 'Arial'}),

    dcc.Tabs([

        dcc.Tab(label='States', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Label("Select Metrics", style={'fontFamily': 'Arial','fontWeight': 'bold'}),
                dcc.Dropdown(
                    id='state-metric-selector',
                    options=zcta_metric_cols,
                    value=['Pop - Total'],
                    multi=True,
                    placeholder="Select metrics...",
                    style={'fontFamily': 'Arial'}
                ),
                html.Iframe(id='state_map', width='100%', height='650')
            ], style={'padding': '20px'})
        ]),

        dcc.Tab(label='DMAs', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Label("Select Metrics", style={'fontFamily': 'Arial','fontWeight': 'bold'}),
                dcc.Dropdown(
                    id='dma-metric-selector',
                    options=dma_metric_cols,
                    value=['Pop - Total'],
                    multi=True,
                    placeholder="Select metrics...",
                    style={'fontFamily': 'Arial'}
                ),
                html.Iframe(id='dma_map', width='100%', height='650')
            ], style={'padding': '20px'})
        ]),

        dcc.Tab(label='Counties', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Label("Select Metrics", style={'fontFamily': 'Arial','fontWeight': 'bold'}),
                dcc.Dropdown(
                    id='county-metric-selector',
                    options=county_metric_cols,
                    value=['Pop - Total'],
                    multi=True,
                    placeholder="Select metrics...",
                    style={'fontFamily': 'Arial'}
                ),
                html.Iframe(id='county_map', width='100%', height='650')
            ], style={'padding': '20px'})
        ]),

        dcc.Tab(label='ZCTAs', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Div([
                    html.Label("Select Metrics", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='zcta-metric-selector',
                        options=zcta_metric_cols,
                        value=['Pop - Total'],
                        multi=True,
                        placeholder="Select metrics..."
                    ),
                    html.Label("Select DMA", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='dma-selector',
                        options=dmas,
                        value='New York',
                        multi=False,
                        placeholder="Select DMA..."
                    ),
                    html.Label("Minimum Population", style={'fontWeight': 'bold'}),
                    dcc.Input(
                        id='zcta-pop-min',
                        type='number',
                        value=0,
                        min=0,
                        step=1
                    ),
                ], style={'fontFamily': 'Arial', 'width': '300px', 'padding': '20px', 'flexShrink': 0}),
                html.Div([
                    html.Iframe(id='zcta_map', width='100%', height='700')
                ], style={'flexGrow': 1, 'padding': '20px'})
            ], style={'display': 'flex', 'alignItems': 'flex-start'})
        ]),

        dcc.Tab(label='Tracts', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Div([
                    html.Label("Select Metrics", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='tract-metric-selector',
                        options=tract_metric_cols,
                        value=['Pop - Total'],
                        multi=True,
                        placeholder="Select metrics...",
                    ),
                    html.Label("Select State", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='state-selector',
                        options=states,
                        value='New York',
                        multi=False,
                        placeholder="Select State..."
                    ),
                    html.Label("Exclude GEOIDs", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='tract-exclude',
                        options=tract_geoid,
                        multi=True,
                        placeholder="Select GEOIDs to exclude...",
                    ),
                    html.Label("Minimum Population", style={'fontWeight': 'bold'}),
                    dcc.Input(
                        id='tract-pop-min',
                        type='number',
                        value=0,
                        min=0,
                        step=1
                    ),
                ], style={'fontFamily': 'Arial', 'width': '300px', 'padding': '20px', 'flexShrink': 0}),
                html.Div([
                    html.Iframe(id='tract_map', width='100%', height='700')
                ], style={'flexGrow': 1, 'padding': '20px'})
            ], style={'display': 'flex', 'alignItems': 'flex-start'})
        ]),

        dcc.Tab(label='Block Groups', style={'fontFamily': 'Arial'}, selected_style={'fontFamily': 'Arial'}, children=[
            html.Div([
                html.Div([
                    html.Label("Select Metrics", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='block-group-metric-selector',
                        options=block_group_metric_cols,
                        value=['Pop - Total'],
                        multi=True,
                        placeholder="Select metrics...",
                    ),
                    html.Label("Select City", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='city-selector',
                        options=cities,
                        value='New York',
                        multi=False,
                        placeholder="Select City..."
                    ),
                    html.Label("Exclude GEOIDs", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='block-group-exclude',
                        options=block_group_geoid,
                        multi=True,
                        placeholder="Select GEOIDs to exclude...",
                    ),
                    html.Label("Minimum Population", style={'fontWeight': 'bold'}),
                    dcc.Input(
                        id='block-group-pop-min',
                        type='number',
                        value=0,
                        min=0,
                        step=1
                    ),
                ], style={'fontFamily': 'Arial', 'width': '300px', 'padding': '20px', 'flexShrink': 0}),
                html.Div([
                    html.Iframe(id='block_group_map', width='100%', height='700')
                ], style={'flexGrow': 1, 'padding': '20px'})
            ], style={'display': 'flex', 'alignItems': 'flex-start'})
        ])
    ])
])


def generate_state_map(selected_metrics):
    """
    Build folium map with user selected metrics
    """
    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=state_geom_json,
            data=c_state,
            columns=['state', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        # Loop through the geojson object and add a new property (i) and assign a value from dataframe
        for s in my_chp.geojson.data['features']:
            if s['id'] in list(c_state['state']):
                val = c_state_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['State'] = s['id']
            s['properties'][i] = val
        # add a tooltip/hover to the choropleth's geojson
        folium.GeoJsonTooltip(['State',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_county_map(selected_metrics):
    """
    Build folium map with user selected metrics
    """
    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=county_geom_json,
            data=c_county_state,
            columns=['GEOID', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        my_geos = list(c_county_state['GEOID'])
        for s in my_chp.geojson.data['features']:
            if s['id'] in my_geos:
                val = c_county_state_indexed.loc[s['id'], i]
                
                if 'NAME' in c_county_state_indexed.columns:
                    county_name = c_county_state_indexed.loc[s['id'], 'NAME']
                else:
                    county_name = s['id']
            else:
                val = 0
                county_name = s['id']
            s['properties']['County'] = county_name
            s['properties'][i] = val
        folium.GeoJsonTooltip(['County',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_dma_map(selected_metrics):
    """
    Build folium map with user selected metrics
    """
    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=dma_geom_json,
            data=c_dma,
            columns=['dma', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        my_geos = list(c_dma['dma'])
        for s in my_chp.geojson.data['features']:
            if s['id'] in my_geos:
                val = c_dma_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['DMA'] = s['id']
            s['properties'][i] = val
        folium.GeoJsonTooltip(['DMA',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_zcta_map(selected_metrics, selected_dma, pop_min=None):
    """
    Build folium map with user selected metrics
    """
    zcta_geom_select = zcta_geom[zcta_geom['dma']==selected_dma].reset_index()
    zcta_geom_select = zcta_geom_select[['ZCTA5CE20', 'geometry']]
    zcta_geom_select = zcta_geom_select.set_index('ZCTA5CE20')
    zcta_json_select = zcta_geom_select.to_json()

    c_zcta_dma_select = c_zcta_dma[c_zcta_dma['dma']==selected_dma].reset_index(drop=True)
    c_zcta_dma_select = c_zcta_dma_select.rename(columns={'zcta': 'ZCTA5CE20'})

    c_zcta_dma_select = c_zcta_dma_select.loc[c_zcta_dma_select['Pop - Total'] >= (pop_min or 0)]

    c_zcta_dma_select_indexed = c_zcta_dma_select.set_index('ZCTA5CE20')

    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=zcta_json_select,
            data=c_zcta_dma_select,
            columns=['ZCTA5CE20', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        my_geos = list(c_zcta_dma_select['ZCTA5CE20'])
        for s in my_chp.geojson.data['features']:
            if s['id'] in my_geos:
                val = c_zcta_dma_select_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['ZCTA'] = s['id']
            s['properties'][i] = val
        folium.GeoJsonTooltip(['ZCTA',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_tract_map(selected_metrics, selected_state, pop_min=None, exclude=None):
    """
    Build state level tract map with user selected metrics
    """
    state_fips = state_name.loc[state_name['state_NAME'] == selected_state, 'state'].values[0]

    c_tract_select = c_tract.loc[c_tract['GEOID'].str[:2] == state_fips].reset_index(drop=True)
    tract_geom_select = tract_geom[tract_geom['GEOID'].str[:2] == state_fips]

    tract_geom_select = tract_geom_select.set_index('GEOID')
    tract_geom_select_json = tract_geom_select.to_json()

    if pop_min is not None:
        c_tract_select = c_tract_select.loc[c_tract_select['Pop - Total'] >= pop_min]
    if exclude:
        c_tract_select = c_tract_select.loc[~c_tract_select['GEOID'].isin(exclude)]

    c_tract_select_indexed = c_tract_select.set_index('GEOID')

    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=tract_geom_select_json,
            data=c_tract_select,
            columns=['GEOID', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        my_geos = list(c_tract_select['GEOID'])
        for s in my_chp.geojson.data['features']:
            if s['id'] in my_geos:
                val = c_tract_select_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['Tract'] = s['id']
            s['properties'][i] = val
        folium.GeoJsonTooltip(['Tract',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_block_group_map(selected_metrics, selected_city, pop_min=None, exclude=None):
    """
    Build folium map with user selected metrics
    """
    if selected_city == 'San Francisco':
        sf = ['06075'] # SF

        c_block_group_select = c_block_group.loc[c_block_group['GEOID'].str[:5].isin(sf)].reset_index(drop=True)
        block_group_geom_select = block_group_geom[block_group_geom['GEOID'].str[:5].isin(sf)]
    elif selected_city == 'Los Angeles':
        la = ['06037'] # SF

        c_block_group_select = c_block_group.loc[c_block_group['GEOID'].str[:5].isin(la)].reset_index(drop=True)
        block_group_geom_select = block_group_geom[block_group_geom['GEOID'].str[:5].isin(la)]
    else:
        nyc = ['36005', '36047', '36061', '36081', '36085'] # NYC counties

        c_block_group_select = c_block_group.loc[c_block_group['GEOID'].str[:5].isin(nyc)].reset_index(drop=True)    
        block_group_geom_select = block_group_geom[block_group_geom['GEOID'].str[:5].isin(nyc)]

    block_group_geom_select = block_group_geom_select.set_index('GEOID')
    block_group_geom_select_json = block_group_geom_select.to_json()

    if pop_min is not None:
        c_block_group_select = c_block_group_select.loc[c_block_group_select['Pop - Total'] >= pop_min]
    if exclude:
        c_block_group_select = c_block_group_select.loc[~c_block_group_select['GEOID'].isin(exclude)]

    c_block_group_select_indexed = c_block_group_select.set_index('GEOID')

    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "pct_male" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=block_group_geom_select_json,
            data=c_block_group_select,
            columns=['GEOID', i],
            key_on="feature.id",
            fill_opacity=0.7,
            fill_color=my_color,
            nan_fill_color="white",
            nan_fill_opacity=0,
            line_opacity=0.2,
            line_weight=0.1,
            legend_name=i,
            highlight=True,
            name=i,
            overlay=False
        ).add_to(m)

        my_geos = list(c_block_group_select['GEOID'])
        for s in my_chp.geojson.data['features']:
            if s['id'] in my_geos:
                val = c_block_group_select_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['Block Group'] = s['id']
            s['properties'][i] = val
        folium.GeoJsonTooltip(['Block Group',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

# Callback to update the map
@app.callback(
    Output('state_map', 'srcDoc'),
    Input('state-metric-selector', 'value')
)
def update_state_map(metrics):
    return generate_state_map(metrics)

@app.callback(
    Output('dma_map', 'srcDoc'),
    Input('dma-metric-selector', 'value')
)
def update_dma_map(metrics):
    return generate_dma_map(metrics)

@app.callback(
    Output('county_map', 'srcDoc'),
    Input('county-metric-selector', 'value')
)
def update_county_map(metrics):
    return generate_county_map(metrics)

@app.callback(
    Output("zcta_map", "srcDoc"),
    Input('zcta-metric-selector', 'value'),
    Input("dma-selector", "value"),
    Input("zcta-pop-min", "value")
)
def update_zcta_map(metrics, dma, pop_min):
    return generate_zcta_map(metrics, dma, pop_min)

@app.callback(
    Output('tract_map', 'srcDoc'),
    Input('tract-metric-selector', 'value'),
    Input("state-selector", "value"),
    Input("tract-pop-min", "value"),
    Input("tract-exclude", "value")
)
def update_tract_map(metrics, state, pop_min, exclude):
    return generate_tract_map(metrics, state, pop_min, exclude)

@app.callback(
    Output('block_group_map', 'srcDoc'),
    Input('block-group-metric-selector', 'value'),
    Input("city-selector", "value"),
    Input("block-group-pop-min", "value"),
    Input("block-group-exclude", "value")
)
def update_block_group_map(metrics, city, pop_min, exclude):
    return generate_block_group_map(metrics, city, pop_min, exclude)

if __name__ == '__main__':
    app.run_server(debug=True)
