"""Create an interactive plot in a browser window"""

import folium
import geopandas as gpd
import pandas as pd
from dash import Dash, html, dcc, Input, Output

MALE_COLOR = "Blues"
FEMALE_COLOR = "Reds"
MF_COLOR = "RdBu"
INCOME_COLOR = "Greens"

# Load files
zcta_geom_raw = gpd.read_file("zcta_geom.shp") # needs zcta_df.cpg, zcta_df.shx, etc

zcta_to_dma = pd.read_csv("zcta_to_dma.csv", dtype={'zcta':object})

dma_polygons_raw = gpd.read_file('dma_polygons.geojson') # https://team.carto.com/u/andrew/tables/dma_master_polygons/public
dma_polygons_raw['cartodb_id'] = dma_polygons_raw['cartodb_id'].astype(str)
dma_polygons_raw['dma_code'] = dma_polygons_raw['dma_code'].astype(str)

dma_polygon_map = pd.read_csv('dma_polygon_map.csv')

c_zcta_dma = pd.read_csv("c_zcta_dma.csv", dtype={'zcta':object})

c_dma = pd.read_csv("c_dma.csv")

# Set up the geographic geometry files
dma_polygons = dma_polygons_raw.merge(dma_polygon_map, left_on='dma_name', right_on='DMA Polygons')

dma_geo = dma_polygons[['DMA', 'geometry']]
dma_geo = dma_geo.set_index('DMA')

dma_geo_json = dma_geo.to_json()

zcta_geom = zcta_geom_raw.merge(zcta_to_dma[['zcta','dma']], how="left", left_on='ZCTA5CE20', right_on='zcta')

# Create a DMA indexed version of the dataframes so we can lookup values
c_dma_indexed = c_dma.set_index('dma')


# Lists for user
metric_cols = [col for col in c_dma.columns if col != 'dma']
metric_cols.sort()

dmas = c_dma['dma'].unique()
# dmas.sort()

# Set up Dash app
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Census Data Explorer", style={'fontFamily': 'Arial'}),

    html.Div([
        html.Label("Select Metrics", style={'fontFamily': 'Arial','fontWeight': 'bold'}),
        dcc.Dropdown(
            id='metric-selector',
            options=metric_cols,
            value=['Pop - Total'],
            multi=True,
            placeholder="Select metrics...",
            style={'fontFamily': 'Arial'}
        ),
        html.Iframe(id='dma_map', width='100%', height='500')
    ]),
    html.Div([
        html.Label("Select DMA", style={'fontFamily': 'Arial','fontWeight': 'bold'}),
        dcc.Dropdown(
            id='dma-selector',
            options=dmas,
            value='New York',
            multi=False,
            placeholder="Select DMA...",
            style={'fontFamily': 'Arial'}
        ),
        html.Iframe(id='zcta_map', width='100%', height='500')
    ])
])

def generate_dma_map(selected_metrics):
    """
    Build folium map with user selected metrics
    """
    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "mf_ratio" in i:
            my_color = MF_COLOR
        elif "Income" in i:
            my_color = INCOME_COLOR
        else:
            my_color = MALE_COLOR

        my_chp = folium.Choropleth(
            tiles="cartodb positron",
            geo_data=dma_geo_json,
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

        # Loop through the geojson object and add a new property (i) and assign a value from dataframe
        for s in my_chp.geojson.data['features']:
            if s['id'] in list(c_dma['dma']):
                val = c_dma_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['DMA'] = s['id']
            s['properties'][i] = val
        # add a tooltip/hover to the choropleth's geojson
        folium.GeoJsonTooltip(['DMA',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

def generate_zcta_map(selected_metrics, selected_dma):
    """
    Build folium map with user selected metrics
    """
    zcta_geom_select = zcta_geom[zcta_geom['dma']==selected_dma].reset_index()
    zcta_geom_select = zcta_geom_select[['ZCTA5CE20', 'geometry']]
    zcta_geom_select = zcta_geom_select.set_index('ZCTA5CE20')
    zcta_json_select = zcta_geom_select.to_json()

    c_zcta_dma_select = c_zcta_dma[c_zcta_dma['dma']==selected_dma].reset_index(drop=True)
    c_zcta_dma_select = c_zcta_dma_select.rename(columns={'zcta': 'ZCTA5CE20'})

    c_zcta_dma_select_indexed = c_zcta_dma_select.set_index('ZCTA5CE20')

    m = folium.Map(tiles=None)

    for i in selected_metrics:
        if "Female" in i:
            my_color = FEMALE_COLOR
        elif "mf_ratio" in i:
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

        # Loop through the geojson object and add a new property (i) and assign a value from dataframe
        for s in my_chp.geojson.data['features']:
            if s['id'] in list(c_zcta_dma_select['ZCTA5CE20']):
                val = c_zcta_dma_select_indexed.loc[s['id'], i]
            else:
                val = 0
            s['properties']['ZCTA'] = s['id']
            s['properties'][i] = val
        # add a tooltip/hover to the choropleth's geojson
        folium.GeoJsonTooltip(['ZCTA',i]).add_to(my_chp.geojson)

    folium.TileLayer(tiles='cartodb positron',control=False).add_to(m)
    folium.LayerControl().add_to(m)
    m.fit_bounds(m.get_bounds(), padding=(10, 10))
    return m.get_root().render()

# Callback to update the map
@app.callback(
    Output('dma_map', 'srcDoc'),
    Input('metric-selector', 'value')
)
def update_dma_map(metrics):
    return generate_dma_map(metrics)

@app.callback(
    Output("zcta_map", "srcDoc"),
    Input('metric-selector', 'value'),
    Input("dma-selector", "value")
)
def update_zcta_map(metrics, dma):
    return generate_zcta_map(metrics, dma)

if __name__ == '__main__':
    app.run_server(debug=True)
