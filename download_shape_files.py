""" Download shape files"""

import pygris

# Get the cartographic boundary files with cb = True, seems to only work for 2020 right now
state_geom = pygris.states(year=2020, cb = True)
state_geom.to_file("state_geom.shp", index=False)

county_geom = pygris.counties(year=2020, cb = True)
county_geom.to_file("county_geom.shp", index=False)

zcta_geom = pygris.zctas(year=2020, cb = True)
zcta_geom.to_file("zcta_geom.shp", index=False)

tract_geom = pygris.tracts(year=2020, cb = True)
tract_geom.to_file("tract_geom.shp", index=False)

block_group_geom = pygris.block_groups(year=2020, cb = True)
block_group_geom.to_file("block_group_geom.shp", index=False)