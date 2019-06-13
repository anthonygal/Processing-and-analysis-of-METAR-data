from cassandra.cluster import Cluster
import numpy as np
import pandas as pd
import glob
from pykrige.ok import OrdinaryKriging
from pykrige.kriging_tools import write_asc_grid
import pykrige.kriging_tools as kt
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Path, PathPatch



# rows = session.execute('SELECT * FROM agaltier_zkang_metar_France_2 LIMIT 10 ;')
# for row in rows:
#     print(row)

def getIndicatorMap(year, month, day, hour, indicator):
    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')
    query = """
            SELECT latitude, longitude, """ + indicator + """
            FROM agaltier_zkang_metar_France_2
            WHERE year = """ + str(year) + """
            AND month = """ + str(month) + """
            AND day = """ + str(day) + """
            AND hour = """ + str(hour) + """


            LIMIT 10000
            ALLOW FILTERING ;
            """

    rows = session.execute(query)

    lats = []
    lons= []
    values = []

    for row in rows:
        if (row[0]!=None) & (row[0]>=40) & (row[0]<=50) & (row[1]!=None) & (row[1]>=-5) & (row[1]<=7.5) & (row[2]!=None) :
            lats.append(row[0])
            lons.append(row[1])
            values.append(row[2])

    res = {}
    res['latitudes'] = lats
    res['longitudes'] = lons
    res['values'] = values

#     return res
#
# res = get_data_by_time(2008,9,11,4, 'tmpf')

    lats = np.array(res['latitudes'])
    lons = np.array(res['longitudes'])
    data = np.array(res['values'])

    minlon = -5
    maxlon = 7.5

    minlat = 40
    maxlat = 50

    minvalues = min(data)
    maxvalues = max(data)

    grid_lat = np.arange(minlat, maxlat, 0.1)
    grid_lon = np.arange(minlon, maxlon, 0.1)


    OK = OrdinaryKriging(list(lons), list(lats), list(data), variogram_model='gaussian', verbose=True, enable_plotting=False,nlags=20)
    z1, ss1 = OK.execute('grid', grid_lon, grid_lat)

    xintrp, yintrp = np.meshgrid(grid_lon, grid_lat)
    fig, ax = plt.subplots(figsize=(10,10))
    m = Basemap(llcrnrlon=lons.min()-0.1,llcrnrlat=lats.min()-0.1,urcrnrlon=lons.max()+0.1,urcrnrlat=lats.max()+0.1, projection='merc', resolution='h',area_thresh=1000.,ax=ax)
    m.drawcoastlines() #draw coastlines on the map
    m.drawcountries() #draw countries
    x,y=m(xintrp, yintrp) # convert the coordinates into the map scales

    ln,lt=m(lons,lats)
    cs=ax.contourf(x, y, z1, np.linspace(minvalues, maxvalues, len(data)),extend='both',cmap='jet') #plot the data on the map.
    cbar=m.colorbar(cs,location='right',pad="7%") #plot the colorbar on the map
    plt.show()
    fname = './' + indicator + str(year) + str(month) + str(day) + str(hour) + '_map.png'
    print(fname)
    plt.savefig(fname)
