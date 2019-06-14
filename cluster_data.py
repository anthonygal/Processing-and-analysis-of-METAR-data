from cassandra.cluster import Cluster

import numpy as np
import pandas as pd
import glob

from math import sqrt

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

def getIndicatorCluster(indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour):
    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    inds = ''
    for indicator in indicators:
        inds = inds +  ', ' + indicator

    if(start_year != end_year):
        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_2
                WHERE year >= """ + str(start_year) + """
                AND year <= """ + str(end_year) + """
                LIMIT 10000
                ALLOW FILTERING;
                """
    elif (start_month != end_month):
        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_2
                WHERE year = """ + str(start_year) + """
                AND month >= """ + str(start_month) + """
                AND month <= """ + str(end_month) + """
                LIMIT 10000
                ALLOW FILTERING;
                """
    elif(start_day != end_day):
        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_2
                WHERE year = """ + str(start_year) + """
                AND month = """ + str(start_month) + """
                AND day >= """ + str(start_day) + """
                AND day <= """ + str(end_day) + """
                LIMIT 100
                ALLOW FILTERING;
                """
    elif (start_hour != end_hour):
        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_2
                WHERE year = """ + str(start_year) + """
                AND month = """ + str(start_month) + """
                AND day = """ + str(start_day) + """
                AND hour >= """ + str(start_hour) + """
                AND hour <= """ + str(end_hour) + """
                LIMIT 10000
                ALLOW FILTERING;
                """
    else:
        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_2
                WHERE year = """ + str(start_year) + """
                AND month = """ + str(start_month) + """
                AND day = """ + str(start_day) + """
                AND hour = """ + str(start_hour) + """
                LIMIT 10000
                ALLOW FILTERING;
                """

    print(query)
    rows = session.execute(query)

    lats = []
    lons= []
    values = []

    for row in rows:
        flag = True
        for x in row:
            if (x == None): flag = False
        if (flag & (row[0]>=40) & (row[0]<=50) & (row[1]!=None) & (row[1]>=-5) & (row[1]<=7.5)):
            res = {}
            i = 2
            for indicator in indicators:
                res[indicator] = row[i]
                i = i+1
            res['latitude'] = row[0]
            res['longitude'] = row[1]
            i = 2

            yield res

def select_random(data, n=3): ## reservoir sampling
    result = [0] * n
    for index, r in enumerate(data):
        if index < n:
            result[index] = r
        else:
            if random.uniform(0,1) < n/(index+1):
                result[random.randint(0, n-1)] = r

    return result


def kmeans(rows, day, k=20):
    n_conv = 20
    epsilon = 0.01

    def dist(r, c):
        n = len(r)
        temp = []
        for i in range(2,n):
            temp.append(r[i])
        temp2 = []
        for i in range(len(temp)):
            temp2.append(temp[i] - c[i])
        temp2 = [x**2 for x in temp2]
        return sqrt(sum(temp2))

    def get_values(r):
        c = []
        for i in range(2,n):
            c.append(r[i])
        return tuple(c)

    def sum_v(x,y):
        return tuple([ a+b for a,b in zip(x,y)])
    def mul_v(s,v):
        return tuple([ s*v_i for v_i in v])

    centroids = [get_values(r) for r in select_random(res, n=k)]
    centroids_count = None
    for i in range(n_conv):
        accu_centroids = [(0,) * (len(r)-2)] * k
        accu_count = [0] * k
        for r in res:
            dist_centroids = [dist(r, c) for c in centroids]
            centroid_id = dist_centroids.index(min(dist_centroids))
            accu_centroids[centroid_id] = sum_v(accu_centroids[centroid_id], get_values(r))
            accu_count[centroid_id] += 1
        new_centroids = [mul_v(1/n, c) for c, n in zip(accu_centroids, accu_count)]
        diff_centroids = [dist(old, new) for old, new in zip(centroids, new_centroids)]
        centroids = new_centroids
        centroids_count = accu_count
        if max(diff_centroids) < epsilon:
            break
    return (centroids, centroids_count)



# getIndicatorCluster('tmpf', 2002, 1, 1, 1, 2003, 1, 1, 15)