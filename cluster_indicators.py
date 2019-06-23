from cassandra.cluster import Cluster

import numpy as np
import glob
from math import sqrt
import random

from pyspark import  SparkContext
from functools import reduce

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import matplotlib.cm as cm

import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

def getIndicatorCluster(indicators, start_year=2001, start_month=1, start_day=1, start_hour=0,
                        end_year=2001, end_month=12, end_day=31, end_hour=24):
    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    if end_year != start_year:
        raise ValueError("The period is supposed to be in the same year")

    inds = ''
    for indicator in indicators:
        inds = inds +  ', ' + indicator

        query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_3
                WHERE year = """ + str(start_year)
    if start_month != end_month:
        query += \
                """
                AND month >= """ + str(start_month) + """
                AND month <= """ + str(end_month)
    else:
        query += \
                """
                AND month = """ + str(start_month)
        if start_day != end_day:
                query += \
                """
                AND day >= """ + str(start_day) + """
                AND day <= """ + str(end_day)
        else:
            query += \
                """
                AND day = """ + str(start_day)
            if start_hour != end_hour:
                query += \
                """
                AND hour >= """ + str(start_hour) + """
                AND hour <= """ + str(end_hour) + """
                """
            else :
                query += \
                """
                AND hour = """ + str(start_hour)

    rows = session.execute(query)

    for row in rows:
        flag = True
        for x in row:
            if (x == None): flag = False
        if (flag & (row[0]>=40) & (row[0]<=50) & (row[1]!=None) & (row[1]>=-5) & (row[1]<=7.5)):
            yield row

def select_random(data, n=3): ## reservoir sampling
    result = [0] * n
    for index, r in enumerate(data):
        if index < n:
            result[index] = r
        else:
            if random.uniform(0,1) < n/(index+1):
                result[random.randint(0, n-1)] = r

    return result

def dist(r, c):
    n = len(r)
    temp = []
    for i in range(n):
        temp.append(r[i])
    temp2 = []
    for i in range(len(temp)):
        temp2.append(temp[i] - c[i])
    temp2 = [x**2 for x in temp2]
    return sqrt(sum(temp2))

def kmeans(k, indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour):
    n_conv = 20
    epsilon = 0.01

    def dist_c(c1, c2):
        temp = []
        for i in range(len(c1)):
            temp.append(c1[i] - c2[i])
        temp = [x**2 for x in temp]
        return sqrt(sum(temp))

    def get_values(r):
        c = []
        for i in range(2,len(r)):
            c.append(r[i])
        return tuple(c)

    def addition(x,y):
        return tuple([ a+b for a,b in zip(x,y)])
    def multiplication(s,v):
        return tuple([ s*v_i for v_i in v])

    centroids = [get_values(r) for r in select_random(getIndicatorCluster(indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour), n=k)]
    centroids_count = None
    for i in range(n_conv):
        accu_centroids = [(0,) * len(indicators)] * k
        accu_count = [0] * k
        for r in getIndicatorCluster(indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour):
            dist_centroids = [dist(r[2:], c) for c in centroids]
            centroid_id = dist_centroids.index(min(dist_centroids))
            accu_centroids[centroid_id] = addition(accu_centroids[centroid_id], get_values(r))
            accu_count[centroid_id] += 1

        new_centroids = [multiplication(1/n, c) for c, n in zip(accu_centroids, accu_count)]
        diff_centroids = [dist_c(old, new) for old, new in zip(centroids, new_centroids)]
        centroids = new_centroids
        centroids_count = accu_count
        if max(diff_centroids) < epsilon:
            break

    res = {}
    res['centers'] = []
    res['count'] = centroids_count
    for centroid in centroids:
        c = {}
        for index, indicator in enumerate(indicators):
            c[indicator]=centroid[index]
        res['centers'].append(c)
    return res

def plot_kmeans_map(km, indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour):

    def legend_without_duplicate_labels(ax):
        handles, labels = ax.get_legend_handles_labels()
        unique = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l not in labels[:i]]
        ax.legend(*zip(*unique))

    """
    Example of centroids_info
    {'centers': [{'tmpf': 70.97648848293485},
    {'tmpf': 57.769181321012965},
    {'tmpf': 33.50896658801345},
    {'tmpf': 49.27801017989916},
    {'tmpf': 42.46053397525058}],
    'count': [97331, 144011, 24424, 96416, 50543]}
    """
    centroids_info = kmeans(km, indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour)

    inds = ''
    for indicator in indicators:
        inds = inds +  ', ' + indicator

    sc = SparkContext.getOrCreate()

    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    query =     """
                SELECT latitude, longitude, year, month""" + inds + """
                FROM agaltier_zkang_metar_France_3
                WHERE year = """ + str(start_year)
    if start_month != end_month:
        query += \
                """
                AND month >= """ + str(start_month) + """
                AND month <= """ + str(end_month)
    else:
        query += \
                """
                AND month = """ + str(start_month)
        if start_day != end_day:
                query += \
                """
                AND day >= """ + str(start_day) + """
                AND day <= """ + str(end_day)
        else:
            query += \
                """
                AND day = """ + str(start_day)
            if start_hour != end_hour:
                query += \
                """
                AND hour >= """ + str(start_hour) + """
                AND hour <= """ + str(end_hour) + """
                """
            else :
                query += \
                """
                AND hour = """ + str(start_hour)

    rows = session.execute(query)
    rows_paral = sc.parallelize(rows)
    for i in range(len(indicators)):
        rows_paral = rows_paral.filter(lambda a: a[4+i] is not None)

    res_map = rows_paral.map(lambda a: ((a[0], a[1], a[2], a[3]), np.array([1, np.array(a[4:])])))
    res_redu = res_map.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
    res = res_redu.map(lambda a: ((a[0][0], a[0][1], a[0][2], a[0][3]), a[1][1]/a[1][0]))

    # Example of row ((45.36289978027344, 5.329400062561035, 2002, 4), 30.200000762939453)
    raw_data = {}
    # supposed to be one year, otherwise the
    nb_month = end_month - start_month + 1
    for row in res.collect():
        if (row[0][0], row[0][1]) not in raw_data:
            raw_data[(row[0][0], row[0][1])] = np.empty((nb_month, len(indicators)))
        raw_data[(row[0][0], row[0][1])][row[0][3]-start_month, :] = row[1]

    """
    Example of raw_data
    (47.444400787353516, 0.7271999716758728): array([[48.45714297],
                                                     [46.40000025],
                                                     [38.59999975],
                                                     [52.40000025]])
    """
    centroids = [[round(v, 2) for k, v in c.items()] for c in centroids_info["centers"]]
    cluster_data = {}
    for i in range(nb_month):
        for k, v in raw_data.items():
            dist_centroids = [dist(v[i, :], c) for c in centroids]
            centroid_id = dist_centroids.index(min(dist_centroids))
            if k not in cluster_data:
                cluster_data[k] = np.empty((nb_month, len(indicators)))
            cluster_data[k][i] = centroid_id
    """
    Example of cluster_data
    (47.444400787353516, 0.7271999716758728): array([[3.],
                                                     [3.],
                                                     [4.],
                                                     [3.]])
    """

    # plot
    colors = cm.rainbow(np.linspace(0, 1, len(centroids)))
    # 3 plot per row
    nb_row = nb_month//3
    nb_row += 1 if nb_month%3!=0 else 0
    fig, ax_arr = plt.subplots(nrows=nb_row, ncols=3)
    fig.set_size_inches(18.5, 10.5)
    handle_legend = [["{}: {}".format(indicators[i], c)
                    for i, c in enumerate(centroid)]
                    for centroid in centroids]

    for i, ax in enumerate(ax_arr.flatten()):
        if i >= nb_month:
            break

        ax.set_title("Clustering of {}-{}".format(start_year, start_month+i))
        m = Basemap(projection='merc', \
                llcrnrlat=42, urcrnrlat=51, \
                llcrnrlon=-5, urcrnrlon=8, \
                lat_ts=20, ax=ax, \
                resolution='h')
        m.drawcoastlines() #draw coastlines on the map
        m.drawcountries()
        for k, v in cluster_data.items():
            m.scatter(k[1], k[0], marker='o', color=colors[int(v[i, 0])], label = handle_legend[int(v[i, 0])], latlon=True)
        ax.legend(loc='upper left')
        legend_without_duplicate_labels(ax)

    fig.suptitle('Clustering of stations from {}-{} to {}-{} about {}'.format(start_year, start_month, end_year,end_month, inds[1:]), fontsize=22)
    plt.savefig("./kmeans_clustering_{}.png".format(inds[2:]))
    print("PNG fie is saved on:  ./kmeans_clustering_{}.png".format(inds[2:]))
    plt.close(fig)


plot_kmeans_map(7, ['tmpf', 'alti', 'feel'], 2009, 3, 3, 17, 2009, 5, 28, 17)

# getIndicatorCluster('tmpf', 2002, 1, 1, 1, 2002, 4, 1, 4)
# kmeans(5, ['tmpf'], 2002, 1, 1, 1, 2002, 4, 1, 1)
