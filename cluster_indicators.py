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



def getIndicatorCluster(indicators, start_year=2001, start_month=1, start_day=1, start_hour=0,
                        end_year=2009, end_month=12, end_day=31, end_hour=24):
    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    # TODO: Add check for length of periode

    inds = ''
    for indicator in indicators:
        inds = inds +  ', ' + indicator

    query = """
                SELECT latitude, longitude""" + inds + """
                FROM agaltier_zkang_metar_France_3
                WHERE year = """ + str(start_year) + """
                AND month >= """ + str(start_month) + """
                AND month >= """ + str(start_month) + """
                AND day <= """ + str(start_day) + """
                AND day <= """ + str(end_day) + """
                AND hour <= """ + str(start_hour) + """
                AND hour <= """ + str(end_hour) + """
                """


    print(query)
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

    def sum_v(x,y):
        return tuple([ a+b for a,b in zip(x,y)])
    def mul_v(s,v):
        return tuple([ s*v_i for v_i in v])

    centroids = [get_values(r) for r in select_random(getIndicatorCluster(indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour), n=k)]
    centroids_count = None
    for i in range(n_conv):
        accu_centroids = [(0,) * len(indicators)] * k
        accu_count = [0] * k
        for r in getIndicatorCluster(indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour):
            dist_centroids = [dist(r[2:], c) for c in centroids]
            centroid_id = dist_centroids.index(min(dist_centroids))
            accu_centroids[centroid_id] = sum_v(accu_centroids[centroid_id], get_values(r))
            accu_count[centroid_id] += 1

        new_centroids = [mul_v(1/n, c) for c, n in zip(accu_centroids, accu_count)]
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
    """
    {'centers': [{'tmpf': 70.97648848293485},
    {'tmpf': 57.769181321012965},
    {'tmpf': 33.50896658801345},
    {'tmpf': 49.27801017989916},
    {'tmpf': 42.46053397525058}],
    'count': [97331, 144011, 24424, 96416, 50543]}
    """
    #centroids_info = kmeans(km, indicators, start_year, start_month, start_day, start_hour, end_year, end_month, end_day, end_hour)
    centroids_info = {'centers': [{'tmpf': 70.97648848293485},
    {'tmpf': 57.769181321012965},
    {'tmpf': 33.50896658801345},
    {'tmpf': 49.27801017989916},
    {'tmpf': 42.46053397525058}],
    'count': [97331, 144011, 24424, 96416, 50543]}
    inds = ''
    for indicator in indicators:
        inds = inds +  ', ' + indicator

    # initialize a sparkcontext
    sc = SparkContext.getOrCreate()
    # Connection avec cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect('agaltier_zkang_projet')

    query = """
                SELECT latitude, longitude, year, month""" + inds + """
                FROM agaltier_zkang_metar_France_3
                WHERE year = """ + str(start_year) + """
                AND month >= """ + str(start_month) + """
                AND month >= """ + str(start_month) + """
                AND day <= """ + str(start_day) + """
                AND day <= """ + str(end_day) + """
                AND hour <= """ + str(start_hour) + """
                AND hour <= """ + str(end_hour) + """
                """

    rows = session.execute(query)
    # Utiliser spark pour parallelization
    rows_paral = sc.parallelize(rows)
    for i in range(len(indicators)):
        rows_paral = rows_paral.filter(lambda a: a[4+i] is not None)

    # It is supposed to have at most 2 indicators
    if len(indicators) == 1:
        res_map = rows_paral.map(lambda a: ((a[0], a[1], a[2], a[3]), np.array([1, a[4]])))
        res_redu = res_map.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
        res = res_redu.map(lambda a: ((a[0][0], a[0][1], a[0][2], a[0][3]), a[1][1]/a[1][0]))
    else :
        res_map = rows_paral.map(lambda a: ((a[0], a[1], a[2], a[3]), np.array([1, a[5], a[6]])))
        res_redu = res_map.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
        res = res_redu.map(lambda a: ((a[0][0], a[0][1], a[0][2], a[0][3]), a[1][1]/a[1][0], a[1][2]/a[1][0]))

    # Example of row ((45.36289978027344, 5.329400062561035, 2002, 4), 30.200000762939453)
    raw_data = {}
    # supposed to be same year
    length_month = end_month - start_month + 1
    for row in res.collect():
        if (row[0][0], row[0][1]) not in raw_data:
            raw_data[(row[0][0], row[0][1])] = np.empty((length_month, len(indicators)))
        raw_data[(row[0][0], row[0][1])][row[0][3]-start_month, :] = row[1:]

    """
    Example of raw_data
    (47.444400787353516, 0.7271999716758728): array([[48.45714297],
                                                     [46.40000025],
                                                     [38.59999975],
                                                     [52.40000025]])
    """
    centroids = [[v for k, v in c.items()] for c in centroids_info["centers"]]
    cluster_data = {}
    for i in range(length_month):
        for k, v in raw_data.items():
            dist_centroids = [dist(v[i, :], c) for c in centroids]
            centroid_id = dist_centroids.index(min(dist_centroids))
            if k not in cluster_data:
                cluster_data[k] = np.empty((length_month, len(indicators)))
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
    fig, ax_arr = plt.subplots(length_month, sharey=True)
    fig.set_size_inches(18.5, 10.5)
    for i, ax in enumerate(ax_arr.flatten()):

        m = Basemap(projection='merc', \
                llcrnrlat=42, urcrnrlat=51, \
                llcrnrlon=-5, urcrnrlon=8, \
                lat_ts=20, ax=ax, \
                resolution='h')
        m.drawcoastlines() #draw coastlines on the map
        m.drawcountries()
        for k, v in cluster_data.items():
        #x, y = m(k[0], k[1])
            m.scatter(k[1], k[0], marker='o', color=colors[int(v[i, 0])], latlon=True)
    plt.savefig("./kmeans_{}.png".format("test"))
    plt.close(fig)

plot_kmeans_map(5, ['tmpf'], 2002, 1, 1, 1, 2002, 10, 1, 1)


# getIndicatorCluster('tmpf', 2002, 1, 1, 1, 2002, 4, 1, 4)
# kmeans(5, ['tmpf'], 2002, 1, 1, 1, 2002, 4, 1, 1)
