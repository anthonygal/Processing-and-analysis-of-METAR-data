from cassandra.cluster import Cluster
from pyspark import  SparkContext
from functools import reduce
from matplotlib.ticker import MaxNLocator
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--station_id', type = str, default = 'LFST',
                     help= 'Station id in France')
parser.add_argument('--indicator', type = str, default = 'tmpf', 
                     help= 'Indicators of weather observations')
opt = parser.parse_args()
print(opt)

sc = SparkContext.getOrCreate()

cluster = Cluster(['localhost'])
session = cluster.connect('agaltier_zkang_projet')

def get_history(station, indicator):
    requete = """SELECT year, month, day, hour, minute, {} 
                 FROM agaltier_zkang_metar_France_1 where station_id = '{}'
                 LIMIT 100000;
              """.format(indicator, station)
    return session.execute(requete)

rows = get_history(opt.station_id, opt.indicator)

# Parallelisation
rows_paral = sc.parallelize(rows)

# exemple of row: ((2001, 5), (336.0, 11282.999938964844, 6.800000190734863, 46.400001525878906, 390615.9551180268))
rows_paral = rows_paral.filter(lambda a: a[5] is not None)
res_map = rows_paral.map(lambda a: ((a[0], a[1]), np.array([1, a[5], a[5], a[5], a[5]**2])))
res_redu = res_map.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], min(a[2], b[2]),
                                             max(a[3], b[3]), a[4]+b[4]))
res = res_redu.map(lambda a: ((a[0][0], a[0][1]), a[1][1]/a[1][0], a[1][2], a[1][3],
                               np.sqrt(-(a[1][1]/a[1][0])**2+a[1][4]/a[1][0])))

data_month = np.zeros((9, 12))
for row in res.collect():
	data_month[row[0][0]-2001, row[0][1]-1] = row[1]

# Season:
data_season = []

res_map_season = rows_paral.map(lambda a: (((a[1])//3),
                                             np.array([1, a[5], a[5], a[5], a[5]**2])))
res_map_season = res_map_season.map(lambda a: ((a[0]+4), a[1]) if a[0] == 0 else ((a[0]), a[1]))
res_redu_season = res_map_season.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], 
                                                           min(a[2], b[2]), max(a[3], b[3]), a[4]+b[4]))
res_season = res_redu_season.map(lambda a: ((a[0]), a[1][1]/a[1][0], a[1][2], a[1][3], 
                                             np.sqrt(-(a[1][1]/a[1][0])**2+a[1][4]/a[1][0])))

for row in res_season.collect():
	data_season.append(row)
data_season = np.array(data_season)

#data_season[:, 0] += 1

# Visualization
fig = plt.figure()
gridspec.GridSpec(4,3)
fig.set_size_inches(18.5, 10.5)
fig.suptitle("Variantion des mesures d'indicateur {} de station {} ".format(opt.indicator, opt.station_id), fontsize=22)

plt.subplot2grid((4,3), (0,0), colspan=2, rowspan=4)
plt.title("Variation de moyen par rapport au mois")
palette = plt.get_cmap('Set1')
for i in range(9):
	plt.plot(range(1, 13), data_month[i, :], color=palette(i+1), label = "200"+str(i+1) )
plt.legend(loc=2, ncol=2)
plt.xlabel('Mois', fontsize=16)
plt.ylabel(opt.indicator, fontsize=20)
plt.xticks(np.arange(1, 13))

plt.subplot2grid((4,3), (0,2))
plt.title("Variation par rapport a la saison")
plt.plot(data_season[:, 0], data_season[:, 1])
plt.xticks(np.arange(1, 5), ('Printemps', 'Ete','Automne','Hiver'))
plt.legend(['Moyen'], loc='upper left')

plt.subplot2grid((4,3), (1,2))
plt.plot(data_season[:, 0], data_season[:, 2])
plt.xticks(np.arange(1, 5), ('Printemps', 'Ete','Automne','Hiver'))
plt.legend(['Min'], loc='upper left')

plt.subplot2grid((4,3), (2,2))
plt.plot(data_season[:, 0], data_season[:, 3])
plt.xticks(np.arange(1, 5), ('Printemps', 'Ete','Automne','Hiver'))
plt.legend(['Max'], loc='upper left')

plt.subplot2grid((4,3), (3,2))
plt.plot(data_season[:, 0], data_season[:, 4])
plt.xticks(np.arange(1, 5), ('Printemps', 'Ete','Automne','Hiver'))
plt.legend(['Ecart type'], loc='upper left')

plt.show()
plt.savefig("./{}_{}.png".format(opt.station_id, opt.indicator))
plt.close(fig)


