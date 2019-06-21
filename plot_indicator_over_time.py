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
                     help= 'id de la station')
parser.add_argument('--indicateur', type = str, default = 'tmpf', 
                     help= 'indicateur pour visualiser')
opt = parser.parse_args()
print(opt)

# initialize a sparkcontext
sc = SparkContext.getOrCreate()

# Connection avec cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('agaltier_zkang_projet')

def get_historique(station, indicateur):
    requete = """SELECT year, month, day, hour, minute, {} 
                 FROM agaltier_zkang_metar_France_1 where station_id = '{}'
                 LIMIT 100000;
              """.format(indicateur, station)
    return session.execute(requete)



# Recuperer donnes and creer une liste
rows = get_historique(opt.station_id, opt.indicateur)

# Utiliser spark pour parallelization
rows_paral = sc.parallelize(rows)

# exemple de row: ((2001, 5), (336.0, 11282.999938964844, 6.800000190734863, 46.400001525878906, 390615.9551180268))
rows_paral = rows_paral.filter(lambda a: a[5] is not None)
res_map = rows_paral.map(lambda a: ((a[0], a[1]), np.array([1, a[5], a[5], a[5], a[5]**2])))
res_redu = res_map.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], min(a[2], b[2]),
                                             max(a[3], b[3]), a[4]+b[4]))
res = res_redu.map(lambda a: ((a[0][0], a[0][1]), a[1][1]/a[1][0], a[1][2], a[1][3],
                               np.sqrt(-(a[1][1]/a[1][0])**2+a[1][4]/a[1][0])))

data_month = np.zeros((9, 12))
for row in res.collect():
	data_month[row[0][0]-2001, row[0][1]-1] = row[1]

# Saison:
data_saison = []

res_map_saison = rows_paral.map(lambda a: (((a[1]-1)//3),
                                             np.array([1, a[5], a[5], a[5], a[5]**2])))
res_redu_saison = res_map_saison.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], 
                                                           min(a[2], b[2]), max(a[3], b[3]), a[4]+b[4]))
res_saison = res_redu_saison.map(lambda a: ((a[0]), a[1][1]/a[1][0], a[1][2], a[1][3], 
                                             np.sqrt(-(a[1][1]/a[1][0])**2+a[1][4]/a[1][0])))

for row in res_saison.collect():
	data_saison.append(row)
data_saison = np.array(data_saison)

# transformer (0, 1, 2, 3) en (1, 2, 3, 4)
data_saison[:, 0] += 1

fig = plt.figure()
gridspec.GridSpec(4,3)
fig.set_size_inches(18.5, 10.5)
fig.suptitle("Variantion des stats d'indicateur {} de station {} ".format(opt.indicateur, opt.station_id), fontsize=22)

plt.subplot2grid((4,3), (0,0), colspan=2, rowspan=4)
plt.title("Variation de moyen par rapport au mois")
palette = plt.get_cmap('Set1')
for i in range(9):
	plt.plot(range(1, 13), data_month[i, :], color=palette(i+1), label = "200"+str(i+1) )
plt.legend(loc=2, ncol=2)
plt.xticks(np.arange(1, 13))


plt.subplot2grid((4,3), (0,2))
plt.title("Variation par rapport a la saison")
plt.plot(data_saison[:, 0], data_saison[:, 1])
plt.xticks(np.arange(1, 5), ('Saison 1', 'Saison 2','Saison 3','Saison 4',))
plt.legend(['Moyen'], loc='upper left')

plt.subplot2grid((4,3), (1,2))
plt.plot(data_saison[:, 0], data_saison[:, 2])
plt.xticks(np.arange(1, 5), ('Saison 1', 'Saison 2','Saison 3','Saison 4',))
plt.legend(['Min'], loc='upper left')

plt.subplot2grid((4,3), (2,2))
plt.plot(data_saison[:, 0], data_saison[:, 3])
plt.xticks(np.arange(1, 5), ('Saison 1', 'Saison 2','Saison 3','Saison 4',))
plt.legend(['Max'], loc='upper left')

plt.subplot2grid((4,3), (3,2))
plt.plot(data_saison[:, 0], data_saison[:, 4])
plt.xticks(np.arange(1, 5), ('Saison 1', 'Saison 2','Saison 3','Saison 4',))
plt.legend(['Ecart type'], loc='upper left')

plt.show()
plt.savefig("./{}_{}.png".format(opt.station_id, opt.indicateur))
plt.close(fig)


