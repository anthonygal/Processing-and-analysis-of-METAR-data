from cassandra.cluster import Cluster
import matplotlib.pyplot as plt 
from pyspark import  SparkContext

import argparse

# initialize a sparkcontext
sc = SparkContext.getOrCreate()

# Connection avec cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('agaltier_zkang_projet')

def get_historique(station, indicateur):
    requete = """SELECT {} FROM agaltier_zkang_metar_France_1 where station_id = '{} LIMIT 10';""".format(indicateur, station)
    
    return session.execute(requete)

parser = argparse.ArgumentParser()
parser.add_argument('--station_id', type = str, default = 'LFST', help= 'id de la station')
parser.add_argument('--indicateur', type = str, default = 'tmpf', help= 'indicateur pour visualiser')
opt = parser.parse_args()
print(opt)

# Recuperer donnes de la base 
res_paral = sc.parallelize(get_historique(opt.station_id, opt.indicateur))

print(res_paral)