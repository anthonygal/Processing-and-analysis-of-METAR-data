from cassandra.cluster import Cluster
import matplotlib.pyplot as plt 
from pyspark import  SparkContext

# initialize a sparkcontext
sc = SparkContext.getOrCreate()

# Connection avec cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('agaltier_zkang_projet')

def get_historique(station, indicateur):
    query = """SELECT {} FROM agaltier_zkang_metar_France_1 where station_id = '{}';""".format(indicateur, station)
    query = """SELECT * FROM agaltier_zkang_metar_France_1"""
    #print(query)
    return session.execute(query)

# test of parallelization
res = sc.parallelize(get_historique("LFST", "alti"))
print(res.count())
