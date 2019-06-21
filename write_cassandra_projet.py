from cassandra.cluster import Cluster
from load_data_projet import loadata

cluster = Cluster(['localhost'])
session = cluster.connect('agaltier_zkang_projet')
csvfilename = './asos.txt'

def limit_gen(g, limit):
    for i, d in enumerate(g):
        if i>= limit:
            return None
        yield d

create_query_1 = '''CREATE TABLE agaltier_zkang_metar_France_1(

                station_id text,
                latitude float,
                longitude float,

                year varint,
                month varint,
                day varint,
                hour varint,
                minute varint,

                tmpf float,
                dwpf float,
                relh float,
                drct float,
                sknt float,
                alti float,
                vsby float,
                gust float,

                skyc1 text,
                skyc2 text,
                skyc3 text,
                skyc4 text,

                skyl1 float,
                skyl2 float,
                skyl3 float,
                skyl4 float,

                wxcodes text,

                feel float,

                metar text,


                PRIMARY KEY ((station_id), year, month, day, hour, minute)
                );'''

create_query_2 = '''CREATE TABLE agaltier_zkang_metar_France_2(

                station_id text,
                latitude float,
                longitude float,

                year varint,
                month varint,
                day varint,
                hour varint,
                minute varint,

                tmpf float,
                dwpf float,
                relh float,
                drct float,
                sknt float,
                alti float,
                vsby float,
                gust float,

                skyc1 text,
                skyc2 text,
                skyc3 text,
                skyc4 text,

                skyl1 float,
                skyl2 float,
                skyl3 float,
                skyl4 float,

                wxcodes text,

                feel float,

                metar text,


                PRIMARY KEY ((year, month, day, hour), latitude, longitude)
                );'''


create_query_3 = '''CREATE TABLE agaltier_zkang_metar_France_3(

                station_id text,
                latitude float,
                longitude float,

                year varint,
                month varint,
                day varint,
                hour varint,
                minute varint,

                tmpf float,
                dwpf float,
                relh float,
                drct float,
                sknt float,
                alti float,
                vsby float,
                gust float,

                skyc1 text,
                skyc2 text,
                skyc3 text,
                skyc4 text,

                skyl1 float,
                skyl2 float,
                skyl3 float,
                skyl4 float,

                wxcodes text,

                feel float,

                metar text,


                PRIMARY KEY ((year), month, day, hour, minute)
                );'''

def writecassandra_1(csvfilename, session):
    # data = limit_gen(loadata(csvfilename),  1000)
    data = loadata(csvfilename)
    i = 0
    for r in data:
        if i % 100000 == 0:
            print(i)
        i += 1
        t = (
                    r["station"],
                    r['lat'],
                    r['lon'],

                    r["timestamp"][0],
                    r["timestamp"][1],
                    r["timestamp"][2],
                    r["timestamp"][3],
                    r["timestamp"][4],

                    r["tmpf"],
                    r["dwpf"],
                    r["relh"],
                    r["drct"],
                    r["sknt"],
                    r["alti"],
                    r["vsby"],
                    r["gust"],

                    r["skyc1"],
                    r["skyc2"],
                    r["skyc3"],
                    r["skyc4"],

                    r["skyl1"],
                    r["skyl2"],
                    r["skyl3"],
                    r["skyl4"],

                    r["wxcodes"],

                    r["feel"],

                    r["metar"]

                    )
        query = """
        INSERT INTO agaltier_zkang_metar_France_1(

            station_id,
            latitude,
            longitude,

            year,
            month,
            day,
            hour,
            minute,

            tmpf,
            dwpf,
            relh,
            drct,
            sknt,
            alti,
            vsby,
            gust,

            skyc1,
            skyc2,
            skyc3,
            skyc4,

            skyl1,
            skyl2,
            skyl3,
            skyl4,

            wxcodes,

            feel,

            metar

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, t)

def writecassandra_2(csvfilename, session):
    # data = limit_gen(loadata(csvfilename),  1000)
    data = loadata(csvfilename)
    i = 0
    for r in data:
        if i % 100000 == 0:
            print(i)
        i += 1
        t = (
                    r["station"],
                    r['lat'],
                    r['lon'],

                    r["timestamp"][0],
                    r["timestamp"][1],
                    r["timestamp"][2],
                    r["timestamp"][3],
                    r["timestamp"][4],

                    r["tmpf"],
                    r["dwpf"],
                    r["relh"],
                    r["drct"],
                    r["sknt"],
                    r["alti"],
                    r["vsby"],
                    r["gust"],

                    r["skyc1"],
                    r["skyc2"],
                    r["skyc3"],
                    r["skyc4"],

                    r["skyl1"],
                    r["skyl2"],
                    r["skyl3"],
                    r["skyl4"],

                    r["wxcodes"],

                    r["feel"],

                    r["metar"]

                    )
        query = """
        INSERT INTO agaltier_zkang_metar_France_2(

            station_id,
            latitude,
            longitude,

            year,
            month,
            day,
            hour,
            minute,

            tmpf,
            dwpf,
            relh,
            drct,
            sknt,
            alti,
            vsby,
            gust,

            skyc1,
            skyc2,
            skyc3,
            skyc4,

            skyl1,
            skyl2,
            skyl3,
            skyl4,

            wxcodes,

            feel,

            metar

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, t)

def writecassandra_3(csvfilename, session):
    # data = limit_gen(loadata(csvfilename),  1000)
    data = loadata(csvfilename)
    i = 0
    for r in data:
        if i % 100000 == 0:
            print(i)
        i += 1
        t = (
                    r["station"],
                    r['lat'],
                    r['lon'],

                    r["timestamp"][0],
                    r["timestamp"][1],
                    r["timestamp"][2],
                    r["timestamp"][3],
                    r["timestamp"][4],

                    r["tmpf"],
                    r["dwpf"],
                    r["relh"],
                    r["drct"],
                    r["sknt"],
                    r["alti"],
                    r["vsby"],
                    r["gust"],

                    r["skyc1"],
                    r["skyc2"],
                    r["skyc3"],
                    r["skyc4"],

                    r["skyl1"],
                    r["skyl2"],
                    r["skyl3"],
                    r["skyl4"],

                    r["wxcodes"],

                    r["feel"],

                    r["metar"]

                    )
        query = """
        INSERT INTO agaltier_zkang_metar_France_3(

            station_id,
            latitude,
            longitude,

            year,
            month,
            day,
            hour,
            minute,

            tmpf,
            dwpf,
            relh,
            drct,
            sknt,
            alti,
            vsby,
            gust,

            skyc1,
            skyc2,
            skyc3,
            skyc4,

            skyl1,
            skyl2,
            skyl3,
            skyl4,

            wxcodes,

            feel,

            metar

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, t)

# session.execute('DROP TABLE IF EXISTS agaltier_zkang_metar_France_1')
# session.execute(create_query_1)
# writecassandra_1(csvfilename, session)

session.execute('DROP TABLE IF EXISTS agaltier_zkang_metar_France_2')
session.execute(create_query_2)
writecassandra_2(csvfilename, session)
#
# session.execute('DROP TABLE IF EXISTS agaltier_zkang_metar_France_3')
# session.execute(create_query_3)
# writecassandra_3(csvfilename, session)
