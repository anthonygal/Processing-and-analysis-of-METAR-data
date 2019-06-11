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
                p01i float,
                alti float,
                mslp float,
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

                ice_accretion_1hr float,
                ice_accretion_3hr float,
                ice_accretion_6hr float,

                peak_wind_gust float,
                peak_wind_drct float,
                peak_wind_time text,

                feel float,

                metar text,


                PRIMARY KEY ((latitude, longitude), year, month, hour, minute)
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
                p01i float,
                alti float,
                mslp float,
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

                ice_accretion_1hr float,
                ice_accretion_3hr float,
                ice_accretion_6hr float,

                peak_wind_gust float,
                peak_wind_drct float,
                peak_wind_time text,

                feel float,

                metar text,


                PRIMARY KEY ((year, month, hour, minute), latitude, longitude)
                );'''

def writecassandra_1(csvfilename, session):
    data = limit_gen(loadata(csvfilename),  100000)
    # data = loadata(csvfilename)
    for r in data:
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
                    r["p01i"],
                    r["alti"],
                    r["mslp"],
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

                    r["ice_accretion_1hr"],
                    r["ice_accretion_3hr"],
                    r["ice_accretion_6hr"],

                    r["peak_wind_gust"],
                    r["peak_wind_drct"],
                    r["peak_wind_time"],

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
            p01i,
            alti,
            mslp,
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

            ice_accretion_1hr,
            ice_accretion_3hr,
            ice_accretion_6hr,

            peak_wind_gust,
            peak_wind_drct,
            peak_wind_time,

            feel,

            metar

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, t)

def writecassandra_2(csvfilename, session):
    data = limit_gen(loadata(csvfilename),  100000)
    # data = loadata(csvfilename)
    for r in data:
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
                    r["p01i"],
                    r["alti"],
                    r["mslp"],
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

                    r["ice_accretion_1hr"],
                    r["ice_accretion_3hr"],
                    r["ice_accretion_6hr"],

                    r["peak_wind_gust"],
                    r["peak_wind_drct"],
                    r["peak_wind_time"],

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
            p01i,
            alti,
            mslp,
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

            ice_accretion_1hr,
            ice_accretion_3hr,
            ice_accretion_6hr,

            peak_wind_gust,
            peak_wind_drct,
            peak_wind_time,

            feel,

            metar

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, t)


# session.execute('DROP TABLE agaltier_zkang_metar_France_1')
# session.execute(create_query_1)
# writecassandra_1(csvfilename, session)

# session.execute('DROP TABLE agaltier_zkang_metar_France_2')
# session.execute(create_query_2)
# writecassandra_2(csvfilename, session)
