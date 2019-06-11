import csv
import re


def data_file_generator(data_path):
    with open(data_path, 'r') as f: 
        reader = csv.DictReader(f, delimiter = ',')
        for r in reader:
            yield dict(r)

            
            
# g = data_file_generator('./asos.txt')
# flag = True
# i = 0
# print("Start...")

"""
# Verifier si un champ est toujours null.
# 
# Resultat:
# p01i == M
# mslp == M
# ice_accretion_1hr == M
# ice_accretion_3hr == M
# ice_accretion_6hr == M
# peak_wind_gust == M
# peak_wind_drct == M
# peak_wind_time == M

while flag:
    if i % 100000 == 0:
        print(i)
    a = next(g)
    i = i+1
    if a['peak_wind_gust'] != 'M':
        print(str(a['peak_wind_gust']))
        print('peak_wind_gust')
        break
    if a['peak_wind_drct'] != 'M':
        print(str(a['peak_wind_drct']))
        print('peak_wind_drct')
        break
    if a['peak_wind_time'] != 'M':
        print(str(a['peak_wind_time']))
        print('peak_wind_time')
        break
"""

def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            
            match_valid = dateparser.match(r["valid"])
            if not match_valid or (r["lon"] == 'M') or (r["lat"] == 'M'):
                        continue
            time = match_valid.groupdict()
            data = {}
            data["station"] = r["station"]
            data["timestamp"] = (
                int(time["year"]),
                int(time["month"]),
                int(time["day"]),
                int(time["hour"]),
                int(time["minute"])
            )
            data["lon"] = float(r["lon"])
            data["lat"] = float(r["lat"])

            if r["tmpf"]!= 'M': data["tmpf"] = float(r["tmpf"])
            if r["dwpf"]!= 'M': data["dwpf"] = float(r["dwpf"])
            if r["relh"]!= 'M': data["relh"] = float(r["relh"])
            if r["drct"]!= 'M': data["drct"] = float(r["drct"])
            if r["sknt"]!= 'M': data["sknt"] = float(r["sknt"])
            if r["p01i"]!= 'M': data["p01i"] = float(r["p01i"])
            if r["alti"]!= 'M': data["alti"] = float(r["alti"])
            if r["mslp"]!= 'M': data["mslp"] = float(r["mslp"])
            if r["vsby"]!= 'M': data["vsby"] = float(r["vsby"])
            if r["gust"]!= 'M': data["gust"] = float(r["gust"])

            if r["skyc1"]!= 'M': data["skyc1"] = r["skyc1"]
            if r["skyc2"]!= 'M': data["skyc2"] = r["skyc2"]
            if r["skyc3"]!= 'M': data["skyc3"] = r["skyc3"]
            if r["skyc4"]!= 'M': data["skyc4"] = r["skyc4"]

            if r["skyl1"]!= 'M': data["skyl1"] = float(r["skyl1"])
            if r["skyl2"]!= 'M': data["skyl2"] = float(r["skyl2"])
            if r["skyl3"]!= 'M': data["skyl3"] = float(r["skyl3"])
            if r["skyl4"]!= 'M': data["skyl4"] = float(r["skyl4"])

            if r["wxcodes"]!= 'M': data["wxcodes"] = r["wxcodes"]

            if r["ice_accretion_1hr"]!= 'M': data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"])
            if r["ice_accretion_3hr"]!= 'M': data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"])
            if r["ice_accretion_6hr"]!= 'M': data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"])

            if r["peak_wind_gust"]!= 'M': data["peak_wind_gust"] = float(r["peak_wind_gust"])
            if r["peak_wind_drct"]!= 'M': data["peak_wind_drct"] = float(r["peak_wind_drct"])
            if r["peak_wind_time"]!= 'M': data["peak_wind_time"] = r["peak_wind_time"]

            if r["feel"]!= 'M': data["feel"] = float(r["feel"])

            if r["metar"]!= 'M': data["metar"] = r["metar"]

            yield data