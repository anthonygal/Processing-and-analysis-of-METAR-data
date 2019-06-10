import csv
def data_file_generator(data_path):
    with open(data_path, 'r') as f: 
        reader = csv.DictReader(f, delimiter = ',')
        for r in reader:
            yield dict(r)

g = data_file_generator('./asos.txt')
flag = True
i = 0
print("Start...")

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
            if not match_start or not match_stop:
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

            data["tmpf"] = float(r["tmpf"])
            data["dwpf"] = float(r["dwpf"])
            data["relh"] = float(r["relh"])
            data["drct"] = float(r["drct"])
            data["sknt"] = float(r["sknt"])
            data["p01i"] = float(r["p01i"])
            data["alti"] = float(r["alti"])
            data["mslp"] = float(r["mslp"])
            data["vsby"] = float(r["vsby"])
            data["gust"] = float(r["gust"])

            data["skyc1"] = r["skyc1"]
            data["skyc2"] = r["skyc2"]
            data["skyc3"] = r["skyc3"]
            data["skyc4"] = r["skyc4"]

            data["skyl1"] = float(r["skyl1"])
            data["skyl2"] = float(r["skyl2"])
            data["skyl3"] = float(r["skyl3"])
            data["skyl4"] = float(r["skyl4"])

            data["wxcodes"] = r["wxcodes"]

            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"])
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"])
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"])

            data["peak_wind_gust"] = float(r["peak_wind_gust"])
            data["peak_wind_drct"] = float(r["peak_wind_drct"])
            data["peak_wind_time"] = r["peak_wind_time"]

            data["feel"] = float(r["peak_wind_gust"])

            data["metar"] = r["metar"]

            yield data
