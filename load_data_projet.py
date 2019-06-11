import csv
import re

def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match_valid = dateparser.match(r["valid"])
            # On ne conserve que les lignes pour lesquelles on connait le timsestamp et les coordonnees geographique de la station
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

            # Les valeurs nulles numeriques sont representees par -999 et les valeurs nulles textuelles par le string vide
            data["tmpf"] = float(r["tmpf"]) if r["tmpf"]!= 'M' else -999
            data["dwpf"] = float(r["dwpf"]) if r["dwpf"]!= 'M' else -999
            data["relh"] = float(r["relh"]) if r["relh"]!= 'M' else -999
            data["drct"] = float(r["drct"]) if r["drct"]!= 'M' else -999
            data["sknt"] = float(r["sknt"]) if r["sknt"]!= 'M' else -999
            data["p01i"] = float(r["p01i"]) if r["p01i"]!= 'M' else -999
            data["alti"] = float(r["alti"]) if r["alti"]!= 'M' else -999
            data["mslp"] = float(r["mslp"]) if r["mslp"]!= 'M' else -999
            data["vsby"] = float(r["vsby"]) if r["vsby"]!= 'M' else -999
            data["gust"] = float(r["gust"]) if r["gust"]!= 'M' else -999

            data["skyc1"] = r["skyc1"] if r["skyc1"]!= 'M' else ''
            data["skyc2"] = r["skyc2"] if r["skyc2"]!= 'M' else ''
            data["skyc3"] = r["skyc3"] if r["skyc3"]!= 'M' else ''
            data["skyc4"] = r["skyc4"] if r["skyc4"]!= 'M' else ''

            data["skyl1"] = float(r["skyl1"]) if r["skyl1"]!= 'M' else -999
            data["skyl2"] = float(r["skyl2"]) if r["skyl2"]!= 'M' else -999
            data["skyl3"] = float(r["skyl3"]) if r["skyl3"]!= 'M' else -999
            data["skyl4"] = float(r["skyl4"]) if r["skyl4"]!= 'M' else -999

            data["wxcodes"] = r["wxcodes"] if r["wxcodes"]!= 'M' else ''

            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"]) if r["ice_accretion_1hr"]!= 'M' else -999
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"]) if r["ice_accretion_3hr"]!= 'M' else -999
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"]) if r["ice_accretion_6hr"]!= 'M' else -999

            data["peak_wind_gust"] = float(r["peak_wind_gust"]) if r["peak_wind_gust"]!= 'M' else -999
            data["peak_wind_drct"] = float(r["peak_wind_drct"]) if r["peak_wind_drct"]!= 'M' else -999
            data["peak_wind_time"] = r["peak_wind_time"] if r["peak_wind_time"]!= 'M' else ''

            data["feel"] = float(r["feel"]) if r["feel"]!= 'M' else -999

            data["metar"] = r["metar"] if r["metar"]!= 'M' else ''

            yield data
