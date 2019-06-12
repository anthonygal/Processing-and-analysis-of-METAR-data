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

            # Les valeurs nulles numeriques sont representees par None et les valeurs nulles textuelles par le string vide
            data["tmpf"] = float(r["tmpf"]) if r["tmpf"]!= 'M' else None
            data["dwpf"] = float(r["dwpf"]) if r["dwpf"]!= 'M' else None
            data["relh"] = float(r["relh"]) if r["relh"]!= 'M' else None
            data["drct"] = float(r["drct"]) if r["drct"]!= 'M' else None
            data["sknt"] = float(r["sknt"]) if r["sknt"]!= 'M' else None
            data["p01i"] = float(r["p01i"]) if r["p01i"]!= 'M' else None
            data["alti"] = float(r["alti"]) if r["alti"]!= 'M' else None
            data["mslp"] = float(r["mslp"]) if r["mslp"]!= 'M' else None
            data["vsby"] = float(r["vsby"]) if r["vsby"]!= 'M' else None
            data["gust"] = float(r["gust"]) if r["gust"]!= 'M' else None

            data["skyc1"] = r["skyc1"] if r["skyc1"]!= 'M' else None
            data["skyc2"] = r["skyc2"] if r["skyc2"]!= 'M' else None
            data["skyc3"] = r["skyc3"] if r["skyc3"]!= 'M' else None
            data["skyc4"] = r["skyc4"] if r["skyc4"]!= 'M' else None

            data["skyl1"] = float(r["skyl1"]) if r["skyl1"]!= 'M' else None
            data["skyl2"] = float(r["skyl2"]) if r["skyl2"]!= 'M' else None
            data["skyl3"] = float(r["skyl3"]) if r["skyl3"]!= 'M' else None
            data["skyl4"] = float(r["skyl4"]) if r["skyl4"]!= 'M' else None

            data["wxcodes"] = r["wxcodes"] if r["wxcodes"]!= 'M' else None

            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"]) if r["ice_accretion_1hr"]!= 'M' else None
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"]) if r["ice_accretion_3hr"]!= 'M' else None
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"]) if r["ice_accretion_6hr"]!= 'M' else None

            data["peak_wind_gust"] = float(r["peak_wind_gust"]) if r["peak_wind_gust"]!= 'M' else None
            data["peak_wind_drct"] = float(r["peak_wind_drct"]) if r["peak_wind_drct"]!= 'M' else None
            data["peak_wind_time"] = r["peak_wind_time"] if r["peak_wind_time"]!= 'M' else None

            data["feel"] = float(r["feel"]) if r["feel"]!= 'M' else None

            data["metar"] = r["metar"] if r["metar"]!= 'M' else None

            yield data
