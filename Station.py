import csv
import pathlib

from Utils import download_file


class station:
    def __init__(self, icao=None, country=None, lat=None, lon=None, alt=None):
        self.icao = icao
        self.country = country
        self.lat = lat
        self.lon = lon
        self.alt = alt


# OpenFlights database url
database_url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'
database_path = database_path = pathlib.Path(__file__).resolve().parent / pathlib.PurePath('airports.dat')

# download database if non-present in the system
if not database_path.exists():
    print(f'Database not present, downloading a copy from: {database_url}. Thanks to OpenFlights!')
    download_file(database_url, None, str(database_path))


# Parses and filters stations from the database
# ARGS:
#   - flt: a function that acts as a filter and returns a bool type
# RET:
#   - an array of station objects
def parse_stations(flt=lambda e: True):
    stations = []

    with open(database_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')

        for row in reader:
            # express csv as a station object an convert alt to meters
            element = station(icao=row[5], country=row[3].lower(), lat=float(row[6]), lon=float(row[7]), alt=float(row[8]) * 0.3048)

            # check if element agrees with our filter
            if flt(element):
                stations.append(element)

        return stations
