import re
import datetime

RE_ICAO = re.compile(r'(?P<icao>\b[A-Z]{4}\b)')
RE_DH = re.compile(r'(?P<day>\d\d)(?P<hour>\d\d)(?P<min>\d\d)Z')
RE_VIS = re.compile(r'(?P<vis>\b\d{4}\b|CAVOK)')
RE_WIND = re.compile(r'(?P<dir>\d{3}|VRB)(?P<speed>\d{2})(?P<units>KT|MPS)')
RE_TEMP_DEW = re.compile(r'(?P<temp>M?\d\d)/(?P<dew>M?\d\d)')


# Minimal implementation of a METAR parsing class,
# it is designed to work only with some of the values that are used
# in this project, it's not intended for general use cases

class MinMetar:
    def __init__(self, raw_metar, year, month):
        self.raw = raw_metar

        self.month = month
        self.year = year
        self.vis = None
        self.w_speed = None
        self.w_dir = None
        self.temp = None
        self.date = None
        self.icao = None
        self.missing = False

        for expr, parser in self.parsers:
            search = expr.search(self.raw)

            # check if data could be parsed
            if search:
                parser(self, search.groupdict())
            else:
                self.missing = True

    def _parse_icao(self, s):
        self.icao = s['icao']

    def _parse_dh(self, s):
        self.date = datetime.datetime(self.year, self.month, int(s['day']), int(s['hour']), int(s['min']))

    def _parse_vis(self, s):
        self.vis = int(s['vis'].replace('CAVOK', '9999'))

    def _parse_wind(self, s):
        self.w_dir = float(s['dir'].replace('VRB', 'nan'))
        self.w_speed = float(s['speed'])

        if s['units'] == 'KT':
            self.w_speed *= 0.514444

    def _parse_temp_dew(self, s):
        self.temp = int(s['temp'].replace('M', '-'))

    parsers = [
        (RE_ICAO, _parse_icao),
        (RE_DH, _parse_dh),
        (RE_VIS, _parse_vis),
        (RE_WIND, _parse_wind),
        (RE_TEMP_DEW, _parse_temp_dew)
    ]
