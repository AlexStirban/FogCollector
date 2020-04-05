import requests
import shutil
import datetime


# Utility function used to download large files
def download_file(url: str, args: dict, name: str):
    with requests.get(url=url, params=args, stream=True) as r:
        try:
            with open(f'{name}', 'wb') as f:
                r.raise_for_status()
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)

        except requests.exceptions.RequestException as err:
            print('Unexpected error:', err)
        except requests.exceptions.HTTPError as errh:
            print('HTTP Error:', errh)
        except requests.exceptions.ConnectionError as errc:
            print('Error connecting:', errc)
        except requests.exceptions.Timeout as errt:
            print('Timeout error:', errt)


# Function used to download a range of METARS from the IEM database
# IEM database: https://mesonet.agron.iastate.edu/
def download_IEM_METARS(icao: list, start: datetime.datetime, end: datetime.datetime, name):
    source = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?'

    args = dict(station=icao, data='metar', year1=start.year, month1=start.month, day1=start.day, year2=end.year,
                month2=end.month, day2=end.day, tz='Etc/UTC', format='onlytdf', latlon='no', missing='M', direct='yes',
                report_type=2)

    download_file(source, args, name)
