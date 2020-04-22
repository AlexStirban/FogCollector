import xarray as xr
import pandas as pd
import pathlib
import datetime
import netCDF4
import argparse
import csv
import warnings
import multiprocessing
import os
import sys

from tqdm import tqdm
from Station import parse_stations
from Metar import MinMetar
from Utils import download_IEM_METARS


# auxiliar function used to unpack args for imap
def unpack_args(args):
    download_IEM_METARS(*args)


# Function used to download all the available METAR entries in a range of time
def download_country_METARS(countries, start, end, verbose=False):
    # Set and make a new folder where the METAR data will be saved
    metars_path = pathlib.Path(__file__).resolve().parent / pathlib.PurePath('temp_metars')
    metars_path.mkdir(parents=True, exist_ok=True)

    # Save parsed stations for later
    stat_dict = dict()
    args = []

    # prepare arrays
    for country in countries:
        stations = parse_stations(lambda e: e.country == country)
        icaos = [station.icao for station in stations]

        # build args for thread pool
        args.append((icaos, start, end, str(metars_path / pathlib.PurePath(country))))

        # Update our dict
        stat_dict.update(dict(zip(icaos, stations)))

    # download metar files in that path
    with multiprocessing.Pool(processes=15) as pool:
        for _ in tqdm(pool.imap_unordered(unpack_args, args), total=len(args),
                      disable=not verbose, desc="Downloading METARS"):
            pass

    return stat_dict


# Function used to process the downloaded METARS into netCDF4 files
def process_metars(start: datetime.datetime, end: datetime.datetime, dt: datetime.timedelta,
                   icao_dict=None, verbose=False):
    # get raw metars files
    metars_path = (pathlib.Path(__file__).resolve().parent / pathlib.PurePath('temp_metars')).glob('**/*')
    filenames = [x for x in metars_path if x.is_file()]

    # make a path for the generated netcdfs
    netcdfs_path = pathlib.Path(__file__).resolve().parent / pathlib.PurePath('netCDF4')
    netcdfs_path.mkdir(parents=True, exist_ok=True)
    mid = start

    # progress bar
    pbar = tqdm(total=(end - start).total_seconds() / dt.total_seconds(),
                desc=start.strftime("Processing %Y-%m-%d %H:%M:%SZ"),
                disable=not verbose, file=sys.stdout)

    while mid < end:
        df = pd.DataFrame()

        for filename in filenames:
            with open(filename, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter='\t')

                # skip the headers line
                next(reader)
                for row in reader:
                    date = datetime.datetime.strptime(row[1], '%Y-%m-%d %H:%M')
                    metar = MinMetar(row[2], date.year, date.month)

                    if date > mid:
                        break

                    # add only if all the needed data is parsed
                    if date == mid and not metar.missing:
                        if icao_dict is None:
                            geo = parse_stations(lambda e: e.icao == metar.icao)[0]
                        else:
                            geo = icao_dict[metar.icao]

                        df = df.append(dict(time=metar.date, lat=geo.lat, lon=geo.lon, zm=metar.vis,
                                            alt=geo.alt, dd=metar.w_dir, ff=metar.w_speed, temp=metar.temp,
                                            fog=metar.vis <= 1000, station_name=metar.icao), ignore_index=True)

        # if there are any elements, write to netcdf
        if len(df.index):
            path = netcdfs_path / pathlib.PurePath(mid.strftime("%Y%m%dT%H%M%SZ.nc"))

            df.set_index(['time', 'station_name'], inplace=True)
            df_to_netCDF4(df, str(path))

        if verbose:
            pbar.set_description(mid.strftime("Processing %Y-%m-%d %H:%M:%SZ"))
            pbar.update(1)

        mid += dt

    #Cleanup temp METARS
    if verbose:
        pbar.close()
        print('\nCleaning temp METARS...')
        pass

    for f in filenames:
        os.remove(f)


# xarray overwrites some attributes, going agains CF convention
# this function is used to manually modify those attributes
def fix_attr_to_CF(file):
    file = netCDF4.Dataset(file, 'r+')
    to_add = ['zm', 'temp', 'station_name', 'fog', 'ff', 'dd']
    to_remove = ['alt', 'lat', 'lon']

    if file.isopen():
        for var_name in to_add:
            var = file.variables[var_name]
            var.coordinates = 'time lat lon alt station_name'

        for var_name in to_remove:
            var = file.variables[var_name]
            del var.coordinates

    file.close()


# function used to write the dataframe to a netCDF4 file using xarray
def df_to_netCDF4(df: pd.DataFrame, name: str):
    ds = xr.Dataset.from_dataframe(df)
    ds = ds.rename_dims(station_name='station')

    # geo vars
    ds['time'].attrs = dict(standard_name='time', long_name='Time Of Measurement')
    ds['lat'].attrs = dict(long_name='Airport Latitude', standard_name='latitude', units='degrees_north')
    ds['lon'].attrs = dict(long_name='Airport Longitude', standard_name='longitude', units='degrees_east')
    ds['alt'].attrs = dict(long_name='Airport Altitude', standard_name='altitude', units='m')
    ds['station_name'].attrs = dict(long_name='Airport_ICAO_code', cf_role='timeseries_id')

    # meteo vars
    ds['dd'].attrs = dict(long_name='Wind Direction 10 Min Average', standard_name='wind_from_direction',
                          units='degree')
    ds['zm'].attrs = dict(long_name='Visibility', standard_name='visibility_in_air', units='m')
    ds['ff'].attrs = dict(long_name='Wind Speed 10 Min Average', standard_name='wind_speed', units='m s-1')
    ds['temp'].attrs = dict(long_name='Air Temperature', standard_name='air_temperature', units='degrees Celsius')
    ds['fog'].attrs = dict(long_name='Presence of fog', units='1')

    # projection var
    ds = ds.assign(projection=0)
    ds['projection'].attrs = dict(long_name='Projection definition', EPSG_code='EPSG:4326')

    # enconding
    enc = {'station_name': {'dtype': 'unicode'},
           'lat': {'dtype': 'float32', '_FillValue': None},
           'lon': {'dtype': 'float32', '_FillValue': None},
           'alt': {'dtype': 'float32', '_FillValue': None},
           'dd': {'dtype': 'float32', '_FillValue': None},
           'ff': {'dtype': 'float32', '_FillValue': None},
           'temp': {'dtype': 'float32', '_FillValue': None},
           'zm': {'dtype': 'int32', '_FillValue': None},
           'fog': {'dtype': 'bool_', '_FillValue': None},
           'time': {'units': 'seconds since 1970-01-01', 'calendar': 'standard'}}

    # global attributes
    ds.attrs['Conventions'] = 'CF-1.8'
    ds.attrs['history'] = 'original NWCSAF file modified to be compatible with ADAGUC, WCT ...'
    ds.attrs['featureType'] = 'timeSeries'

    # suppress SerializationWarning
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        ds.to_netcdf(name, unlimited_dims=['time', 'station'], engine='netcdf4', encoding=enc)

    fix_attr_to_CF(name)


def main():
    desc = """ 
            Given a country or list of them, builds a list netCDF4 files that are CF-1.8 compliant 
            and contain data related to the presence of fog in the country's airports 
            """
    parser = argparse.ArgumentParser(description=desc)

    # arguments
    parser.add_argument('-c', '--countries', type=str, required=True,
                        help='txt file with countries separated by a newline')
    parser.add_argument('-s', '--start', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'), required=True,
                        help='starting day in ISO format: yyyy-mm-dd')
    parser.add_argument('-e', '--end', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'), required=True,
                        help='ending day in ISO format: yyyy-mm-dd')
    parser.add_argument('-d', '--delta', type=int, required=True,
                        help='increment of time for every netCDF in mins')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='output info of the current state of the program')

    r = parser.parse_args()

    # open file with countries
    try:
        with open(r.countries, 'r') as f:
            # remove newline char and any possible duplicates
            c = [x.replace('\n', '') for x in f.readlines()]
            c = list(dict.fromkeys(c))

            icao_dict = download_country_METARS(c, r.start, r.end, r.verbose)

            print("Processing METARS")
            process_metars(r.start, r.end, datetime.timedelta(minutes=r.delta), icao_dict, r.verbose)

            print("Done!")

    except FileNotFoundError as fnferr:
        print(fnferr)


if __name__ == '__main__':
    main()
