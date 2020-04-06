# Introduction
Given a list of countries builds a set of netCDF4 files that are CF-1.8 compliant and contain weather 
data realated to the presence of fog. Requieres Python >= 3.6. An example txt file with countries is already provided.

# Dependencies
* `pandas 1.0.3`
* `netcdf4 1.5.3`
* `xarray 0.15.0`
* `requests 2.25.0`

# Usage Example
If one would want to request data from 2020-01-01 to 2020-01-02, where the netCDF4 files are generated 
at intervals of 5 mins (only if there are METAR entries) for the countries provided in `countries.txt`, the 
following command would be applied: `python3 build_database.py -c countries.txt -s 2020-01-01 -e 2020-01-02 -d 5 -v`, 
where `-v` stands for verbose output.
For more information on command options: `python3 build_database.py -h`.

