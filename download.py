import console_utils
import sys
import os
import datetime
import array
import json
import csv
import pycurl
from io import BytesIO
import requests



DOWNLOAD_ARGS = {
    '-u': 'username to query USGS/SciHub',
    'p': 'password to query USGS/SciHub',
    '-b': 'border geojson file with polygon/multipolygon geometry',
    '-sat': 'platform: s2|l8',
    '-sd': 'start date yyyymmdd',
    '-ed': 'end date yyyymmdd',
    '-cld': 'max cloud filter',
    '-o': 'output csv file name'
}

USAGE_EXAMPLES = ("download.py -u user:password -b 1.geojson -p s2 -sd 20190501 -ed 20191001 -cld 50 -o 1.csv\n")
#############################################################






