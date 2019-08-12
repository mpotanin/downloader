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



DOWNLOAD_GOOGLE_ARGS = {
    '-i': 'input csv file',
    '-o': 'output folder',
    '-wget':'wget path'
}

USAGE_EXAMPLES = ("download_google.py -i s2.csv -o raw/sentinel2\n")
#####################################################################

# class SceneDownloader 
# stores base url as class object
# stores sceneid as instance object
# stores file list template as derivative class object  
# instance method that runs downloading: creates folder with temporal ending and renames after downloading is finished 
#
#

#####################################################################
if (len(sys.argv) == 1) :
    console_utils.print_usage(DOWNLOAD_GOOGLE_ARGS)
    #return 0





