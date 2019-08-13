import console_utils
import sys
import os
import datetime
import array
import json
import csv
from google.cloud import storage
from google.oauth2 import service_account
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

# Instantiates a client

'''
credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\m.potanin\\Downloads\\DownloaderS2-L8-bc99c45ffb4c.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
'''
storage_client = storage.Client()


blobs = storage_client.list_blobs('gs://gcp-public-data-sentinel-2')

#https://console.cloud.google.com/storage/browser/gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE/
#https://storage.cloud.google.com/gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE/INSPIRE.xml?_ga=2.243900869.-1305591509.1565596600

for blob in blobs:
    print(blob.name)

#gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE



