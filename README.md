# downloader
python scripts for downloading s2-l8 data

Script 1
Queries USGS/SCI-Hub and creates csv file with columns: sceneid, productid, date, download url (google)

Input params:
-p: password
-u: user
-border: geojson file with one polygon/multipolygon 
-platform: S2/L8
-startdate: yyyy-mm-dd
-enddate: yyyy-mm-dd
-output: output file name
-maxcld: max cloud percentage

Script 2
Run bulk download and processing

Input params:
-file: output file of script 1
-p: password
-u: user
-download_dir: a dir for downloaded files save to
-l2a_dir: a dir for l2a products write to
-l2a_config: config file to run for l2a processor 

