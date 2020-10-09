import os
import sys

with open("/vol_img/Uttar_Pradesh/4tiles.csv") as f:
    lines = f.read().splitlines()

output_folder = "/vol_img/Uttar_Pradesh/img_zip2"

os.chdir(output_folder)
for l in lines:
    columns = l.split(',')
    command = 'wget --content-disposition --continue --user=mpotanin --password=kosmosnimkiesa "https://scihub.copernicus.eu/dhus/odata/v1/Products(\''
    command += columns[2]
    command +='\')/\\$value"'
    os.system(command)
    print(columns[1])
    
    #exit(0)