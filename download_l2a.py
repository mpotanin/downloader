import os
import sys
import argparse

parser = argparse.ArgumentParser(description = 'Download products by parsing output csv file from query.py script. Requires wget')

parser.add_argument('-i',required=True, metavar='input csv', help='Input csv file')
parser.add_argument('-o', required=True, metavar='output foder', help='Output folder')


if (len(sys.argv)==1):
    parser.print_usage()
    exit(0)

args = parser.parse_args()



with open(args.i) as f:
    lines = f.read().splitlines()
lines.pop(0)
os.chdir(args.o)
for l in lines:
    columns = l.split(',')
    command = 'wget --content-disposition --continue --user=mpotanin --password=kosmosnimkiesa "https://scihub.copernicus.eu/dhus/odata/v1/Products(\''
    command += columns[2]
    command +='\')/\\$value"'
    os.system(command)
    print(columns[1])
    
    #exit(0)