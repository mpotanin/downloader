import os
import sys
import argparse

parser = argparse.ArgumentParser(description = 'Download products by parsing output csv file from query.py script. Requires wget')

parser.add_argument('-i',required=True, metavar='input csv', help='Input csv file')
parser.add_argument('-o', required=True, metavar='output foder', help='Output folder')
parser.add_argument('-aws', help='Download S2-L2 from AWS', action='store_true')


if (len(sys.argv)==1):
    parser.print_usage()
    exit(0)

args = parser.parse_args()



with open(args.i) as f:
    lines = f.read().splitlines()
lines.pop(0)
os.chdir(args.o)
if args.aws is None:
    for l in lines:
        columns = l.split(',')
        command = 'wget --content-disposition --continue --user=mpotanin --password=kosmosnimkiesa "https://scihub.copernicus.eu/dhus/odata/v1/Products(\''
        command += columns[2]
        command +='\')/\\$value"'
        os.system(command)
        print(columns[1])
else:
    for l in lines:
        columns = l.split(',')
        if not os.path.exists(columns[1]):
            os.mkdir(columns[1])
        command = ('aws s3 cp "s3://sentinel-s2-l2a/tiles/'
                  + str(int(columns[1][39:41])) + '/' + columns[1][41:42] + '/' + columns[1][42:44] + '/'
                  + columns[1][11:15] + '/' + str(int(columns[1][15:17])) + '/' + str(int(columns[1][17:19])) + '" "' + columns[1] + '" ' 
                  + '--recursive --request-payer requester')
        os.system(command + ' > /dev/null')
        
        command = ('aws s3 cp "s3://sentinel-s2-l2a/products/' 
          + columns[1][11:15] + '/' + str(int(columns[1][15:17])) + '/' + str(int(columns[1][17:19])) + '/' + columns[1]  
          + '" "' + columns[1] + '" '  + ' --recursive --request-payer requester')
          
        os.system(command + ' > /dev/null')
        print(columns[1])
