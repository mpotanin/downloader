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
    if not os.path.exists(columns[1]):
        os.mkdir(columns[1])
    command = ('aws s3 cp "s3://sentinel-s2-l2a/tiles/'
              + str(int(columns[1][39:41])) + '/' + columns[1][41:42] + '/' + columns[1][42:44] + '/'
              + columns[1][11:15] + '/' + str(int(columns[1][15:17])) + '/' + str(int(columns[1][17:19])) + '" "' + columns[1] + '" '
              + '--recursive --request-payer requester')
    os.system(command + ' > /dev/null')
    if len(os.listdir(columns[1]))==0:
        print(columns[1] + " - isn't available on AWS")
        os.rmdir(columns[1])
        continue

    command = ('aws s3 cp "s3://sentinel-s2-l2a/products/'
      + columns[1][11:15] + '/' + str(int(columns[1][15:17])) + '/' + str(int(columns[1][17:19])) + '/' + columns[1]
      + '" "' + columns[1] + '" '  + ' --recursive --request-payer requester')

    os.system(command + ' > /dev/null')
    print(columns[1])
