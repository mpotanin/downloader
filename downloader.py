import sys
import datetime
import array
import pycurl
import StringIO




ARGS = {
    '-u': "username to get access to USGS/SciHub",
    '-p': "password to get access to USGS/SciHub",
    '-b': 'border geojson file with polygon/multipolygon geometry',
    '-p': 'platform: s2|l8',
    '-sd': 'start date yyyymmdd',
    '-ed': 'end date yyyymmdd',
    '-cld': 'max cloud filter',
    '-o': 'output csv file name'
}

USAGE_EXAMPLES = ("downloader.py -u user -p password -b 1.geojson -p s2 -sd 20190501 -ed 20191001 -cld 50 -o 1.csv\n")


def check_input_args () :
    #TODO: loop sys.argv
    for i in range(1,len(sys.argv)) :
        if (sys.argv[i][0]=="-") :
            if (sys.argv[i] not in ARGS.keys()) :
                print ("ERROR: unknown option name: " + sys.argv[i])
                return False
        elif (not (sys.argv[i-1][0]=="-")) :
             print ("ERROR: not expected option value: " + sys.argv[i])
             return False
    return True

def print_usage () :
    #TODO: print ARGS
    max_width = 80
    print("")
    print("Usage:")
    usage_text=""
    new_line = ""
    for k in ARGS.keys() :
        opt_text = "["+str(k)+" " + ARGS[k]+"]"
        if (len(new_line) + len(opt_text)>max_width) :
            usage_text+=new_line+"\n"
            new_line=opt_text
        else : new_line+=opt_text
    usage_text+=new_line
    print (usage_text)
    return 0


def get_option_Value (optname, isflag = False) :
    for i in range(1,len(sys.argv)) :
        if (sys.argv[i] == optname) :
            if isflag : return True
            else :
                if i == len(sys.argv)-1 : return ""
                else: return sys.argv[i+1]
    return ""

#############################################################

####################
# create usgs_query
# create scihub_query
# parse usgs_query
# parse sci_hub query
# save output csv file

class MetadataExtractor:
    def create_query () :
        print ()
    def parse_response () :
        print ()
    def write_output_file () :
        print ()

class USGSMetadataExtractor(MetadataExtractor):
    def create_query () :
        print ()
    def parse_response () :
        print ()
    def write_output_file () :
        print ()

###################
print_usage()
