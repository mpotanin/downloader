import sys
import datetime
import array


ARGS = {
    '-year': "for what year to analyze data",
    '-fids': 'fields ids comma separated',
    '-lid': 'layerid',
    '-coln': 'column name',
    '-colval': 'column values comma separated',
    '-ndvi_filt':'some ndvi curve metrics filter',
    '-alg':'averaging algorithm: average, dlog)',
    '-o': 'output filename without extension',
    '-mindoy': 'ndvi curve min day of the year ',
    '-maxdoy': 'ndvi curve max day of the year '
}

USAGE_EXAMPLES = ("ndvi_metrics.py ...\n"
                    "ndvi_metrics.py ...\n"
                )


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


def get_option_value (optname, isflag = False) :
    for i in range(1,len(sys.argv)) :
        if (sys.argv[i] == optname) :
            if isflag : return True
            else :
                if i == len(sys.argv)-1 : return ""
                else: return sys.argv[i+1]
    return ""

#############################################################