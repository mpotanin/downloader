import sys

def check_input_args_def_def (args_def) :
    #TODO: loop sys.argv
    for i in range(1,len(sys.argv)) :
        if (sys.argv[i][0]=="-") :
            if (sys.argv[i] not in args_def.keys()) :
                print ("ERROR: unknown option name: " + sys.argv[i])
                return False
        elif (not (sys.argv[i-1][0]=="-")) :
             print ("ERROR: not expected option value: " + sys.argv[i])
             return False
    return True

def print_usage (args_def) :
    #TODO: print args_def
    max_width = 80
    print("")
    print("Usage:")
    usage_text=""
    new_line = ""
    for k in args_def.keys() :
        opt_text = "["+str(k)+" " + args_def[k]+"]"
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