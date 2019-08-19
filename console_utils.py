"""
This module provides some functionality to work with command line arguments.
PS. May be it's a good idea to shift towards "import argparse" instead of "bike invention"
"""

import sys
import json


def check_input_args (args_def, args) :
    """ 
    Verifies if input args matches template

    Args:
        args_def (Dictionary): args template
        args (List): input arguments to check

    Returns:
        bool: True for corret input, False otherwise
    """
    for i in range(1,len(args)) :
        if (args[i][0]=="-") :
            if (args[i] not in args_def.keys()) :
                print ("ERROR: unknown option name: " + args[i])
                return False
        elif (not (args[i-1][0]=="-")) :
             print ("ERROR: not expected option value: " + args[i])
             return False
    return True

def print_usage (args_def) :
    """ prints arguments template """
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


def get_option_value (args, optname, isflag = False) :
    """
    Gets option value by its name.

    Args:
        args (List): input arguments
        optname (str): optiona name, e.g. "-o"
        isflag (bool): True if a flag option
    
    Returns:
        string: option value or empty string ""
    """

    for i in range(1,len(args)) :
        if (args[i] == optname) :
            if isflag : return True
            else :
                if i == len(args)-1 : return ""
                else: return args[i+1]
    if isflag : return False
    else : return ""


def parse_args_from_json_file (json_file) :
    """
    Parse arguments to run script from json file.

    Args:
        json_file (string): json file path
    
    Returns:
        List: if success - parsed arguments, otherwise - empty list
    """
    args = list()
    try:
        with open (json_file, 'r') as file :
            data_from_file=file.read()
            json_obj = json.loads(data_from_file)
            args.append(sys.argv[0])
            for opt in json_obj:
                args.append(opt)
                args.append(json_obj[opt])
    except Exception as inst:
        print ('ERROR: parsing json file: ' + json_file)
        print ('Exception: ' + str(type(inst)))
        return list()
    return args