"""
Script downloads Sentinel2/Landsat8 from public Google S3 buckets. L8/S2 scenes are stored unpacked in buckets. 
Each scene is downloaded file by file and stored into separate folder.
"""

import console_utils
import sys
import os
import time
import datetime
import array
import json
import csv
from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO
import requests
import hashlib
import base64
import shutil



# Console arguments description
DOWNLOAD_GOOGLE_ARGS = {
    '-i': 'input csv file',
    '-o': 'output folder',
    '-cred': 'google credentials json file path',
    '-log' : 'errors file log '
}

USAGE_EXAMPLES = ("download_google.py -i s2.csv -o raw/sentinel2\n")




class BucketFolder :
    """
    Represents abstract Google Storage bucket that contains blobs (files or folders).
    """

    def __init__(self,bucket_name,bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix

    #Alien method, should be removed
    @staticmethod
    def __file_as_bytes(file) :
        with file:
            return file.read()
 

    #Compares file blob and file on disk
    @staticmethod
    def __is_blob_copy_correct (blob, filename) :
        file_md5_hash = base64.b64encode(hashlib.md5(BucketFolder.__file_as_bytes(open(filename, 'rb'))).digest())
        return True if (file_md5_hash.decode() == blob.md5_hash) else False


    @staticmethod
    def __is_blob_a_folder (blob) :
        return True if blob.name.find('_$folder$')>=0 else False
    
    #Cuts out filename (part of blob name after last slash)
    @staticmethod
    def __get_filename (blob) :
        return blob.name[blob.name.rfind('/')+1:]

    def __get_relative_path (self, blob) :
        if BucketFolder.__is_blob_a_folder (blob) :
            return (blob.name[:blob.name.find('_$folder$')])[len(self.bucket_prefix):]
        else :
            return (blob.name[:blob.name.rfind('/')])[len(self.bucket_prefix):]


    #Downloads single blog to disk file
    def __download_file (self,blob,dest_folder) :
        dest_filename = self.__get_filename(blob)
        dest_path = dest_folder + self.__get_relative_path(blob)
        os.makedirs(dest_path,exist_ok=True)
        dest_full_path = dest_path + '/' + dest_filename
        blob.download_to_filename(dest_full_path)
        if not BucketFolder.__is_blob_copy_correct(blob,dest_full_path) :
            os.remove(dest_full_path)
            return False
        else: return True


    def try_download_all (self, dest_path) :
        """
        Tries to downloads all blobs inside folder (=bucket_prefix). 
        Preserves all structure: file names, folder names, nesting

        Args:
            dest_path (string): destination path on local disk

        Return:
            bool if success otherwise raise Exception 
        """
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(self.bucket_name,prefix=self.bucket_prefix)

        scene_exists = False

        for blob in blobs:
            scene_exists = True
            if not self.__is_blob_a_folder(blob) :
                if not self.__download_file(blob,dest_path) : 
                    raise Exception('file download failure: ' + blob.name)
        
        if not scene_exists : 
            raise Exception("Scene doesn't exist: " + self.bucket_prefix)
        else : return True


class S2BucketFolder(BucketFolder) :
    """
    Represents Sentinel 2 bucket on Google Storage that contains 
    files and folders of Sentinel 2 data structure
    """
    def __init__(self,bucket_prefix) :
        super(S2BucketFolder,self).__init__('gcp-public-data-sentinel-2',bucket_prefix)
   
    @staticmethod
    def get_prefix_by_sceneid (sceneid) :
        """tiles/33/U/UP/S2A_MSIL1C_20150711T100006_N0204_R122_T33UUP_20150711T100008"""
        return 'tiles/'+sceneid[39:41]+'/'+sceneid[41:42]+'/'+sceneid[42:44]+'/'+sceneid+'.SAFE'
                
class L8BucketFolder(BucketFolder) :
    """
    Represents Landsat 8 bucket on Google Storage that contains files of Landsat 8 data structure
    """
    def __init__(self,bucket_prefix) :
        super(L8BucketFolder,self).__init__('gcp-public-data-landsat',bucket_prefix)

    @staticmethod
    def get_prefix_by_sceneid (sceneid) :
        """LC08/01/171/021/LC08_L1TP_171021_20170131_20170215_01_T2""" 
        return 'LC08/01/'+sceneid[10:13] + '/'+sceneid[13:16]+'/'+sceneid


#################################################################################
# 
# 1. Parse input args
# 2. Loop through rows of input csv file which contains sceneids of L8/S2
# 3. For each scneneid creates BucketFolder instance and tries to download 
#    all data into separate folder. If error happens it is store to error list
# 4. Saves errors log file
#  
#################################################################################

if (len(sys.argv) == 1) :
    console_utils.print_usage(DOWNLOAD_GOOGLE_ARGS)
    exit(0)

read_args_from_file = False
json_file_params = 'download_s2_params.json'

args = ( sys.argv if not read_args_from_file
        else console_utils.parse_args_from_json_file(json_file_params))

if not console_utils.check_input_args(DOWNLOAD_GOOGLE_ARGS,args) :
    print ('ERROR: not valid input args')
    exit(1)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = console_utils.get_option_value(args,'-cred')
input_csv = console_utils.get_option_value(args,'-i')
output_path = console_utils.get_option_value(args,'-o')
log_file = console_utils.get_option_value(args,'-log')


num_success = 0
num_error = 0
download_attempts = 2
interval_long_sec = 300
interval_sec = 10
failed_scenes = list()
with open(input_csv,newline='') as csvfile :
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in csvreader :
        dest_folder = None
        bucket_folder = None
        if len(row)<=1 : continue 

        if (row[0]=='Sentinel 2') :
            bucket_folder = S2BucketFolder(S2BucketFolder.get_prefix_by_sceneid(row[1]))
            dest_folder = row[1]+'.SAFE'
        elif (row[0]=='Landsat 8'):
            bucket_folder = L8BucketFolder(L8BucketFolder.get_prefix_by_sceneid(row[2]))
            dest_folder = row[1]
        else : continue
        
        full_path = output_path +'/' + dest_folder
        if (os.path.exists(full_path)): shutil.rmtree(full_path)
             
        for i in range(0,download_attempts) :
            try :
                bucket_folder.try_download_all(full_path + '__temp')
                os.rename(full_path + '__temp',full_path)
                num_success+=1
                break
            except Exception as inst: 
                if (i==download_attempts-1) :
                    failed_scenes.append({"scene":dest_folder,
                                        "error_msg" :str(inst)})
                    num_error+=1
                else :
                    if (str(inst)[0:4]=='503') : time.sleep(interval_long_sec)
                    else : time.sleep(interval_sec)
        

if (num_error > 0) :
    with open (log_file, 'w', newline='') as file :
        writer = csv.DictWriter(file, fieldnames=["scene","error_msg"])
        writer.writeheader()
        for e in failed_scenes :
            writer.writerow(e)
        file.close()
        
print ("Success downloads: " + str(num_success))
print ("Failed downloads: " + str(num_error))
exit(0)