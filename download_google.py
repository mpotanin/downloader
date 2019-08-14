import console_utils
import sys
import os
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




DOWNLOAD_GOOGLE_ARGS = {
    '-i': 'input csv file',
    '-o': 'output folder',
    '-cred': 'google credentials json file path'
}

USAGE_EXAMPLES = ("download_google.py -i s2.csv -o raw/sentinel2\n")
#####################################################################

# class SceneDownloader 
# stores base url as class object
# stores sceneid as instance object
# stores file list template as derivative class object  
# instance method that runs downloading: creates folder with temporal ending and renames after downloading is finished 
#
#



class BucketFolder :
    def __init__(self,bucket_name,bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix

    @staticmethod
    def __file_as_bytes(file) :
        with file:
            return file.read()
        
       


    @staticmethod 
    def __is_blob_copy_correct (blob, filename) :
        file_md5_hash = base64.b64encode(hashlib.md5(BucketFolder.__file_as_bytes(open(filename, 'rb'))).digest())
        return True if (file_md5_hash.decode() == blob.md5_hash) else False


    @staticmethod
    def __is_blob_folder (blob) :
        return True if blob.name.find('_$folder$')>=0 else False
    
    @staticmethod
    def __get_filename (blob) :
        return blob.name[blob.name.rfind('/')+1:]

    def __get_relative_path (self, blob) :
        if BucketFolder.__is_blob_folder (blob) :
            return (blob.name[:blob.name.find('_$folder$')])[len(self.bucket_prefix):]
        else :
            return (blob.name[:blob.name.rfind('/')])[len(self.bucket_prefix):]

    def __download_file (self,blob,dest_folder) :
        dest_filename = self.__get_filename(blob)
        dest_path = dest_folder + self.__get_relative_path(blob)
        os.makedirs(dest_path,exist_ok=True)
        dest_full_path = dest_path + '/' + dest_filename
        blob.download_to_filename(dest_full_path)
        if not BucketFolder.__is_blob_copy_correct(blob,dest_full_path) :
            os.remove(dest_full_path)
            blob.download_to_filename(dest_full_path)
            if not BucketFolder.__is_blob_copy_correct(blob,dest_full_path) :
                os.remove(dest_full_path)
                return False
        return True

    def download_all (self, dest_path, dest_folder) :
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(self.bucket_name,prefix=self.bucket_prefix)

        for blob in blobs:
            if not self.__is_blob_folder(blob) :
                if not self.__download_file(blob,dest_path +'/' +dest_folder) : return False
        
        return True


class S2BucketFolder(BucketFolder) :

    def __init__(self,bucket_prefix) :
        super(S2BucketFolder,self).__init__('gcp-public-data-sentinel-2',bucket_prefix)
   
    @staticmethod
    def get_prefix_by_sceneid (sceneid) :
        return 'tiles/'+sceneid[39:41]+'/'+sceneid[41:42]+'/'+sceneid[42:44]+'/'+sceneid+'.SAFE'
                
class L8BucketFolder(BucketFolder) :
    
    def __init__(self,bucket_prefix) :
        super(L8BucketFolder,self).__init__('gcp-public-data-landsat',bucket_prefix)

    @staticmethod
    def get_prefix_by_sceneid (sceneid) : #ToDo
        #LC08/01/171/021/LC08_L1TP_171021_20170131_20170215_01_T2
        return 'LC08/01/'+sceneid[10:13] + '/'+sceneid[13:16]+'/'+sceneid


#####################################################################
if (len(sys.argv) == 1) :
    console_utils.print_usage(DOWNLOAD_GOOGLE_ARGS)
    #exit 0

read_args_from_file = False
json_file_params = 'download_l8_params.json'

args = ( sys.argv if not read_args_from_file
        else console_utils.parse_args_from_json_file(json_file_params))

if not console_utils.check_input_args(DOWNLOAD_GOOGLE_ARGS,args) :
    print ('ERROR: not valid input args')
    exit(1)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = console_utils.get_option_value(args,'-cred')
input_csv = console_utils.get_option_value(args,'-i')
output_path = console_utils.get_option_value(args,'-o')

with open(input_csv,newline='') as csvfile :
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in csvreader :
        dest_folder = None
        bucket_folder = None
        if (row[0]=='Sentinel 2') :
            bucket_folder = S2BucketFolder(S2BucketFolder.get_prefix_by_sceneid(row[1]))
            dest_folder = row[1]+'.SAFE'
        elif (row[0]=='Landsat 8'):
            bucket_folder = L8BucketFolder(L8BucketFolder.get_prefix_by_sceneid(row[2]))
            dest_folder = row[1]
        else : continue
        
        full_path = output_path +'/' + dest_folder
        if (os.path.exists(full_path)): shutil.rmtree(full_path)
                
        if not bucket_folder.download_all(output_path,dest_folder) :
            print ('ERROR: downloading scene data: ' + dest_folder)
            exit(2)
        else :
            os.rename(output_path +'/' + dest_folder,
                    output_path +'/' + dest_folder[:-6])

                        #storage_client = storage.Client()
#blobs = storage_client.list_blobs('gcp-public-data-sentinel-2',prefix='tiles/33/U/UP/S2A_MSIL1C_20150711T100006_N0204_R122_T33UUP_20150711T100008.SAFE')

#https://console.cloud.google.com/storage/browser/gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE/
#https://storage.cloud.google.com/gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE/INSPIRE.xml?_ga=2.243900869.-1305591509.1565596600


#gcp-public-data-sentinel-2/tiles/33/U/UP/S2A_MSIL1C_20150704T101337_N0202_R022_T33UUP_20160606T205155.SAFE


