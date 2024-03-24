import os
import sys
import argparse
import boto3
import pathlib



class S3:
    @staticmethod
    def check_s3_path (s3_path) :
        if len(s3_path) == 0: return ''
        elif s3_path[-1] != '/': return s3_path + '/'
        else: return s3_path

    @staticmethod
    def get_file_data(s3_client, bucket_name, key, req_pays=False):

        response = None
        fun_kwargs = ({'Bucket':bucket_name, 'Key':key, 'RequestPayer':'requester'} if req_pays
                      else {'Bucket':bucket_name, 'Key':key})
        response = s3_client.get_object(**fun_kwargs)

        return response['Body'].read()


    @staticmethod
    def download_file(s3_client,bucket_name,key,output_path,req_pays=False):
        file_data = S3.get_file_data(s3_client,bucket_name,key,req_pays)
        f = open(output_path, 'wb')
        f.write(file_data)
        f.close()



    def listsubfolders(s3_client, bucket_name, remote_dir, req_pays=False):

        remote_dir = S3.check_s3_path(remote_dir)

        all_objects = None
        fun_kwargs = {'Bucket':bucket_name, 'Delimiter':'/'}
        if req_pays: fun_kwargs['RequestPayer'] = 'requester'
        if remote_dir != '' and remote_dir is not None: fun_kwargs['Prefix'] = remote_dir

        objects = s3_client.list_objects(**fun_kwargs)
        subfolders = list()
        for o in objects.get('CommonPrefixes'):
            subfolders.append(o.get('Prefix')[len(remote_dir):-1])

        return subfolders




    @staticmethod
    def download_dir_recursive(s3_client,bucket_name,remote_dir,local_dir,req_pays=False):
        if remote_dir is None or len(remote_dir) == 0: return  False
        else: remote_dir = S3.check_s3_path(remote_dir)


        fun_kwargs = ({'Bucket': bucket_name, 'Prefix' : remote_dir, 'RequestPayer' : 'requester'} if req_pays
                        else {'Bucket': bucket_name, 'Prefix' : remote_dir})

        all_objects = s3_client.list_objects(**fun_kwargs)
        if 'Contents' not in all_objects.keys(): return False
        remote_dir = os.path.dirname(remote_dir[:-1])
        for obj in all_objects['Contents']:
            filename = os.path.basename(obj['Key'])
            rel_path = os.path.dirname(obj['Key'])[len(remote_dir) + 1:]

            loc_path = os.path.join(local_dir,rel_path)
            if not os.path.exists(loc_path):
                pathlib.Path(loc_path).mkdir(parents=True, exist_ok=True)

            S3.download_file(s3_client,bucket_name,obj['Key'],os.path.join(loc_path,filename),req_pays)

        return True



