from downloader import s2_meta
from downloader.s3_common import S3
import boto3


class AWS_L2A:
    s3_client = None
    l2a_bucket = 'sentinel-s2-l2a'

    @staticmethod
    def init(aws_access_key_id, aws_secret_access_key) :
        AWS_L2A.s3_client = boto3.client('s3',
                                       aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)

    @staticmethod
    def get_l2a_prod_dir (sceneid):
        return f'products/{s2_meta.SceneID.year(sceneid)}' \
               f'/{s2_meta.SceneID.month(sceneid,type=int)}' \
               f'/{s2_meta.SceneID.day(sceneid,type=int)}' \
               f'/{sceneid}'

    @staticmethod
    def get_l2a_tile_dir(sceneid):
        return f'tiles/{s2_meta.SceneID.tile_name(sceneid)[0:2]}' \
               f'/{s2_meta.SceneID.tile_name(sceneid)[2:3]}' \
               f'/{s2_meta.SceneID.tile_name(sceneid)[3:5]}' \
               f'/{s2_meta.SceneID.year(sceneid)}' \
               f'/{s2_meta.SceneID.month(sceneid,type=int)}' \
               f'/{s2_meta.SceneID.day(sceneid,type=int)}'

    @staticmethod
    def download_l2a_scene (sceneid, dest_folder):
        scene_path = os.path.join(dest_folder,sceneid)
        if not os.path.exists(scene_path):
            os.mkdir(scene_path)

    @staticmethod
    def download_bands(sceneid,bands,dest_folder):
        return True

    def download_metadata(sceneid,dest_folder):
        return True

    def aws2scihub (sceneid_loc_folder):
        return True

class AWS_COG:
    s3_client = None
    bucket = 'sentinel-cogs'
    root_dir = 'sentinel-s2-l2a-cogs'
    base_url = 'https://sentinel-cogs.s3.us-west-2.amazonaws.com'
    #S2B_39UVB_20190818_0_L2A

    @staticmethod
    def init(aws_access_key_id, aws_secret_access_key):
        AWS_COG.s3_client = boto3.client('s3',
                                     aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
    @staticmethod
    def tile(scene):
        return scene[4:9]

    @staticmethod
    def month_day(scene):
        return scene[14:18]

    @staticmethod
    def list_scenes_by_tile_year_month (tile,year,month, single_version_only = False):
        remote_dir = f'{AWS_COG.root_dir}/{tile[0:2]}/{tile[2:3]}/{tile[3:5]}/{year}/{month}/'
        if not single_version_only:
            return S3.listsubfolders(AWS_COG.s3_client,AWS_COG.bucket,remote_dir,True)
        else:
            scenes = S3.listsubfolders(AWS_COG.s3_client,AWS_COG.bucket,remote_dir,True)
            scenes_filt = dict()
            for scene in scenes:
                date = scene[10:18]
                if date not in scenes_filt:
                    scenes_filt[date] = scene
                elif int(scene[19:20])>int(scenes_filt[date][19:20]):
                    scenes_filt[date] = scene
            return list(scenes_filt.values())

    @staticmethod
    def get_remote_dir_by_scene(scene):
        #S2B_39UVB_20231225_0_L2A
        return f'{AWS_COG.root_dir}/{scene[4:6]}/{scene[6:7]}/{scene[7:9]}/{scene[10:14]}/{int(scene[14:16])}/{scene}'

    @staticmethod
    def get_scl_url (scene):
        return f'{AWS_COG.base_url}/{AWS_COG.get_remote_dir_by_scene(scene)}/SCL.tif'

    @staticmethod
    def get_10m_bands_urls(scene):
        bands=['B02.tif','B03.tif','B04.tif','B08.tif']
        return [f'{AWS_COG.base_url}/{AWS_COG.get_remote_dir_by_scene(scene)}/{b}' for b in bands]


class GCS:
    s3_client = None
    l1c_bucket = 'gcp-public-data-sentinel-2'

    @staticmethod
    def init(google_access_key_id, google_access_key_secret) :
        GCS.s3_client = boto3.client('s3',
                                    region_name="auto",
                                    endpoint_url="https://storage.googleapis.com",
                                    aws_access_key_id=google_access_key_id,
                                    aws_secret_access_key=google_access_key_secret)

    def get_l1c_dir (sceneid):
        return  f'tiles/{s2_meta.SceneID.tile_name(sceneid)[0:2]}' \
                f'/{s2_meta.SceneID.tile_name(sceneid)[2:3]}' \
                f'/{s2_meta.SceneID.tile_name(sceneid)[3:5]}' \
                f'/{sceneid}.SAFE'

            # products/2021/5/15/S2A_MSIL2A_20210515T004641_N0300_R045_T56TNS_20210515T025256/
# Key: products/2021/5/15/S2A_MSIL2A_20210515T004641_N0300_R045_T56UNU_20210515T025256/metadata.xml

"""
    @staticmethod
    def download_dir_from_s3(bucket_name, remote_dir):
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucketName)
    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key) # save to same path
"""


