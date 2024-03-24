import os
import sys
import copy
import math
import numpy as np
import datetime
from common_utils import raster_proc as rproc
from common_utils import vector_operations as vop
from osgeo import gdal, osr, ogr
import json


class SceneID:
    # "S2A_MSIL2A_20200501T065631_N0214_R063_T40UFB_20200501T100734/"
    @staticmethod
    def date(sceneid):
        return sceneid[11:19]

    @staticmethod
    def day(sceneid, type='string'):
        return sceneid[17:19] if type == 'string' else int(sceneid[17:19])

    @staticmethod
    def month(sceneid, type='string'):
        return sceneid[15:17] if type == 'string' else int(sceneid[15:17])

    @staticmethod
    def year(sceneid, type='string'):
        return sceneid[11:15] if type == 'string' else int(sceneid[11:15])

    @staticmethod
    def tile(sceneid):
        return sceneid[39:44]


class L2AScene:
    #BANDS = ['B02','B03','B04','B05','B06','B07','B08', 'B11', 'B12','SCL']
    BANDS = ['B02', 'B03', 'B04', 'B08', 'B12']

    @staticmethod
    def get_band_index_by_spec_name(band_spec_name):
        return ['BLUE', 'GREEN', 'RED', 'NIR', 'SWIR'].index(band_spec_name)

    @staticmethod
    def get_scene_id_from_path (scene_full_path):
        while scene_full_path[-1] == '\\' or scene_full_path[-1] == '/':
            scene_full_path = scene_full_path[:-1]
        return os.path.basename(scene_full_path)

    @staticmethod
    def get_proper_tile_version (scene_full_path):
        sceneid = L2AScene.get_scene_id_from_path(scene_full_path)
        if not os.path.exists(os.path.join(scene_full_path,'1')):
            return 0
        else:
            # Opening JSON file
            f = open(os.path.join(scene_full_path,'0/productInfo.json'))
            data = json.load(f)
            f.close()
            return 0 if data['name']==sceneid else 1



    @staticmethod
    def get_band_file(scene_full_path, band_name):
        tv = L2AScene.get_proper_tile_version(scene_full_path)
        return  f'{tv}/R10m/{band_name}.jp2' if band_name in ['B02', 'B03', 'B04', 'B08'] else f'0/R20m/{band_name}.jp2'

    @staticmethod
    def get_pixel_quality_file (scene_full_path):
        tv = L2AScene.get_proper_tile_version(scene_full_path)
        return f'{tv}/R20m/SCL.jp2'

    @staticmethod
    def get_cloud_mask_file (scene_full_path):
        tv = L2AScene.get_proper_tile_version(scene_full_path)
        return f'{tv}/R20m/SCL.jp2'

    @staticmethod
    def transform_sr_values (raw_values):
        return raw_values*0.0001

    @staticmethod
    def calc_SR_values (band_file_full_path):
        return L2AScene.transform_sr_values(rproc.open_clipped_raster_as_image(band_file_full_path))

    @staticmethod
    def calc_valid_pixels_mask (cloud_qa_pixels, qa_pixels):
        valid_pixels = np.empty( (cloud_qa_pixels.shape[0], cloud_qa_pixels.shape[1], 1), dtype=np.uint8 )

        valid_pixels[:,:,0] = np.where(
            (qa_pixels == 2) | (qa_pixels == 4) | (qa_pixels == 5) | (qa_pixels == 6) | (qa_pixels == 7),1,0)

        return valid_pixels
