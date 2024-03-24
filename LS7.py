import os
import sys
import copy
import math
import numpy as np
import datetime
from common_utils import raster_proc as rproc
from common_utils import vector_operations as vop
from osgeo import gdal, osr, ogr


class SceneID:
    # B1 - BLUE, B2 - GREEN, B3 - RED, B4 - NIR, B5/B7 - SWIR (like B5/B7 LS05)
    # LE07_L2SP_172022_20000421_20200918_02_T1
    # LE07_L2SP_172022_20000421_20200918_02_T1_SR_B4.TIF
    @staticmethod
    def date(sceneid):
        return sceneid[17:25]

    @staticmethod
    def day(sceneid, type='string'):
        return sceneid[23:25] if type == 'string' else int(sceneid[23:25])

    @staticmethod
    def month(sceneid, type='string'):
        return sceneid[21:23] if type == 'string' else int(sceneid[21:23])

    @staticmethod
    def year(sceneid, type='string'):
        return sceneid[17:21] if type == 'string' else int(sceneid[17:21])

    @staticmethod
    def tile(sceneid):
        return sceneid[10:16]


class L2AScene:
    BANDS = ['B1','B2','B3','B4','B7']


    @staticmethod
    def get_band_index_by_spec_name(band_spec_name):
        return ['BLUE', 'GREEN', 'RED', 'NIR', 'SWIR'].index(band_spec_name)



    @staticmethod
    def get_scene_id_from_path (scene_full_path):
        while scene_full_path[-1] == '\\' or scene_full_path[-1] == '/':
            scene_full_path = scene_full_path[:-1]
        return os.path.basename(scene_full_path)

    @staticmethod
    def get_band_file(scene_full_path, band_name):
        scene_full_path = L2AScene.get_scene_id_from_path(scene_full_path)

        if band_name not in L2AScene.BANDS:
            return None

        scene = os.path.basename(scene_full_path)
        if band_name != 'B6':
            return f'{scene}_SR_{band_name}.TIF'
        else:
            return f'{scene}_ST_{band_name}.TIF'

    @staticmethod
    def get_pixel_quality_file (scene_full_path):
        scene_full_path = L2AScene.get_scene_id_from_path(scene_full_path)
        return f'{os.path.basename(scene_full_path)}_QA_PIXEL.TIF'

    @staticmethod
    def get_cloud_mask_file (scene_full_path):
        scene_full_path = L2AScene.get_scene_id_from_path(scene_full_path)
        return f'{os.path.basename(scene_full_path)}_SR_CLOUD_QA.TIF'

    @staticmethod
    def transform_sr_values (raw_values):
        return np.minimum(np.maximum((0.275 * raw_values - 2000)*0.0001, 0),0.9)

    @staticmethod
    def calc_SR_values (band_file_full_path):
        return L2AScene.transform_sr_values(rproc.open_clipped_raster_as_image(band_file_full_path))

    @staticmethod
    def calc_valid_pixels_mask (cloud_qa_pixels, qa_pixels):
        valid_pixels = np.empty( (cloud_qa_pixels.shape[0], cloud_qa_pixels.shape[1], 1), dtype=np.uint8 )

        # cloud or shadow only of high confidence
        valid_pixels[:,:,0] = np.where( (qa_pixels == 1) | (qa_pixels & 768 == 768) | (qa_pixels & 3072 == 1) , 0, 1)

        # cloud or shadow of any confidence
        """
        valid_pixels[:,:,2] = (np.where((cloud_qa_pixels >> 1 & 1 == 1) | (cloud_qa_pixels >> 2 & 1 == 1)
                                       | (cloud_qa_pixels >> 3 & 1 == 1), 0, 1)
                                * np.where((qa_pixels == 1) | (qa_pixels >> 1 & 1 == 1) | (qa_pixels >> 3 & 1 == 1)
                                           | (qa_pixels >> 4 & 1 == 1), 0, 1)
                               )
        """
        return valid_pixels
