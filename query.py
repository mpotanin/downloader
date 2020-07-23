"""
This script generates queries to USGS/SciHUB metedata services and writes response into csv file.
Query destination depends on input satellite type: 
    'Landsat 8' -> USGS service
    'Sentinel 2' -> SciHUB service
"""

import argparse
import sys
import os
import datetime
import array
import json
import csv
from io import BytesIO
import requests
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

from common_utils import vector_operations as vop




class MetadataEntity :
    """
    Single scene metadata record. Only main fields are included.
    """
    def __init__(self,platform,sceneid,productid,acqdate,tileid) :
        self.platform = platform    # 'Sentinel 2' | 'Landsat 8'
        self.sceneid = sceneid      
        self.productid = productid
        self.acqdate = acqdate
        self.tileid = tileid

    @staticmethod
    def get_fieldnames () :
        return ['platform','sceneid','productid','acqdate','tileid']

    
    def get_values (self) :
        return {'platform':self.platform,
                'sceneid':self.sceneid,
                'productid':self.productid,
                'acqdate':self.acqdate.strftime("%Y-%m-%d %H:%M:%S"),
                'tileid':self.tileid}
    
    

class MetadataOperations:
    @staticmethod
    def write_csv_file (metadata_list, csv_file, append) :
        """Creates csv file from metadata list."""
        try :
            with open (csv_file, 'a' if append else 'w' , newline='') as file :
                writer = csv.DictWriter(file, fieldnames=MetadataEntity.get_fieldnames())
                if not append: writer.writeheader()
                for e in metadata_list :
                    writer.writerow(e.get_values())
                file.close()
        except :
            print ('ERROR: writing csv file: ' + csv_file)
            return False
        return True


class SciHubMetadataExtractor :
    base_url = 'https://scihub.copernicus.eu/dhus/search?format=json'
    page_num = 100
    
    @staticmethod
    def __convert_bbox_to_wktpolygon (bbox) :
        wkt_poly = '"Intersects(POLYGON(('
        wkt_poly+= str(bbox.minx) + ' ' + str(bbox.miny) + ','
        wkt_poly+= str(bbox.minx) + ' ' + str(bbox.maxy) + ','
        wkt_poly+= str(bbox.maxx) + ' ' + str(bbox.maxy) + ','
        wkt_poly+= str(bbox.maxx) + ' ' + str(bbox.miny) + ','
        wkt_poly+= str(bbox.minx) + ' ' + str(bbox.miny) + ')))"'
        return wkt_poly

    @staticmethod
    def __convert_raw_entity (raw_entity) :
        sceneid = raw_entity["title"]
        productid = raw_entity["id"]
        acqdate = datetime.datetime.strptime(raw_entity["date"][1]['content'][0:10],
                                                '%Y-%m-%d')
        platform = 'Sentinel 2'
        #tileid = [e["content"] for e in raw_entity["str"] if e["name"] == 'tileid'][0]

        tileid = sceneid[38:44]
        return MetadataEntity(platform,sceneid,productid,acqdate,tileid)
    
    @staticmethod
    def __compose_q_param (vector_file, tiles, product, stardate, enddate, cloud_max) :
    
        if (vector_file is None) and (tiles is None) : 
            print('ERROR: AOI isn\'t defined, \"tiles" or "vector_file" have to be specified')
            return ''

        q_param = 'platformName:Sentinel-2'

        startdate_txt = startdate.strftime('%Y-%m-%dT%H:%M:%S.00Z')
        enddate_txt = enddate.strftime('%Y-%m-%dT23:59:59.999Z')
        q_param+=' AND endPosition:[' + startdate_txt + ' TO ' + enddate_txt + ']'
            
        if vector_file is not None:
            bbox = vop.BBOX.calc_BBOX_from_vector_file(vector_file)
            if bbox.is_undefined() : 
                print ('ERROR: not valid geojson file, can\'t parse geometry')
                return ''
            else :
                q_param+= ' AND footprint:' + SciHubMetadataExtractor.__convert_bbox_to_wktpolygon(bbox)
            
        if product is not None:
            if (product.lower()=='l2') :
                q_param+=' AND productType:S2MSI2A'
            else:
                q_param+=' AND productType:S2MSI1C'
        
        if cloud_max is not None:
            q_param+=' AND cloudcoverpercentage:[0 TO ' + str(cloud_max) + ']'

        if tiles is not None:
            q_param+=' AND ('
            for tile in tiles:
                q_param+='filename:*' + tile + '* OR '
            q_param= q_param[:-3] + ')'

        return q_param
    
    def retrieve_all (self, user, pwd, vector_file, tiles, product, startdate, enddate, cloud_max) :
        """
        main method that queries SciHUB service and saves metadata into in memory list
        """
        q_param = (SciHubMetadataExtractor.
                    __compose_q_param(vector_file,tiles, product,startdate,enddate,cloud_max))
        if (q_param=='') :
            print ("ERROR: can't compose query string")
            return list()

        start = 0
        list_result = list()
        while True :
            query_base = SciHubMetadataExtractor.base_url
            query_base+='&start='+str(start) + '&rows='+str(SciHubMetadataExtractor.page_num)
            r = requests.post(query_base,{"q":q_param},auth=(user,pwd))
            if (r.status_code!=200) :
                print ('ERROR: ' + str(r.status_code))
                return ''
            json_response = json.loads(r.text)
            total = int(json_response["feed"]["opensearch:totalResults"])
            if (total == 0) :
                return list_result
            
            raw_entities = json_response["feed"]["entry"]
            for re in raw_entities :
                list_result.append(SciHubMetadataExtractor.__convert_raw_entity(re)) 
            
            if (start + SciHubMetadataExtractor.page_num >= total) :
                break
            else :
                start+=SciHubMetadataExtractor.page_num
            
        return list_result
    

class USGSMetadataExtractor :
    base_url = 'https://earthexplorer.usgs.gov/inventory/json/v/1.4.0/'
    page_num = 10

    def __login (self,user,pwd) :
        json_body = {
                        "username": user,
                        "password": pwd,
                        "authType": "EROS",
                        "catalogId": "EE"}
       
        post_param = {"jsonRequest":json.dumps(json_body)}
        r = requests.post(USGSMetadataExtractor.base_url + 'login',post_param)
        if (r.status_code != 200) :
            print ('ERROR login: ' + str(r.status_code))
            return False
        else :
            r_body = json.loads (r.text)
            if (r_body["errorCode"]) :
                print ('ERROR login: ' + r_body["errorCode"])
                return False
            else :
                self.apikey = r_body["data"]
                return True
        
    @staticmethod
    def __convert_raw_entity (raw_entity) :
        sceneid = raw_entity["entityId"]
        productid = raw_entity["displayId"]
        acqdate = datetime.datetime.strptime(raw_entity["acquisitionDate"],'%Y-%m-%d')
        platform = 'Landsat 8'
        tileid = productid[10:16]

        return MetadataEntity(platform,sceneid,productid,acqdate,tileid)

    def retrieve_all (self, user, pwd, vector_file, stardate, enddate, cloud_max) :
        if not self.__login(user,pwd) :
            print ('ERROR: authorization failed')
            return ''

        if (vector_file is None):
            print('ERROR: AOI isn\'t specified')
            return ''

        bbox = vop.BBOX.calc_BBOX_from_vector_file(vector_file)

        if bbox.is_undefined() : return list()

        request_body = dict()
        request_body["datasetName"] = "LANDSAT_8_C1"
        request_body["spatialFilter"] = {"filterType": "mbr"}
        request_body["spatialFilter"]["lowerLeft"] = {"longitude":bbox.minx,
                                                        "latitude":bbox.miny}
        request_body["spatialFilter"]["upperRight"] = {"longitude":bbox.maxx,
                                                        "latitude":bbox.maxy}
        request_body["temporalFilter"] = {
                "startDate":startdate.strftime('%Y-%m-%d'),
                "endDate":enddate.strftime('%Y-%m-%d')}
        request_body["apiKey"] = self.apikey
        request_body["maxCloudCover"] = cloud_max

        starting_number = 1
        list_result = list()
        while True :
            request_body["startingNumber"] = starting_number
            r = requests.post(USGSMetadataExtractor.base_url + 'search',
                                {"jsonRequest":json.dumps(request_body)} )
            if (r.status_code != 200) :
                print ('ERROR search: ' + str(r.status_code))
                return False
            else :
                r_body = json.loads (r.text)
                if (r_body["errorCode"]) :
                    print ('ERROR search: ' + r_body["errorCode"])
                    return False
                else :
                    if not "results" in r_body["data"] : break
                    for re in r_body["data"]["results"] :
                        list_result.append(USGSMetadataExtractor.__convert_raw_entity(re))
                    starting_number+=USGSMetadataExtractor.page_num
                    if (starting_number>r_body["data"]["totalHits"]) : break
                    
        return list_result

#################################################################################################
# main:
# 1. Parse input args
# 2. Create MetadataExtractor (SciHubMetadataExtractor | USGSMetadataExtractor)
# 3. Query metadata service and save it into in-memory list 
# 4. Save in-memory list to disk as csv file
#
#################################################################################################


parser = argparse.ArgumentParser(description=
    ('This script generates queries to USGS/SciHUB metedata services and writes '
    'response into csv file. Query destination depends on input satellite type: '
    'Landsat 8 -> USGS service,  Sentinel 2 -> SciHUB service.'))


parser.add_argument('-u', required=True, metavar='user', 
                    help = 'Username to query USGS/SciHub')
parser.add_argument('-p', required=True, metavar='pwd',
                    help='Password to query USGS/SciHub')
parser.add_argument('-b', required=False, metavar='vector AOI', 
                    help = 'Border geojson/shp file with polygon/multipolygon geometry(ies) EPSG:4326')
parser.add_argument('-sat', required=True, metavar='s2|l8',  
                    help= 'Platform: Sentinel 2 (s2) or Landsat 8 (l8)')
parser.add_argument('-sd', required=True, metavar='start date yyyy-mm-dd',
                     help='Start date yyyy-mm-dd')
parser.add_argument('-ed', required=True, metavar='end date yyyy-mm-dd',
                     help='End date yyyy-mm-dd')
parser.add_argument('-cld', type=int, required=False, metavar='cloud max cover',
                     help='Start date cloud max cover')
parser.add_argument('-o', required=True, metavar='output file',
                     help='Output csv file')
parser.add_argument('-a', help='Append to existing csv file', action='store_true')
parser.add_argument('-prod', metavar='L2/L1', help='Product type')
parser.add_argument('-tiles',metavar='tiles list', help='Tiles list comma separated')


if (len(sys.argv) == 1) :
    parser.print_usage()
    exit(0)
args = parser.parse_args()


#args = parser.parse_args('-i /ext/Calculate/L2A/2018 -o /ext/Calculate/L4A/4tiles.txt -tids 39VVC 39UUB 39UVB 39VUC'.split(' '))

startdate = datetime.datetime.strptime(args.sd,'%Y-%m-%d')
enddate = datetime.datetime.strptime(args.ed,'%Y-%m-%d')
tiles = None
if args.tiles is not None:
    tiles = args.tiles.split(',')

list_metadata = list()

if (args.sat == 's2') :
    list_metadata = SciHubMetadataExtractor().retrieve_all(
                                                            user=args.u,
                                                            pwd=args.p,
                                                            vector_file=args.b,
                                                            startdate=startdate,
                                                            enddate=enddate,
                                                            cloud_max=args.cld,
                                                            tiles=tiles,
                                                            product=args.prod)
else :
    list_metadata = USGSMetadataExtractor().retrieve_all(args.u,
                                                        args.p,
                                                        args.b,
                                                        startdate,
                                                        enddate,
                                                        args.cld)

MetadataOperations.write_csv_file(list_metadata,args.o,args.a)


print (args.sat + ': '  + str(len(list_metadata)))




