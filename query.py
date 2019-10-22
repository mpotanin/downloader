"""
This script generates queries to USGS/SciHUB metedata services and writes response into csv file.
Query destination depends on input satellite type: 
    'Landsat 8' -> USGS service
    'Sentinel 2' -> SciHUB service
"""

import console_utils
import sys
import os
import datetime
import array
import json
import csv
from io import BytesIO
import requests



QUERY_ARGS_DEF = {
    '-u': 'username to query USGS/SciHub',
    '-p': 'password to query USGS/SciHub',
    '-b': 'border geojson file with polygon/multipolygon geometry',
    '-sat': 'platform: s2|l8',
    '-sd': 'start date yyyy-mm-dd',
    '-ed': 'end date yyyy-mm-dd',
    '-cld': 'max cloud filter',
    '-o': 'output csv file name'
}

QUERY_USAGE_EXAMPLES = ("query.py -u user:password -b 1.geojson -p s2 -sd 20190501 -ed 20191001 -cld 50 -o 1.csv\n")


class BBOX :
    """
    Represents bounding box rectangular.
    """
    def __init__(self,minx=1e+100,miny=1e+100,maxx=-1e+100,maxy=-1e+100) :
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy
    def is_undefined (self) :
        if (self.minx>self.maxx) : return True
        else : return False


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
    def create_csv_file (metadata_list, csv_file) :
        """Creates csv file from metadata list."""
        try :
            with open (csv_file, 'w', newline='') as file :
                writer = csv.DictWriter(file, fieldnames=MetadataEntity.get_fieldnames())
                writer.writeheader()
                for e in metadata_list :
                    writer.writerow(e.get_values())
                file.close()
        except :
            print ('ERROR: writing csv file: ' + csv_file)
            return False
        return True


class GeoJSONParser :
    
    @staticmethod
    def calculate_bbox (geom_json) :
        
        if not "type" in geom_json : return BBOX()

        gtype = geom_json["type"]
        if (gtype == 'Point') :
            return BBOX(geom_json["coordinates"][0],
                        geom_json["coordinates"][1],
                        geom_json["coordinates"][0] + 1e-4,
                        geom_json["coordinates"][1] + + 1e-4)
        elif (gtype == 'Polygon' or gtype == 'MultiPolygon' ) :
            outline_ring = (geom_json["coordinates"][0] 
                            if gtype == 'Polygon' else geom_json["coordinates"][0][0])
            bbox = BBOX()
            for p in outline_ring :
                if p[0]<bbox.minx : bbox.minx=p[0]
                if p[0]>bbox.maxx : bbox.maxx=p[0]
                if p[1]<bbox.miny : bbox.miny=p[1]
                if p[1]>bbox.maxy : bbox.maxy=p[1]
            return bbox
        else : return BBOX()
            
            
    
    @staticmethod
    def extract_geometry_from_geojson (geojson_file) :
        """
        Parse geometry type and coordinates from geojson into memory
        Returns: json Dictionary object 
        """
        try:
            with open (geojson_file, 'r') as file :
                data_from_file=file.read()
                json_obj = json.loads(data_from_file)
                return json_obj["features"][0]["geometry"]
        except:
            print ("ERROR: can't extract geometry from file: " + geojson_file)
            return dict()

    

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
    def __extract_cloudcover (raw_entity) :
        return float(raw_entity["double"]["content"])

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
    def __compose_q_param (geojson_file, stardate, enddate, cloud_max) :
        bbox = GeoJSONParser.calculate_bbox(
                GeoJSONParser.extract_geometry_from_geojson(geojson_file))
        if bbox.is_undefined() : 
            return ''
        else :
            q_param= 'footprint:' + SciHubMetadataExtractor.__convert_bbox_to_wktpolygon(bbox)
            q_param+=' AND platformName:Sentinel-2 AND productType:S2MSI1C'
            startdate_txt = startdate.strftime('%Y-%m-%dT%H:%M:%S.00Z')
            enddate_txt = enddate.strftime('%Y-%m-%dT%H:%M:%S.00Z')

            q_param+=' AND endPosition:[' + startdate_txt + ' TO ' + enddate_txt + ']'
            q_param+=' AND beginPosition:[' + startdate_txt + ' TO ' + enddate_txt + ']'
            
            #endPosition:[2017-02-01T00:00:00.000Z TO 2019-08-07T12:23:31.000Z] 
            #beginPosition:[2017-02-01T00:00:00.000Z TO 2019-08-07T12:23:31.000Z]
            
            return q_param
    
    def retrieve_all (self, user, pwd, geojson_file, startdate, enddate, cloud_max) :
        """
        main method that queries SciHUB service and saves metadata into in memory list
        """
        q_param = (SciHubMetadataExtractor.
                    __compose_q_param(geojson_file,startdate,enddate,cloud_max))
        if (q_param=='') :
            print ("ERROR: can't compose 'q' parameter")
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
            raw_entities = json_response["feed"]["entry"]
            for re in raw_entities :
                if (SciHubMetadataExtractor.__extract_cloudcover(re) <= cloud_max) :
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

    def retrieve_all (self, user, pwd, geojson_file, stardate, enddate, cloud_max) :
        if not self.__login(user,pwd) :
            print ('ERROR: authorization failed')
            return ''
        bbox = GeoJSONParser.calculate_bbox(
                GeoJSONParser.extract_geometry_from_geojson(geojson_file))
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

def init_console_args_from_json_file (args_def, json_file) :
    args = list()
    try:
        with open (json_file, 'r') as file :
            data_from_file=file.read()
            json_obj = json.loads(data_from_file)
            args.append(sys.argv[0])
            for opt in json_obj:
                args.append(opt)
                args.append(json_obj[opt])
    except:
        print ('ERROR: parsing json file: ' + json_file)
        return list()
    return args



#################################################################################################
# 
# 1. Parse input args
# 2. Create MetadataExtractor (SciHubMetadataExtractor | USGSMetadataExtractor)
# 3. Query metadata service and save it into in-memory list 
# 4. Save in-memory list to disk as csv file
#
#################################################################################################

if (len(sys.argv) == 1) :
    console_utils.print_usage(QUERY_ARGS_DEF)
    exit(0)


read_args_from_file = False
json_file_params = 's2_query_param.json'


args = ( sys.argv if not read_args_from_file
        else console_utils.parse_args_from_json_file(json_file_params))

if not console_utils.check_input_args(QUERY_ARGS_DEF,args) :
    print ('ERROR: not valid input args')
    exit(1)

user = console_utils.get_option_value(args,'-u')
pwd = console_utils.get_option_value(args,'-p')
startdate = datetime.datetime.strptime(console_utils.get_option_value(args,'-sd'),
                                        '%Y-%m-%d')
enddate = datetime.datetime.strptime(console_utils.get_option_value(args,'-ed'),
                                        '%Y-%m-%d')
cloud_max = int(console_utils.get_option_value(args,'-cld'))
geojson_file = console_utils.get_option_value(args,'-b')

platform = console_utils.get_option_value(args,'-sat')
csv_file = console_utils.get_option_value(args,'-o')


list_metadata = (SciHubMetadataExtractor().retrieve_all(
                            user,pwd,geojson_file,startdate,enddate,cloud_max)
                    if (platform == 's2') else 
                USGSMetadataExtractor().retrieve_all(
                            user,pwd,geojson_file,startdate,enddate,cloud_max))
MetadataOperations.create_csv_file(list_metadata,csv_file)


print (platform + ': '  + str(len(list_metadata)))
#print ('ERROR: SciHubMetadataExtractor:retrieve_all\n')




