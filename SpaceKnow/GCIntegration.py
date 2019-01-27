from google.cloud import storage
from google.oauth2 import service_account
import json
import sys
from os.path import abspath, dirname, join
import pymongo
import analona
from dateutil import parser
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians

# Load configurations
config = json.load(open(sys.argv[1]))
bucket_name = config['bucketName']
credentials_file = abspath(join(dirname(__file__), config['credentialsFile']))
project_name = config['projectName']
mongo_uri = config['db']
aoi_id = config['aoi']
tile_id = config['tile']
item_types = config['itemTypes']
start_date = datetime.strptime(config['start_date'], '%Y-%m-%dT%H:%M:%S.000Z')
end_date = datetime.strptime(config['end_date'], '%Y-%m-%dT%H:%M:%S.000Z')

scope_url = 'https://www.googleapis.com/auth/devstorage.full_control'

# Initialize the GC client
credentials = service_account.Credentials.from_service_account_file(credentials_file)
scoped_credentials = credentials.with_scopes([scope_url])
storage_client = storage.Client(project_name, scoped_credentials)

# Initiate the bucket
bucket = storage_client.get_bucket(bucket_name)
weekly_blobs_list = []
daily_blobs_list = []

if 'weekly' in item_types:
    if(aoi_id and tile_id):
        prefix = 'weekly/{}/{}'.format(aoi_id, tile_id)
    elif(aoi_id):
        prefix = 'weekly/{}'.format(aoi_id)
    else:
        prefix = 'weekly'
    weekly_blobs_list = list(bucket.list_blobs(prefix=prefix))

if 'daily' in item_types:
    if(aoi_id and tile_id):
        prefix = 'daily/{}/{}'.format(aoi_id, tile_id)
    elif(aoi_id):
        prefix = 'daily/{}'.format(aoi_id)
    else:
        prefix = 'daily'
    daily_blobs_list = list(bucket.list_blobs(prefix=prefix))

# Config db
client = pymongo.MongoClient(mongo_uri, connect=False)
db = client.louvre
ships_collection = db.ships
airplanes_collection = db.airplanes
buildings_collection = db.buildings
roads_collection = db.roads
vegetation_collection = db.vegetation

def parse_weekly_wrunc_to_mongo(wrunc_file, aoi, tile, week_string, date_string, record_file):
    features = wrunc_file['features']
    index = 1
        
    date_timestamp, _ = date_string.split('_')
    features_date = parser.parse(date_timestamp)
    
    original_images = [record_file['image']['sceneId']]
    
    for feature in features:
        item_class = feature['properties']['class']
        item = {}
        item['_id'] = 'SpaceKnow_{}_{}_{}_{}'.format(item_class, tile, week_string, index)
        index += 1
        item['company'] = 'SpaceKnow'
        item['geometry'] = feature['geometry']
        item['observed_start'] = datetime.utcfromtimestamp(int(round(features_date.timestamp())))
        item['observed_end'] = datetime.utcfromtimestamp(int(round(features_date.timestamp())))
        item['analyticsInfo'] = {
            'storage': 'GoogleCloud',
            'url': 'gs://{}/weekly/{}/{}/{}/{}'.format(bucket_name, aoi, tile, week_string, date_string)
        }
        item['tileId'] = record_file['tilePosition']
        item['sourceImagesIds'] = original_images
        
        if item_class == 'roads':
            is_valid = analona.Road(item).validate()
            if is_valid == True:
                roads_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        elif item_class == 'urban':
            is_valid = analona.Building(item).validate()            
            if is_valid == True:
                buildings_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        else:
            is_valid = analona.Building(item).validate() 
            if is_valid == True:
                vegetation_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))

if weekly_blobs_list:
    for blob in weekly_blobs_list: 
        if(blob.name.endswith('wrunc_detections.geojson')):
            prod_type, aoi, tile, week_string, date_string, file_type = blob.name.split('/')
            record_file = "{}/{}/{}/{}/{}/record.json".format(prod_type, aoi, tile, week_string, date_string)
            record_blob = bucket.blob(record_file)
            record_file = json.loads(record_blob.download_as_string())
            wrunc_file = json.loads(blob.download_as_string())
            if int(week_string) <= end_date.isocalendar()[1] and int(week_string) >= start_date.isocalendar()[1]:
                parse_weekly_wrunc_to_mongo(wrunc_file, aoi, tile, week_string, date_string, record_file)


# We should export this into a different rep in the future
# Approximate radius of earth in meters
Erad = 6373.0 * 1000

def distance(p1, p2):
    lon1 = radians(p1[0])
    lat1 = radians(p1[1])
    lon2 = radians(p2[0])
    lat2 = radians(p2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = Erad * c
    return round(distance)

def measurements(rectangle):
    d1 = distance(rectangle[0], rectangle[1])
    d2 = distance(rectangle[1], rectangle[2])

    if d1 < d2:
        return d1, d2
    else:
        return d2, d1


def parse_daily_to_mongo(detection_file, aoi, tile, day_date_string, specific_date_string, record_file):
    features = detection_file['features']
    index = 1
        
    date_timestamp, _ = specific_date_string.split('_')
    features_date = parser.parse(date_timestamp)
    
    original_image = record_file['image']['sceneId']
    
    for feature in features:
        item_class = feature['properties']['class']
        item = {}
        item['_id'] = 'SpaceKnow_{}_{}_{}_{}'.format(item_class, tile, day_date_string, index)
        index += 1
        item['company'] = 'SpaceKnow'
        item['geometry'] = feature['geometry']
        item['originalImageId'] = original_image
        item['observed_start'] = features_date
        item['observed_end'] = features_date
        item['area'] = feature['properties']['area']

        width, length = measurements(item['geometry']['coordinates'][0])
        item['width'] = width
        item['length'] = length
        
        if item_class == 'ships':
            is_valid = analona.Ship(item).validate()            
            if is_valid == True:
                ships_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        elif item_class == 'aircraft':
            is_valid = analona.Plane(item).validate()            
            if is_valid == True:
                airplanes_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        else: 
            print("unknown object type: {}".format(item_class))

if daily_blobs_list:
    for blob in daily_blobs_list:
        if(blob.name.endswith("detections.geojson")):
            file_type, aoi, tile, day_date, file_timestamp, file_name = blob.name.split('/')
            record_file = '{}/{}/{}/{}/{}/record.json'.format(file_type, aoi, tile, day_date, file_timestamp)
            record_blob = bucket.blob(record_file)
            record_file = json.loads(record_blob.download_as_string())
            detection_file = json.loads(blob.download_as_string())
            if datetime.strptime(day_date, '%Y%m%d') <= end_date and datetime.strptime(day_date, '%Y%m%d') >= start_date:
                parse_daily_to_mongo(detection_file, aoi, tile, day_date, file_timestamp, record_file)