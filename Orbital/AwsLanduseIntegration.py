import boto3
import json
import pymongo
import sys
import analona
from dateutil import parser
from datetime import datetime, timezone

# Load configurations
config = json.load(open(sys.argv[1]))
bucket_name = config['bucketName']
access_key = config['accessKey']
secret_key = config['secretKey']
tile_id = config['tile']
mongo_uri = config['db']
start_time = parser.parse(config['startTime'])
end_time = parser.parse(config['endTime'])

# Initialize bucket
s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
bucket = s3.Bucket(bucket_name)

# Setup mongo
client = pymongo.MongoClient(mongo_uri)
db = client.louvre
ship_collection = db.ship
buildings_collection = db.buildings
roads_collection = db.roads
vegetation_collection = db.vegetation

def build_feature(feature, landuse_key, landuse_id, obj_start, obj_end):
    item = {
        '_id': landuse_id,
        'company': 'orbital',
        'geometry': feature['geometry'],
        'observed_start': datetime.utcfromtimestamp(int(round(obj_start.timestamp()))),
        'observed_end': datetime.utcfromtimestamp(int(round(obj_end.timestamp()))),
        'analyticsInfo': {
            'url': "s3://orbital-transfer-cricket/{}".format(landuse_key),
            'storage': 'AWS'
        }
    }
    return item

def do_dates_overlap(start1, start2, end1, end2):
    return max(start1, start2) <= min(end1, end2)

def do_tiles_overlap(tile_json, required_tile):
    if required_tile and not required_tile in tile_json:
        return False
    return True

def download_landuse_obj(obj, required_start, required_end, obj_start, obj_end, tile_json, required_tile):
    feature_collection = json.load(obj.get()['Body'])
    tile_id = tile_json.split('.json')[0]
    index = 0
    if not feature_collection['features']:
        return
    for feature in feature_collection['features']:
        item_type = feature['properties']['metadata']

        if 'valid_info' in item_type:
            continue

        landuse_id = "Orbital_{}_{}_{}".format(tile_id, item_type, index)
        item = build_feature(feature, obj.key, landuse_id, obj_start, obj_end)
        index += 1
        if 'roads' in item_type:
            is_valid = analona.Road(item).validate()
            if is_valid == True:
                roads_collection.replace_one({ '_id': item['_id'] }, item, upsert=True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        elif 'buildings' in item_type:
            is_valid = analona.Building(item).validate()
            if is_valid == True:
                buildings_collection.replace_one({ '_id': item['_id'] }, item, upsert=True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        elif 'forest' in item_type:
            is_valid = analona.Vegetation(item).validate()
            if is_valid == True:
                vegetation_collection.replace_one({ '_id': item['_id'] }, item, upsert=True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
        elif 'grass_ag' in item_type:
            is_valid = analona.Vegetation(item).validate()
            if is_valid == True:
                vegetation_collection.replace_one({ '_id': item['_id'] }, item, upsert=True)
            else:
                print("didn't pass verification- {}\nItem is {}".format(is_valid, item))

def get_objects_by_dates_and_tiles(required_start, required_end, required_tile=""):    
    for obj in bucket.objects.filter(Prefix='landuse'):
        _, _, date, tile_json = obj.key.split('/')
        start, end = date.split('|')
        obj_start = parser.parse(start).replace(tzinfo=timezone.utc)
        obj_end = parser.parse(end).replace(tzinfo=timezone.utc)
        dates_overlap = do_dates_overlap(required_start, obj_start, required_end, obj_end)
        tiles_overlap = do_tiles_overlap(tile_json, required_tile)
        is_json = ('.json' in obj.key)
        if (dates_overlap and tiles_overlap and is_json):
            download_landuse_obj(obj, required_start, required_end, obj_start, obj_end, tile_json, required_tile)     

get_objects_by_dates_and_tiles(start_time, end_time, tile_id)