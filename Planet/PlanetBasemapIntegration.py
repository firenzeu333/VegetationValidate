import os
import json
import sys
import shutil
from datetime import datetime
import pymongo
import re

from planet_downloader.basemap import basemap

from itertools import product
import rasterio as rio
from rasterio import windows
import rasterio.features
import rasterio.warp

import analona

planet_api_key = os.environ['PL_API_KEY']

# Setup current configuration
config = json.load(open(sys.argv[1]))
download_directory = config['download_directory']
basemap_types = ['buildings']
start_date = datetime.strptime(config['start_date'].split('T')[0], "%Y-%m-%d") 
end_date = datetime.strptime(config['end_date'].split('T')[0], "%Y-%m-%d") 
bbox = config['bbox']
mongo_uri = config['db']

# Setup db 
client = pymongo.MongoClient(mongo_uri)
db = client.louvre
buildings_collection = db.buildings

def get_tiles(img, width=256, height=256):
    ncols, nrows = img.meta['width'], img.meta['height']
    offsets = product(range(0, ncols, width), range(0, nrows, height))
    big_window = windows.Window(col_off=0, row_off=0, width=ncols, height=nrows)
    for col_off, row_off in  offsets:
        window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, img.transform)
        yield window, transform


def split_file(filepath, folder):
    if os.path.exists(folder):
        print("already split {}".format(filepath.split('/')[1]))
        return
    
    os.mkdir(folder)
    with rio.open(filepath) as img:
        meta = img.meta.copy()
        for window, transform in get_tiles(img):
            meta['transform'] = transform
            meta['width'], meta['height'] = window.width, window.height
            outpath = os.path.join(folder,'{}_{}'.format(int(window.col_off), int(window.row_off)))
            with rio.open(outpath, 'w', **meta) as out_imgs:
                out_imgs.write(img.read(window=window))


def raster_to_geojson(filepath, mosaic_start_date, mosaic_end_date, url):
    filepath_parts = filepath.split('/')
    folder = 'tmp_for_{}'.format(filepath_parts[len(filepath_parts) - 1])
    
    # The first thing we do is split the original file
    split_file(filepath, folder)
    
    # Now the folder should contain all splited tiles.
    for _, _, files in os.walk(folder):  
        for filename in files:
            splitted_filepath = os.path.join(folder, filename)
            with rio.open(splitted_filepath) as dataset:
                _, _, image_id = filepath.split('/')
                image_id = image_id.split('.')[0]
                
                # Init a counter for different IDs
                counter = 0
                # RGB Raster
                if dataset.count == 4:
                    # Read the dataset's valid data mask as a ndarray.
                    mask = dataset.read(3)
                # BW Raster
                elif dataset.count == 2:
                    mask = dataset.read(1)
                    
                shp = rio.features.shapes(mask, transform=dataset.transform)
                # Extract feature shapes and values from the array.
                for geom, _ in shp:
                    item = {}
                    
                    # Transform shapes from the dataset's own coordinate
                    # reference system to CRS84 (EPSG:4326).
                    geom = rio.warp.transform_geom(dataset.crs, 'EPSG:4326', geom, precision=6)

                    item['_id'] = 'planet_buildings_{}_{}_{}'.format(image_id, filename, counter)
                    item['geometry'] = geom
                    item['company'] = 'Planet'
                    item['observed_start'] = datetime.utcfromtimestamp(int(round(mosaic_start_date.timestamp())))
                    item['observed_end'] = datetime.utcfromtimestamp(int(round(mosaic_end_date.timestamp())))
                    item['analyticsInfo'] = {
                        'url': url,
                        'storage': 'Planet'
                    }
                    item['sourceImagesIds'] = [image_id]
                    is_valid = analona.Building(item).validate()
                    counter += 1
                    if is_valid == True:
                        buildings_collection.replace_one({ '_id': item['_id'] }, item, upsert = True)
                    else:
                        print("didn't pass verification- {}\nItem is {}".format(is_valid, item))
    # Remove the folder of tiles
    shutil.rmtree(folder)
    # Remove the image because we don't need it anymore
    os.remove(filepath)

def download_quad(session, mosaic_name, mosaic_start_date, mosaic_end_date, quad):
    directory = ''
    url = quad['_links']['download']
    if url:
        directory = '{}/{}'.format(str(download_directory),\
                                    str(mosaic_name))

        if not os.path.exists(directory):
            os.makedirs(directory)

        res = session.get(url, stream = True)

        if res.status_code != 200:
            raise Exception('Error while getting quads: ' + res.text)

        filename = str(re.findall('filename=(.+)', str(res.headers.get('Content-Disposition')))[0])

        filepath = '{}/{}'.format(str(directory), filename)

        print('downloading to:', filepath)
        with open(filepath, 'wb') as f:
            for chunk in res.iter_content(chunk_size = 1024 * 1024):
                if chunk:
                    f.write(chunk)

        # After we download the file locally, we can transform it and insert to louvre
        raster_to_geojson(filepath, mosaic_start_date, mosaic_end_date, url)        
    else:
        print('no available download')


def handle_quads_batch(session, mosaic_name, mosaic_start_date, mosaic_end_date, quads):
    quads_items = quads['items']
    for item in quads_items:        
        download_quad(session, mosaic_name, mosaic_start_date, mosaic_end_date, item)


def fetch_quads(session, mosaic_name, mosaic_start_date, mosaic_end_date, quad_items, download_directory):
    if not quad_items:
        return
    handle_quads_batch(session, mosaic_name, mosaic_start_date, mosaic_end_date, quad_items)
    
    next_quads = basemap.get_next_quads_list(session, quad_items['_links'])
    if next_quads:
        fetch_quads(session, mosaic_name, mosaic_start_date, mosaic_end_date, next_quads, download_directory)
    

if not os.path.exists(download_directory):
    os.mkdir(download_directory)

session = basemap.create_session(planet_api_key)
mosaics_list = basemap.get_mosaics_to_download(session, basemap_types)

for mosaic in mosaics_list:
    quads = basemap.get_initial_quads_list(session, mosaic, bbox, start_date, end_date)
    mosaic_start_date = datetime.strptime(mosaic['first_acquired'].split('T')[0], '%Y-%m-%d')
    mosaic_end_date =  datetime.strptime(mosaic['last_acquired'].split('T')[0], '%Y-%m-%d')
    fetch_quads(session, mosaic["name"], mosaic_start_date, mosaic_end_date, quads, download_directory)