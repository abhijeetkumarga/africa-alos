import logging
import os
import shutil
import subprocess
import yaml

import boto3
from osgeo import gdal

import rasterio
from get_uuid import odc_uuid

def get_ref_points(OUTDIR, YEAR, TILE):
    datasetpath = os.path.join(OUTDIR, '{}/{}_date.tif'.format(YEAR, TILE))
    dataset = rasterio.open(datasetpath)
    trans = dataset.transform*(0, 0)
    return {
        'll': {'x': trans[0], 'y': trans[1]-5},
        'lr': {'x': trans[0]+5, 'y': trans[1]-5},
        'ul': {'x': trans[0], 'y': trans[1]},
        'ur': {'x': trans[0]+5, 'y': trans[1]},
    }

def get_coords(OUTDIR, YEAR, TILE):
    datasetpath = os.path.join(OUTDIR, '{}/{}_date.tif'.format(YEAR, TILE))
    dataset = rasterio.open(datasetpath)
    trans = dataset.transform*(0, 0)
    return {
        'll': {'lat': trans[1]-5, 'lon': trans[0]},
        'lr': {'lat': trans[1]-5, 'lon': trans[0]+5},
        'ul': {'lat': trans[1], 'lon': trans[0]},
        'ur': {'lat': trans[1], 'lon': trans[0]+5},
    }


def write_yaml(OUTDIR, YEAR, TILE):
    logging.warning("Write_yaml not implemented.")
    yaml_filename = os.path.join(OUTDIR, "{}_{}.yaml".format(TILE, YEAR))
    geo_ref_points = get_ref_points(OUTDIR, YEAR, TILE)
    coords = get_coords(OUTDIR, YEAR, TILE)
    today = datetime.datetime.today()
    format = "%Y-%m-%dT%H:%M:%S"
    creation_date = today.strftime(format)
    metadata_doc = {
        'id': str(odc_uuid('alos', '1', [], YEAR=YEAR,TILE=TILE)),
        'creation_dt': creation_date,
        'product_type': 'gamma0',
        'platform': {'code': 'ALOS'},
        'instrument': {'name': 'PALSAR'},
        'format': {'name': 'GeoTIFF'}
        'extent': {
            'coord': coords,
            'from_dt': "{}-01-01T00:00:01".format(YEAR),
            'center_dt': "{}-06-15T11:00:00".format(YEAR),
            'to_dt': "{}-12-31T23:59:59".format(YEAR),
                  },
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': 'EPSG:4326',
                            }
                        },
        'image': {
            'bands': {
                'HH': {
                    'path': "{}_HH.tif".format(TILE),
                    } 
                'HV': {
                    'path': "{}_HV.tif".format(TILE),
                    }
                'linci': {
                    'path': "{}_linci.tif".format(TILE),
                    }
                'mask': {
                    'path': "{}_mask.tif".format(TILE),
                    }
                'date': {
                    'date': "{}_date.tif".format(TILE),
                    }
            }
        },
        'lineage': {'source_datasets': {}},
    }

    with open(yaml_filename, 'w') as f:
        yaml = YAML(typ='safe', pure=False)
        yaml.default_flow_style = False
        yaml.dump(metadata_doc, f)

    # Note that this needs to return a file path to the metadata
    return yaml_filename





