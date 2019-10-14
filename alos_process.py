#!/usr/bin/env python3

import logging
import os
import subprocess
import shutil

from osgeo import gdal

# from ruamel.yaml import YAML

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command
    """
    subprocess.check_call(command, cwd=work_dir)


def make_directories(directories):
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)


def delete_directories(directories):
    for directory in directories:
        for the_file in os.listdir(directory):
            a_file = os.path.join(directory, the_file)
            if os.path.isfile(a_file):
                logging.debug("Deleting file: {}".format(a_file))
                os.unlink(a_file)
            elif os.path.isdir(a_file):
                logging.debug("Deleting directory: {}".format(a_file))
                shutil.rmtree(a_file)


def download_files(WORKDIR, OUTDIR, YEAR, TILE):
    filename = "{}_{}_MOS_F02DAR.tar.gz".format(TILE, YEAR[-2:])
    logging.info("Downloading file: {}".format(filename))
    ftp_location = "ftp://ftp.eorc.jaxa.jp/pub/ALOS-2/ext1/PALSAR-2_MSC/25m_MSC/{}/{}".format(
        YEAR, filename
    )
    tar_file = os.path.join(WORKDIR, filename)

    try:
        if not os.path.exists(tar_file):
            run_command(['wget', ftp_location], WORKDIR)
        else:
            logging.info("Skipping download, file already exists")
        make_directories([os.path.join(OUTDIR, TILE)])
        logging.info("Untarring file")
        run_command(['tar', '-xvf', filename], WORKDIR)
    except subprocess.CalledProcessError:
        print('File does not exist')


def combine_cog(PATH, OUTPATH, TILE):
    logging.info("Combining GeoTIFFs")
    bands = ['HH', 'HV', 'linci', 'date', 'mask']

    gtiff_abs_path = os.path.abspath(PATH)
    outtiff_abs_path = os.path.abspath(OUTPATH)

    for band in bands:
        # Find all the files
        all_files = []
        for path, subdirs, files in os.walk(gtiff_abs_path):
            for fname in files:
                if '_{}_'.format(band) in fname and not fname.endswith('.hdr'):
                    in_filename = os.path.join(path, fname)
                    all_files.append(in_filename)

        # Create the VRT
        logging.info("Building VRT for {} with {} files found".format(
            band, len(all_files)))
        vrt_path = os.path.join(gtiff_abs_path, '{}.vrt'.format(band))
        vrt_options = gdal.BuildVRTOptions()
        gdal.BuildVRT(
            vrt_path,
            all_files,
            options=vrt_options
        )

        run_command([
            'rio',
            'cogeo',
            'create',
            '--nodata',
            '0',
            vrt_path,
            '{}/{}_{}.tif'.format(outtiff_abs_path, TILE, band)
        ],
            gtiff_abs_path
        )


def write_yaml(OUTDIR, YEAR, TILE):
    logging.warning("Write_yaml not implemented.")
    return None


def upload_to_s3(OUTDIR, S3_DESTINATION):
    logging.warning("Upload to S3 not yet complete")
    if S3_DESTINATION:
        logging.info("Uploading to {}".format(S3_DESTINATION))
    else:
        logging.warning("Not uploading to S3, because the bucket isn't set.")


def run_one(TILE_STRING, WORKDIR, OUTDIR, S3_DESTINATION):
    YEAR = TILE_STRING.split('/')[0]
    TILE = TILE_STRING.split('/')[1]

    try:
        make_directories([WORKDIR, OUTDIR])
        download_files(WORKDIR, OUTDIR, YEAR, TILE)
        combine_cog(WORKDIR, OUTDIR, TILE)
        write_yaml(OUTDIR, YEAR, TILE)
        upload_to_s3(OUTDIR, S3_DESTINATION)
        delete_directories([WORKDIR, OUTDIR])
        # Assume job success here
        return True
    except Exception as e:
        logging.error("Job failed with error {}".format(e))
        return False


if __name__ == "__main__":
    logging.info("Starting default process")
    TILE_STRING = '2017/N00E030'
    S3_DESTINATION = None
    WORKDIR = 'data/download'
    OUTDIR = 'data/out'

    run_one(TILE_STRING, WORKDIR, OUTDIR, S3_DESTINATION)
