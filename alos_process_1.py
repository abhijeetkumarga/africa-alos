import os
import subprocess
from os.path import basename, dirname
from os.path import join as pjoin
import copy
from ruamel.yaml import YAML
#from filenames import final_mesh


def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command
    """
    subprocess.check_call(command, cwd=work_dir)


def make_directories(year, WORKDIR):
    run_command(['mkdir', year], WORKDIR)
    run_command(['mkdir', year], OUTDIR)
    PATH = '{}/{}'.format(WORKDIR, year)
    OUTPATH = '{}/{}'.format(OUTDIR, year)
    return PATH, OUTPATH


def download_files(year, final_mesh, WORKDIR, OUTDIR, PATH, OUTPATH):
    for j, l_j in enumerate(final_mesh[0:3]):
        fileactual = l_j
        filename = "ftp://ftp.eorc.jaxa.jp/pub/ALOS-2/ext1/PALSAR-2_MSC/25m_MSC/{}/{}_{}_MOS_F02DAR.tar.gz".format(year, fileactual, year[-2:])
        local_file = "{}_{}_MOS_F02DAR.tar.gz".format(fileactual, year[-2:])
        try:
            run_command(['wget', filename], PATH)
            download_mesh.append(fileactual)
            run_command(['mkdir', fileactual], PATH)
            run_command(['mkdir', fileactual], OUTPATH)
            run_command(['tar', '-xvf', local_file, '-C', fileactual], PATH)
            run_command(['rm', local_file], PATH)
            combine_cog(PATH,OUTPATH,year,fileactual)
        except subprocess.CalledProcessError:
            print('File does not exist')


def combine_cog(PATH,OUTPATH,year,fileactual):

    hhfname = []
    hvfname = []
    lincifname = []
    maskfname = []
    datefname = []

    gtiff_path = os.path.join(PATH,fileactual)
    gtiff_abs_path = os.path.abspath(gtiff_path)
    
    outtiff_path = os.path.join(OUTPATH,fileactual)
    outtiff_abs_path = os.path.abspath(outtiff_path)


    for path,subdirs,files in os.walk(gtiff_abs_path):
        for fname in files:
            if '_HH_' in fname and not fname.endswith('.hdr'):
                in_filename = os.path.join(path,fname)
                hhfname.append(in_filename)
            if '_HV_' in fname and not fname.endswith('.hdr'):
                in_filename = os.path.join(path,fname)
                hvfname.append(fname)
            if '_linci_' in fname and not fname.endswith('.hdr'):
                in_filename = os.path.join(path,fname)
                lincifname.append(fname)
            if '_mask_' in fname and not fname.endswith('.hdr'):
                in_filename = os.path.join(path,fname)
                maskfname.append(fname)
            if '_date_' in fname and not fname.endswith('.hdr'):
                in_filename = os.path.join(path,fname)
                datefname.append(fname)

    filehh = open('{}/{}/filehh.txt'.format(PATH, fileactual), "w")
    filehh.writelines(["%s\n" % item for item in hhfname])
    filehh.close()

    filehv = open('{}/{}/filehv.txt'.format(PATH, fileactual), "w")
    filehv.writelines(["%s\n" % item for item in hvfname])
    filehv.close()

    filelinci = open('{}/{}/filelinci.txt'.format(PATH, fileactual), "w")
    filelinci.writelines(["%s\n" % item for item in lincifname])
    filelinci.close()

    filedate = open('{}/{}/filedate.txt'.format(PATH, fileactual), "w")
    filedate.writelines(["%s\n" % item for item in datefname])
    filedate.close()

    filemask = open('{}/{}/filemask.txt'.format(PATH, fileactual), "w")
    filemask.writelines(["%s\n" % item for item in maskfname])
    filemask.close()

    bands = ['hh', 'hv', 'linci', 'date', 'mask']

    for band in bands:
        run_command(['gdalbuildvrt','-input_file_list','{}/file{}.txt'.format(gtiff_abs_path,fileactual,band),'{}/{}_{}.vrt'.format(gtiff_abs_path,fileactual,band)],gtiff_abs_path)
        run_command(['rio','cogeo','create','--nodata','0','{}/{}_{}.vrt'.format(gtiff_abs_path,fileactual,band),'{}/{}_{}.tif'.format(outtiff_abs_path,fileactual,band)],gtiff_abs_path)

def write_yaml


final_mesh = ['N00E030', 'N00E035']
WORKDIR = 'data/download/'
OUTDIR = 'data/out/'
year = '2017'
download_mesh = []

make_directories(year, WORKDIR)


download_files(year, final_mesh, WORKDIR, OUTDIR, PATH, OUTPATH)
