#!/usr/bin/env python

import os, subprocess
from sirius.tools.rebuild_mongo_database import TCGA_URL, parse_upload_TCGA_files

def download():
    subpath = 'gene_data_tmp/TCGA'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    filename = os.path.basename(TCGA_URL)
    if os.path.isfile(filename):
        print(f"File {filename} already exist, skipping downloading")
    else:
        print("Downloading data file")
        subprocess.check_call('wget '+TCGA_URL, shell=True)

def main():
    download()
    parse_upload_TCGA_files()

if __name__ == '__main__':
    main()
