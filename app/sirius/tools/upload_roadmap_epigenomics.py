#!/usr/bin/env python

import os, subprocess
from sirius.tools.rebuild_mongo_database import ROADMAP_EPIGENOMICS_URL, download_not_exist, parse_upload_ROADMAP_EPIGENOMICS

def download():
    subpath = 'gene_data_tmp/roadmap_epigenomics'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    download_not_exist(ROADMAP_EPIGENOMICS_URL)

def main():
    download()
    parse_upload_ROADMAP_EPIGENOMICS()

if __name__ == '__main__':
    main()