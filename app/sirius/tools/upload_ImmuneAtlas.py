#!/usr/bin/env python

import os, subprocess
from sirius.tools.rebuild_mongo_database import IMMUNE_ATLAS_URL, download_not_exist, parse_upload_ImmuneAtlas

def download():
    subpath = 'gene_data_tmp/'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    download_not_exist(IMMUNE_ATLAS_URL)
    os.chdir('ImmuneAtlasBed')

def main():
    download()
    parse_upload_ImmuneAtlas()

if __name__ == '__main__':
    main()