#!/usr/bin/env python
import os

from sirius.helpers.constants import DATA_SOURCE_ENCODEbigwig
from sirius.mongo import InfoNodes
from sirius.mongo.upload import update_insert_many
from sirius.tools.rebuild_mongo_database import ENCODE_BIGWIG_URL, download_not_exist ,parse_upload_data, mkchdir, TSVParser_ENCODEbigwig


def patch_bigwig_keyname():
    print("This patch replaces existing bigwig InfoNodes with new ones from updated parser")
    # delete existing bigwig infonodes
    print(f'Deleting InfoNodes with source: {DATA_SOURCE_ENCODEbigwig}')
    r = InfoNodes.delete_many({'source': DATA_SOURCE_ENCODEbigwig})
    print(f"Deleted {r.deleted_count} InfoNodes")
    # parse and upload new bigwig InfoNodes
    # find location of ENCODE data file
    print("Downloading bigwig data file")
    mkchdir('gene_data_tmp')
    mkchdir('encode_bigwig')
    download_not_exist(ENCODE_BIGWIG_URL)
    print("Parsing bigwig data file and uploading")
    parser = TSVParser_ENCODEbigwig(os.path.basename(ENCODE_BIGWIG_URL), verbose=True)
    parse_upload_data(parser, {"sourceurl": ENCODE_BIGWIG_URL})

if __name__ == '__main__':
    patch_bigwig_keyname()