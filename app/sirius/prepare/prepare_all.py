#!/usr/bin/env python

import os
from sirius.prepare import update_valis_webfront, download_pyensembl_cache, download_tiledb_data

def prepare_all():
    try:
        update_valis_webfront.update_valis_webfront()
    except Exception as e:
        print(str(e))
    try:
        download_pyensembl_cache.download_pyensembl_cache()
    except Exception as e:
        print(str(e))
    try:
        download_tiledb_data.download_fasta_tiledb()
    except Exception as e:
        print(str(e))

    # create the temp folder if not exist
    tmpdir = os.environ.get('SIRIUS_TEMP_DIR', None)
    if tmpdir != None:
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)

if __name__ == "__main__":
    prepare_all()
