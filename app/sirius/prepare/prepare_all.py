#!/usr/bin/env python

from sirius.prepare import update_valis_webfront, copy_pyensembl_cache, download_tiledb_data

def prepare_all():
    try:
        update_valis_webfront.update_valis_webfront()
    except Exception as e:
        print(str(e))
    try:
        copy_pyensembl_cache.copy_pyensembl_cache()
    except Exception as e:
        print(str(e))
    try:
        download_tiledb_data.download_fasta_tiledb()
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    prepare_all()
