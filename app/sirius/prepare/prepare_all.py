#!/usr/bin/env python

from sirius.prepare import update_valis_webfront, copy_pyensembl_cache, copy_tiledb

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
        copy_tildb.copy_tiledb()
    except Exception as e:
        print(str(e))
   
if __name__ == "__main__":
    prepare_all()
