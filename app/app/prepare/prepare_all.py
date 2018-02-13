#!/usr/bin/env python

import update_valis_webfront, copy_pyensembl_cache

def prepare_all():
    try:
        update_valis_webfront.update_valis_webfront()
        copy_pyensembl_cache.copy_pyensembl_cache()
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    prepare_all()
