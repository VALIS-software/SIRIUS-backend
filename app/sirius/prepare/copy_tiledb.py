#!/usr/bin/env python

import os, shutil

def copy_tiledb(source='/pd/tiledb'):
    """
    Copy pyensembl cache from /pd/tiledb to a writable cache folder.
    """
    if not os.path.isdir(source):
        print("Source not found at %s. Nothing done." % source)
        return
    dest = os.environ.get('TILEDB_ROOT', None)
    if not os.path.isdir(dest):
        shutil.copytree(source, dest)
        print(f"Copy from {source} to {dest} finished.")
    else:
        print(f"{dest} already exist, skipped copy.")

if __name__ == "__main__":
     copy_tiledb()
