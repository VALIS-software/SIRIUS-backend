#!/usr/bin/env python

import os, shutil

def copy_tiledb(source='/pd/tiledb'):
    """
    Copy pyensembl cache from \pd\pyensembl to a writable cache folder.
    This will help avoid downloading the cache everytime in Docker build.
    """
    if not os.path.isdir(source):
        print("Source not found at %s. Nothing done." % source)
        return
    dest = os.environ['TILEDB_ROOT']
    if not os.path.isdir(dest):
        shutil.copytree(source, dest)
        print(f"Copy from {source} to {dest} finished.")
    else:
        print(f"{dest} already exist, skipped copy.")

if __name__ == "__main__":
     copy_tiledb()
