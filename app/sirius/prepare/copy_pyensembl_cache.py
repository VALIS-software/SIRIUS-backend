#!/usr/bin/env python

import os, shutil

def copy_pyensembl_cache(source='/pd/pyensembl'):
    """
    Copy pyensembl cache from /pd/pyensembl to a writable cache folder.
    This will help avoid downloading the cache everytime in Docker build.
    """
    if not os.path.isdir(source):
        print("Source not found at %s. Nothing done." % source)
        return
    cachedir = os.environ.get('PYENSEMBL_CACHE_DIR', os.path.join(os.environ['HOME'], '.cache'))
    dest = os.path.join(cachedir, 'pyensembl')
    if not os.path.exists(dest):
        shutil.copytree(source, dest)
        print(f"Copy from {source} to {dest} finished.")
    else:
        print(f"{dest} already exists, skipped copy")

if __name__ == "__main__":
     copy_pyensembl_cache()
