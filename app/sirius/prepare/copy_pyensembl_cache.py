#!/usr/bin/env python

import os, shutil

def copy_pyensembl_cache(source='\pd\pyensembl'):
    """
    Copy pyensembl cache from \pd\pyensembl to a writable cache folder.
    This will help avoid downloading the cache everytime in Docker build.
    """
    if not os.path.isdir(source):
        print("Source not found at %s. Nothing done." % source)
        return
    if 'PYENSEMBL_CACHE_DIR' in os.environ:
        cachedir = os.environ['PYENSEMBL_CACHE_DIR']
    else:
        cachedir = os.path.join(os.environ['HOME'], '.cache')
    dest = os.path.join(cachedir, 'pyensembl')
    shutil.copytree(source, dest)
    print("Copy from %s to %s finished." % (source, dest))

if __name__ == "__main__":
     copy_pyensembl_cache()
