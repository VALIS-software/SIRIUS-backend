#!/usr/bin/env python

import os, shutil, subprocess
from sirius.helpers import storage_buckets

def download_pyensembl_cache():
    cachedir = os.environ['PYENSEMBL_CACHE_DIR']
    dest = os.path.join(cachedir, 'pyensembl')
    if not os.path.exists(dest):
        os.chdir(cachedir)
        bucket = storage_buckets['siriusdata']
        filename = 'pyensembl.tar.gz'
        blob = bucket.get_blob(filename)
        print(f"Connected to google cloud. File {filename} found.")
        blob.download_to_filename(filename)
        print("File download finished. Extracting")
        subprocess.check_call(f"tar zxf {filename}", shell=True)
        # delete the tar file
        os.unlink(filename)
        print(f"File {filename} deleted")
    else:
        print(f"{dest} already exists, skipped downloading")

if __name__ == "__main__":
     download_pyensembl_cache()
