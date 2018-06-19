#!/usr/bin/env python

import os, shutil, subprocess
from google.cloud import storage

def download_fasta_tiledb():
    dest = os.environ['TILEDB_ROOT']
    filename = 'sirius_fasta_data.tar.gz'
    if not os.path.isdir(dest):
        os.makedirs(dest)
        os.chdir(dest)
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('siriusdata')
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
     download_fasta_tiledb()
