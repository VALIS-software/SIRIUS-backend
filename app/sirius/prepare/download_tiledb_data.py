#!/usr/bin/env python

import os
import shutil
import subprocess
from datetime import datetime, timezone, timedelta
from sirius.helpers import storage_buckets

def download_fasta_tiledb():
    dest = os.environ['TILEDB_ROOT']
    if not os.path.isdir(dest):
        os.makedirs(dest)
    os.chdir(dest)
    filename = 'sirius_fasta_data.tar.gz'
    bucket = storage_buckets['siriusdata']
    blob = bucket.get_blob(filename)
    print(f"Connected to google cloud. File {filename} found.")
    # check if local download exists
    skip_download = False
    if os.path.exists(filename):
        # compare updated time of remote and local versions of file
        local_file_mtime = datetime.fromtimestamp(os.path.getmtime(filename), tz=timezone.utc)
        if blob.updated - local_file_mtime < timedelta(seconds=10):
            skip_download = True
            print(f"Found local file {filename} up-to-date, skip downloading")
        else:
            print(f"Found local file {filename} older than cloud blob, cleaning files fasta_*")
            subprocess.check_call("rm -r fasta_*", shell=True)
    if not skip_download:
        print(f"Downloading {filename} from cloud storage bucket")
        blob.download_to_filename(filename)
        print("File download finished. Extracting")
        subprocess.check_call(f"tar zxf {filename}", shell=True)
        print("File extraction finished")


    ## 2nd method: use rsync command to download and sync tiledb folder
    ## This method is more robust, which can avoid interupted downloading and unzipping
    ## However the rsync checksums is too slow in container, so we fallback to the first method
    # print(f"Running rsync to download tiledb folder")
    # subprocess.run(f"gsutil -m rsync -r {dest} gs://siriusdata/tiledb > rsync.log", shell=True, check=True)

if __name__ == "__main__":
     download_fasta_tiledb()
