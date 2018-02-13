#!/usr/bin/env python

import os, shutil
from google.cloud import storage
from zipfile import ZipFile

def update_valis_webfront():
    # Update /static/dist with the latest dist.zip file on our Google Cloud
    # We assume that the environment variable GOOGLE_APPLICATION_CREDENTIALS is set, or we are running in a cloud compute engine or kubernete engine
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('valis-front-dev')
    blob = bucket.get_blob('dist/dist.zip')
    print("Connected to google cloud. File dist/dist.zip found.")
    # download the archieve to tmp_path
    tmp_path = "tmp_dist"
    if os.path.isdir(tmp_path):
        shutil.rmtree(tmp_path)
    os.mkdir(tmp_path)
    dest_fname = os.path.join(tmp_path, 'dist.zip')
    blob.download_to_filename(dest_fname)
    print("File download finished.")

    # unzip
    with ZipFile(dest_fname) as zf:
        zf.extractall(tmp_path)
    # We will leave the zip file for debugging
    # os.remove(dest_fname)
    print("Unzip finished.")

    # target folder to put our front-end js code
    # We use the relative path to the location of this file, to allow running this script from anywhere
    this_file_folder = os.path.dirname(os.path.realpath(__file__))
    target_path = os.path.join(this_file_folder, "../valis-dist")
    # check if the target folder already exists, if so, remove it
    if os.path.isdir(target_path):
        shutil.rmtree(target_path)
    shutil.move(tmp_path, target_path)
    print("static/dist folder was replaced. All step finished!")

if __name__ == "__main__":
     update_valis_webfront()
