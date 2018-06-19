#!/usr/bin/env python

import os, subprocess
from sirius.parsers.VCFParser import VCFParser_ExAC
from sirius.mongo.upload import update_insert_many
from sirius.mongo import GenomeNodes, InfoNodes, Edges

ExAC_URL = 'https://storage.googleapis.com/gnomad-public/legacy/exacv1_downloads/liftover_grch38/release1/ExAC.r1.sites.liftover.b38.vcf.gz'
FILENAME = os.path.basename(ExAC_URL)

def download():
    subpath = 'gene_data_tmp/ExAC'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
        os.chdir(subpath)
    print("Downloading dbSNP data file")
    if os.path.isfile(FILENAME):
        print(f"File {FILENAME} already exist, skipping downloading")
    else:
        print("Downloading dbSNP data file")
        subprocess.check_call('wget '+ExAC_URL, shell=True)

def parse_upload():
    parser = VCFParser_ExAC(FILENAME, verbose=True)
    parser.metadata['sourceurl'] = ExAC_URL
    i_chunk = 0
    while True:
        finished = parser.parse_chunk(100000)
        print(f'Parsing and uploading chunk {i_chunk}')
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        i_chunk += 1
        if finished == True:
            break
    update_insert_many(InfoNodes, info_nodes)

def main():
    download()
    parse_upload()

if __name__ == '__main__':
    main()
