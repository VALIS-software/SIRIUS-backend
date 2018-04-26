#!/usr/bin/env python

import os, subprocess
from sirius.parsers.VCFParser import VCFParser_dbSNP
from sirius.mongo.upload import update_insert_many
from sirius.mongo import GenomeNodes, InfoNodes, Edges

DBSNP_URL = 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/common_all_20180418.vcf.gz'
FILENAME = os.path.basename(DBSNP_URL)

def download():
    subpath = 'gene_data_tmp/dbSNP'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
        os.chdir(subpath)
    print("Downloading dbSNP data file")
    if os.path.isfile(FILENAME):
        print(f"File {FILENAME} already exist, skipping downloading")
    else:
        print("Downloading dbSNP data file")
        subprocess.check_call('wget '+DBSNP_URL, shell=True)

def parse_upload():
    parser = VCFParser_dbSNP(FILENAME, verbose=True)
    parser.metadata['sourceurl'] = DBSNP_URL
    i_chunk = 0
    while True:
        finished = parser.parse_chunk()
        print(f'Parsing and uploading chunk {i_chunk}')
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        i_chunk += 1
        if finished == True:
            break
    # we only insert the infonode for dbSNP dataSource once
    update_insert_many(InfoNodes, info_nodes)

def main():
    download()
    parse_upload()

if __name__ == '__main__':
    main()
