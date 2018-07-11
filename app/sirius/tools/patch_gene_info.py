#!/usr/bin/env python

import os, subprocess
from sirius.mongo import GenomeNodes
from sirius.parsers.GFFParser import GFFParser_RefSeq
from sirius.helpers.constants import ENSEMBL_GENE_SUBTYPES

RefSeq_GFF_URL = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.38_GRCh38.p12/GCF_000001405.38_GRCh38.p12_genomic.gff.gz'

def download_refseq_gff():
    subpath = 'gene_data_tmp/RefSeq_gff'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    filename = os.path.basename(RefSeq_GFF_URL)
    if os.path.isfile(filename):
        print(f"File {filename} already exist, skipping downloading")
    else:
        print("Downloading data file")
        subprocess.check_call('wget '+RefSeq_GFF_URL, shell=True)
    return filename

def parse_refseq_gene_info(filename):
    parser = GFFParser_RefSeq(filename)
    gene_name_extra_info = dict()
    i_chunk = 0
    while True:
        finished = parser.parse_chunk()
        for feature in parser.features:
            if feature['type'] in ('gene', 'pseudogene'):
                name = feature['attributes'].get('Name', None)
                if name is None: continue
                info = dict()
                dbxref = feature['attributes'].get('Dbxref', '')
                for ref in dbxref.split(','):
                    refname, ref_id = ref.split(':', 1)
                    info['info.'+refname] = ref_id
                gene_name_extra_info[name] = info
        print(f"Data of chunk {i_chunk} parsed")
        i_chunk += 1
        if finished == True:
            break
    print(f"Parsing RefSeq genes finished, total {len(gene_name_extra_info)} genes")
    return gene_name_extra_info

def pull_existing_gene_name_id():
    existing_gene_name_id = dict()
    for gnode in GenomeNodes.find({'type': {'$in': ENSEMBL_GENE_SUBTYPES}}, projection=['_id', 'name']):
        existing_gene_name_id[gnode['name']] = gnode['_id']
    print(f"Pulling existing genes finished, total {len(existing_gene_name_id)} genes")
    return existing_gene_name_id

def update_existing_genes(gene_name_extra_info, existing_gene_name_id):
    n_update = 0
    not_found = []
    not_updated = set(existing_gene_name_id.keys())
    for genename, info in gene_name_extra_info.items():
        existing_id = existing_gene_name_id.get(genename, None)
        # try to find MT- names
        if existing_id is None:
            genename = 'MT-' + genename
            existing_id = existing_gene_name_id.get(genename, None)
        if existing_id is not None:
            filt = {'_id': existing_id}
            update = {
                '$set': info
            }
            GenomeNodes.update_one(filt, update)
            n_update += 1
            not_updated.remove(genename)
        else:
            not_found.append(genename)
    with open('not_found_genes.txt', 'w') as outfile:
        outfile.write('\n'.join(sorted(not_found)))
    with open('not_updated.txt', 'w') as outfile:
        outfile.write('\n'.join(sorted(not_updated)))
    print(f"Update Genomenodes finished. Updated {n_update} genes")

def patch_gene_ID_info():
    print("\n** Patching Gene info with RefSeq database **\n")
    filename = download_refseq_gff()
    gene_name_extra_info = parse_refseq_gene_info(filename)
    existing_gene_name_id = pull_existing_gene_name_id()
    update_existing_genes(gene_name_extra_info, existing_gene_name_id)

def main():
    patch_gene_ID_info()

if __name__ == '__main__':
    main()


