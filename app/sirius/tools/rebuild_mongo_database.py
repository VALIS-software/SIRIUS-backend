#!/usr/bin/env python

import os, shutil, subprocess
from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.mongo.upload import update_insert_many
from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.GWASParser import GWASParser
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP

GRCH38_URL = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.36_GRCh38.p10/GCF_000001405.36_GRCh38.p10_genomic.gff.gz'
GWAS_URL = 'https://www.ebi.ac.uk/gwas/api/search/downloads/full'
#EQTL_URL = 'http://www.exsnp.org/data/GSexSNP_allc_allp_ld8.txt'
# We use a private source here because the above one is too slow now.
EQTL_URL = 'https://storage.googleapis.com/sirius_data_source/eQTL/GSexSNP_allc_allp_ld8.txt'
CLINVAR_URL = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/archive_2.0/2018/clinvar_20180128.vcf.gz'

def download_genome_data():
    " Download Genome Data on to disk "
    print("\n\n#1. Downloading all datasets to disk, please make sure you have 5 GB free space")
    os.mkdir('gene_data_tmp')
    os.chdir('gene_data_tmp')
    # GRCh38_gff
    print("Downloading GRCh38 annotation data in GRCh38_gff folder")
    os.mkdir('GRCh38_gff')
    os.chdir('GRCh38_gff')
    subprocess.check_call('wget '+GRCH38_URL, shell=True)
    print("Decompressing")
    subprocess.check_call('gzip -d %s' % (os.path.basename(GRCH38_URL)), shell=True)
    os.chdir('..')
    # GWAS
    print("Downloading GWAS data in gwas folder")
    os.mkdir('gwas')
    os.chdir('gwas')
    subprocess.check_call('curl -o gwas.tsv '+GWAS_URL, shell=True)
    os.chdir('..')
    # eQTL
    print("Downloading eQTL data in eQTL folder")
    os.mkdir("eQTL")
    os.chdir("eQTL")
    subprocess.check_call('curl -O '+EQTL_URL, shell=True)
    os.chdir('..')
    # ClinVar
    print("Downloading ClinVar data into ClinVar folder")
    os.mkdir("ClinVar")
    os.chdir("ClinVar")
    subprocess.check_call('wget '+CLINVAR_URL, shell=True)
    print("Decompressing")
    subprocess.check_call('gzip -d clinvar_20180128.vcf.gz', shell=True)
    os.chdir('..')
    # ENCODE
    print("ENCODE data downloading will be handled later.")
    os.mkdir("ENCODE")
    # Finish
    print("All downloads finished")
    os.chdir('..')

def drop_all_data():
    " Drop all collections from database "
    print("\n\n#2. Deleting existing data.")
    for cname in db.list_collection_names():
        print("Dropping %s" % cname)
        db.drop_collection(cname)

def parse_upload_all_datasets():
    print("\n\n#3. Parsing and uploading each data set")
    os.chdir('gene_data_tmp')
    # GRCh38_gff
    print("\n*** GRCh38_gff ***")
    os.chdir('GRCh38_gff')
    parse_upload_gff_chunk()
    os.chdir('..')
    # GWAS
    print("\n*** GWAS ***")
    os.chdir('gwas')
    parser = GWASParser('gwas.tsv', verbose=True)
    parse_upload_data(parser, GWAS_URL)
    os.chdir('..')
    # eQTL
    print("\n*** eQTL ***")
    os.chdir('eQTL')
    parser = EQTLParser('GSexSNP_allc_allp_ld8.txt', verbose=True)
    parse_upload_data(parser, EQTL_URL)
    os.chdir('..')
    # ClinVar
    print("\n*** ClinVar ***")
    os.chdir('ClinVar')
    parser = VCFParser_ClinVar('clinvar_20180128.vcf', verbose=True)
    parse_upload_data(parser, CLINVAR_URL)
    os.chdir('..')
    # ENCODE
    print("\n*** ENCODE ***")
    os.chdir('ENCODE')
    from sirius.tools import automate_encode_upload
    automate_encode_upload.main()
    os.chdir('..')
    # Finished
    print("All parsing and uploading finished!")
    os.chdir('..')


def parse_upload_gff_chunk():
    filename = os.path.basename(GRCH38_URL)[:-3]
    parser = GFFParser(filename, verbose=True)
    chunk_fnames = parser.parse_save_data_in_chunks()
    # parse and upload data in chunks to reduce memory usage
    prev_parser = None
    for fname in chunk_fnames:
        # we still want the original filename for each chunk
        parser = GFFParser(filename)
        # the GFF data set are sequencially depending on each other
        # so we need to inherit some information from previous parser
        if prev_parser != None:
            parser.seqid_loc = prev_parser.seqid_loc
            parser.known_id_set = prev_parser.known_id_set
        prev_parser = parser
        with open(fname) as chunkfile:
            parser.load_json(chunkfile)
            parser.metadata['sourceurl'] = GRCH38_URL
            genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
            update_insert_many(GenomeNodes, genome_nodes)
        print("Data from %s uploaded" % fname)
    # we only upload info_nodes once here because all the chunks has the same single info node for the dataSource.
    update_insert_many(InfoNodes, info_nodes)
    print("InfoNodes uploaded")

def parse_upload_data(parser, url):
    parser.parse()
    parser.metadata['sourceurl'] = url
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    update_insert_many(GenomeNodes, genome_nodes)
    update_insert_many(InfoNodes, info_nodes)
    update_insert_many(Edges, edges)

def build_mongo_index():
    print("\n\n#4. Building index in data base")
    print("GenomeNodes")
    for idx in ['source', 'assembly', 'type', 'chromid', 'start', 'end', 'length', 'info.biosample', 'info.accession', 'info.targets']:
        print("Creating index %s" % idx)
        GenomeNodes.create_index(idx)

    print("InfoNodes")
    for idx in ['source', 'type']:
        print("Creating index %s" % idx)
        InfoNodes.create_index(idx)
    print("Creating text index 'info.description'")
    InfoNodes.create_index([('info.description', 'text')], default_language='english')

    print("Edges")
    for idx in ['source', 'from_id', 'to_id', 'type', 'info.p-value']:
        print("Creating index %s" % idx)
        Edges.create_index(idx)

def clean_up():
    shutil.rmtree('gene_data_tmp')


Instruction = '''
---------------------------------------------------------
| Automated script to rebuild the entire Mongo database |
---------------------------------------------------------
Steps:
1. Download data sets files onto disk
2. Delete all data from existing database
3. Parse each data sets and upload to MongoDB
4. Build index in data base
5. Clean up
'''

def main():
    print(Instruction)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--starting_step', type=int, default=1, help='Choose a step to start.')
    parser.add_argument('-k', '--keep_tmp', action='store_true', help='Keep gene_data_tmp folder.')
    args = parser.parse_args()
    if args.starting_step <= 1:
        download_genome_data()
    if args.starting_step <= 2:
        drop_all_data()
    if args.starting_step <= 3:
        parse_upload_all_datasets()
    if args.starting_step <= 4:
        build_mongo_index()
    if args.starting_step <= 5 and not args.keep_tmp:
        clean_up()

if __name__ == "__main__":
    main()
