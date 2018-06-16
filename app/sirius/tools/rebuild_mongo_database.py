#!/usr/bin/env python

import os, shutil, subprocess
from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.mongo.upload import update_insert_many
from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.FASTAParser import FASTAParser
from sirius.parsers.BigWigParser import BigWigParser
from sirius.parsers.TSVParser import TSVParser_GWAS, TSVParser_ENCODEbigwig
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers.OBOParser import OBOParser_EFO
from sirius.helpers.tiledb import tilehelper

GRCH38_URL = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.36_GRCh38.p10/GCF_000001405.36_GRCh38.p10_genomic.gff.gz'
GRCH38_FASTA_URL = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.36_GRCh38.p10/GCF_000001405.36_GRCh38.p10_genomic.fna.gz'
GWAS_URL = 'https://www.ebi.ac.uk/gwas/api/search/downloads/alternative'
ENCODE_BIGWIG_URL = 'https://storage.googleapis.com/sirius_data_source/ENCODE_bigwig/ENCODE_bigwig_metadata.tsv'
#EQTL_URL = 'http://www.exsnp.org/data/GSexSNP_allc_allp_ld8.txt'
# We use a private source here because the above one is too slow now.
EQTL_URL = 'https://storage.googleapis.com/sirius_data_source/eQTL/GSexSNP_allc_allp_ld8.txt'
CLINVAR_URL = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/archive_2.0/2018/clinvar_20180128.vcf.gz'
DBSNP_URL = 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/common_all_20180418.vcf.gz'
EFO_URL = 'https://raw.githubusercontent.com/EBISPOT/efo/master/efo.obo'
ExAC_URL = 'https://storage.googleapis.com/gnomad-public/legacy/exacv1_downloads/liftover_grch38/release1/ExAC.r1.sites.liftover.b38.vcf.gz'

def download_genome_data():
    " Download Genome Data on to disk "
    print("\n\n#1. Downloading all datasets to disk, please make sure you have 5 GB free space")
    os.mkdir('gene_data_tmp')
    os.chdir('gene_data_tmp')
    # ENCODE bigwig
    print("Downloading ENCODE sample to bigwig folder")
    os.mkdir('encode_bigwig')
    os.chdir('encode_bigwig')
    subprocess.check_call('wget '+ENCODE_BIGWIG_URL, shell=True)
    os.chdir('..')
    # GRCh38_fasta
    print("Downloading GRCh38 sequence data in GRCh38_fasta folder")
    os.mkdir('GRCh38_fasta')
    os.chdir('GRCh38_fasta')
    subprocess.check_call('wget '+GRCH38_FASTA_URL, shell=True)
    os.chdir('..')
    # GRCh38_gff
    print("Downloading GRCh38 annotation data in GRCh38_gff folder")
    os.mkdir('GRCh38_gff')
    os.chdir('GRCh38_gff')
    subprocess.check_call('wget '+GRCH38_URL, shell=True)
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
    os.chdir('..')
    # ENCODE
    print("Downloading ENCODE data files into ENCODE folder")
    os.mkdir("ENCODE")
    os.chdir("ENCODE")
    from sirius.tools import automate_encode_upload
    automate_encode_upload.download_search_files()
    os.chdir('..')
    #dbSNP
    print("Downloading dbSNP dataset in dbSNP folder")
    os.mkdir('dbSNP')
    os.chdir('dbSNP')
    subprocess.check_call('wget '+DBSNP_URL, shell=True)
    os.chdir('..')
    # EFO
    print("Downloading the EFO Ontology data file")
    os.mkdir("EFO")
    os.chdir("EFO")
    subprocess.check_call('wget '+EFO_URL, shell=True)
    os.chdir('..')
    # ExAC
    print("Downloading ExAC data file into ExAC folder")
    os.mkdir('ExAC')
    os.chdir('ExAC')
    subprocess.check_call('wget '+ExAC_URL, shell=True)
    os.chdir('..')
    # Finish
    print("All downloads finished")
    os.chdir('..')

def drop_all_data():
    " Drop all collections from database and delete all TileDB files"
    # fetch all the InfoNodes for organisms:
    # iterate through the chromosomes for each organism and delete each TileDB file:
    print("\n\n#2. Deleting existing data.")
    for cname in db.list_collection_names():
        print(f"Dropping {cname}")
        db.drop_collection(cname)
    # drop the tileDB directory
    if os.path.exists(tilehelper.root):
        print(f"Deleting tiledb folder {tilehelper.root}")
        shutil.rmtree(tilehelper.root)
        os.makedirs(tilehelper.root)

def parse_upload_all_datasets():
    print("\n\n#3. Parsing and uploading each data set")
    os.chdir('gene_data_tmp')
    # ENCODE bigWig
    print("*** ENCODE_bigwig ***")
    os.chdir('encode_bigwig')
    parser = TSVParser_ENCODEbigwig(os.path.basename(ENCODE_BIGWIG_URL), verbose=True)
    parse_upload_data(parser, {"sourceurl": ENCODE_BIGWIG_URL})
    os.chdir('..')
    # GRCh38_fasta
    print("\n*** GRCh38_fasta ***")
    os.chdir('GRCh38_fasta')
    parser = FASTAParser(os.path.basename(GRCH38_FASTA_URL), verbose=True)
    parse_upload_data(parser, {"sourceurl": GRCH38_FASTA_URL})
    os.chdir('..')
    # GRCh38_gff
    print("\n*** GRCh38_gff ***")
    os.chdir('GRCh38_gff')
    parse_upload_gff_chunk()
    os.chdir('..')
    # GWAS
    print("\n*** GWAS ***")
    os.chdir('gwas')
    parser = TSVParser_GWAS('gwas.tsv', verbose=True)
    parse_upload_data(parser, {"sourceurl": GWAS_URL})
    os.chdir('..')
    # eQTL
    print("\n*** eQTL ***")
    os.chdir('eQTL')
    parser = EQTLParser('GSexSNP_allc_allp_ld8.txt', verbose=True)
    parse_upload_data(parser, {"sourceurl": EQTL_URL})
    os.chdir('..')
    # ClinVar
    print("\n*** ClinVar ***")
    os.chdir('ClinVar')
    parser = VCFParser_ClinVar('clinvar_20180128.vcf.gz', verbose=True)
    parse_upload_data(parser, {"sourceurl": CLINVAR_URL})
    os.chdir('..')
    # ENCODE
    print("\n*** ENCODE ***")
    os.chdir('ENCODE')
    from sirius.tools import automate_encode_upload
    automate_encode_upload.parse_upload_files()
    os.chdir('..')
    # dbSNP
    os.chdir('dbSNP')
    parse_upload_dbSNP_chunk()
    os.chdir('..')
    # EFO
    print("\n*** EFO ***")
    os.chdir('EFO')
    parser = OBOParser_EFO('efo.obo', verbose=True)
    parse_upload_data(parser, {"sourceurl": EFO_URL})
    os.chdir('..')
    # ExAC
    print("\n*** ExAC ***")
    os.chdir('ExAC')
    parse_upload_ExAC_chunk()
    os.chdir('..')
    # Finished
    print("All parsing and uploading finished!")
    os.chdir('..')

def parse_upload_gff_chunk():
    filename = os.path.basename(GRCH38_URL)
    parser = GFFParser(filename, verbose=True)
    parser.metadata['sourceurl'] = GRCH38_URL
    i_chunk = 0
    while True:
        finished = parser.parse_chunk()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        update_insert_many(InfoNodes, info_nodes[1:])
        print(f"Data of chunk {i_chunk} uploaded")
        i_chunk += 1
        if finished == True:
            break
    # we only upload info_nodes[0] once here because all the chunks has the same first info node for the dataSource.
    update_insert_many(InfoNodes, info_nodes[0:1])
    print("InfoNodes uploaded")

def parse_upload_data(parser, metadata={}):
    parser.parse()
    parser.metadata.update(metadata)
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    update_insert_many(GenomeNodes, genome_nodes)
    update_insert_many(InfoNodes, info_nodes)
    update_insert_many(Edges, edges)

def parse_upload_dbSNP_chunk():
    filename = os.path.basename(DBSNP_URL)
    parser = VCFParser_dbSNP(filename, verbose=True)
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

def parse_upload_ExAC_chunk():
    filename = os.path.basename(ExAC_URL)
    parser = VCFParser_ExAC(filename, verbose=True)
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
    # we only insert the infonode for ExAC dataSource once
    update_insert_many(InfoNodes, info_nodes)

def build_mongo_index():
    print("\n\n#4. Building index in data base")
    print("GenomeNodes")
    for idx in ['source', 'type', 'contig', 'start', 'end', 'length', 'info.biosample', 'info.accession', 'info.targets']:
        print("Creating index %s" % idx)
        GenomeNodes.create_index(idx)
    print("Creating compound index for 'type' and 'info.biosample'")
    GenomeNodes.create_index([('type', 1), ('info.biosample', 1)])
    print("InfoNodes")
    for idx in ['source', 'type', 'info.biosample', 'info.targets', 'info.types', 'info.assay', 'info.outtype']:
        print("Creating index %s" % idx)
        InfoNodes.create_index(idx)
    print("Creating text index 'name'")
    InfoNodes.create_index([('name', 'text')], default_language='english')
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
