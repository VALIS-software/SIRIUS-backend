#!/usr/bin/env python

import os, shutil, subprocess, json
from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.mongo.upload import update_insert_many
from sirius.parsers.GFFParser import GFFParser_ENSEMBL
from sirius.parsers.FASTAParser import FASTAParser
from sirius.parsers.BigWigParser import BigWigParser
from sirius.parsers.TSVParser import TSVParser_GWAS, TSVParser_ENCODEbigwig
from sirius.parsers.EQTLParser import EQTLParser_GTEx
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers.OBOParser import OBOParser_EFO
from sirius.parsers.TCGAParser import TCGA_XMLParser, TCGA_MAFParser, TCGA_CNVParser, DATA_SOURCE_TCGA
from sirius.helpers.tiledb import tilehelper

GRCH38_URL = 'ftp://ftp.ensembl.org/pub/release-92/gff3/homo_sapiens/Homo_sapiens.GRCh38.92.chr.gff3.gz'
GRCH38_FASTA_URL = 'ftp://ftp.ensembl.org/pub/release-92/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna_rm.primary_assembly.fa.gz'
GWAS_URL = 'https://www.ebi.ac.uk/gwas/api/search/downloads/alternative'
ENCODE_BIGWIG_URL = 'https://storage.googleapis.com/sirius_data_source/ENCODE_bigwig/ENCODE_bigwig_metadata.tsv'
CLINVAR_URL = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/archive_2.0/2018/clinvar_20180128.vcf.gz'
DBSNP_URL = 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/common_all_20180418.vcf.gz'
EFO_URL = 'https://raw.githubusercontent.com/EBISPOT/efo/master/efo.obo'
ExAC_URL = 'https://storage.googleapis.com/gnomad-public/legacy/exacv1_downloads/liftover_grch38/release1/ExAC.r1.sites.liftover.b38.vcf.gz'
GTEx_URL = 'https://storage.googleapis.com/gtex_analysis_v7/single_tissue_eqtl_data/GTEx_Analysis_v7_eQTL.tar.gz'
TCGA_URL = 'https://storage.googleapis.com/sirius_data_source/TCGA/tcga.tar.gz'

def mkchdir(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)
    os.chdir(dir)

def download_not_exist(url, filename=None, command=None):
    if filename == None:
        filename = os.path.basename(url)
    if command == None:
        command = 'wget'
    if not os.path.isfile(filename):
        subprocess.check_call(f'{command} {url}', shell=True)
    else:
        print(f"File {filename} exists, skipped download")

def download_genome_data():
    " Download Genome Data on to disk "
    print("\n\n#1. Downloading all datasets to disk, please make sure you have 5 GB free space")
    mkchdir('gene_data_tmp')
    # ENCODE bigwig
    print("Downloading ENCODE sample to bigwig folder")
    mkchdir('encode_bigwig')
    download_not_exist(ENCODE_BIGWIG_URL)
    os.chdir('..')
    # GRCh38_fasta
    print("Downloading GRCh38 sequence data in GRCh38_fasta folder")
    mkchdir('GRCh38_fasta')
    download_not_exist(GRCH38_FASTA_URL)
    os.chdir('..')
    # GRCh38_gff
    print("Downloading GRCh38 annotation data in GRCh38_gff folder")
    mkchdir('GRCh38_gff')
    download_not_exist(GRCH38_URL)
    os.chdir('..')
    # GWAS
    print("Downloading GWAS data in gwas folder")
    mkchdir('gwas')
    download_not_exist(GWAS_URL, filename='gwas.tsv', command='curl -o gwas.tsv')
    os.chdir('..')
    # ClinVar
    print("Downloading ClinVar data into ClinVar folder")
    mkchdir("ClinVar")
    download_not_exist(CLINVAR_URL)
    os.chdir('..')
    # ENCODE
    print("Downloading ENCODE data files into ENCODE folder")
    mkchdir("ENCODE")
    from sirius.tools import automate_encode_upload
    automate_encode_upload.download_search_files()
    os.chdir('..')
    #dbSNP
    print("Downloading dbSNP dataset in dbSNP folder")
    mkchdir('dbSNP')
    download_not_exist(DBSNP_URL)
    os.chdir('..')
    # EFO
    print("Downloading the EFO Ontology data file")
    mkchdir("EFO")
    download_not_exist(EFO_URL)
    os.chdir('..')
    # ExAC
    print("Downloading ExAC data file into ExAC folder")
    mkchdir('ExAC')
    download_not_exist(ExAC_URL)
    os.chdir('..')
    # GTEx
    print("Downloading GTEx data in GTEx folder")
    mkchdir("GTEx")
    download_not_exist(GTEx_URL)
    os.chdir('..')
    # TCGA
    print("Downloading TCGA data in TCGA folder")
    mkchdir("TCGA")
    download_not_exist(TCGA_URL)
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
    print("\n*** dbSNP ***")
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
    # GTEx
    print("\n*** GTEx ***")
    os.chdir('GTEx')
    parse_upload_GTEx_files()
    os.chdir('..')
    # TCGA
    print("\n*** TCGA ***")
    os.chdir('TCGA')
    parse_upload_TCGA_files()
    os.chdir('..')
    # Finish
    print("All parsing and uploading finished!")
    os.chdir('..')

def parse_upload_gff_chunk():
    filename = os.path.basename(GRCH38_URL)
    parser = GFFParser_ENSEMBL(filename, verbose=True)
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

def parse_upload_GTEx_files():
    filename = os.path.basename(GTEx_URL)
    # the big tar.gz file contains many individual data files
    print(f"Decompressing {filename}")
    subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    foldername = filename.split('.',1)[0]
    for f in os.listdir(foldername):
        if f.endswith('egenes.txt.gz'):
            fname = os.path.join(foldername, f)
            print(f"Parsing and uploading from {fname}")
            parser = EQTLParser_GTEx(fname, verbose=True)
            parser.parse()
            genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
            # we only insert the edges here for each file
            update_insert_many(Edges, edges)
    # change the filename to the big tar.gz file
    info_nodes[0]['info']['filename'] = filename
    # insert one infonode for the GTEx dataSource
    update_insert_many(InfoNodes, info_nodes)

def parse_upload_TCGA_files():
    filename = os.path.basename(TCGA_URL)
    print(f"Decompressing {filename}")
    subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    # three subfolders have been prepared and we will parse them one by one
    # XML for patient info
    os.chdir('BCRXML')
    xml_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.xml'):
                xml_files.append(os.path.join(root, f))
    all_patient_infonodes = []
    # this is used in MAF parser
    patient_barcode_tumor_site = dict()
    # this is used in CNV parser
    patient_uuid_tumor_site = dict()
    print(f"Parsing {len(xml_files)} patient xml files")
    for f in xml_files:
        parser = TCGA_XMLParser(f, verbose=True)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        # record the tumor site for each patient barcode
        info = info_nodes[0]['info']
        patient_barcode = info['bcr_patient_barcode']
        patient_barcode_tumor_site[patient_barcode] = info['tumor_tissue_site']
        patient_uuid = info['bcr_patient_uuid']
        patient_uuid_tumor_site[patient_uuid] = info['tumor_tissue_site']
        # collection individual info_nodes for each patient
        all_patient_infonodes += info_nodes
    # upload all patient info_nodes at once
    update_insert_many(InfoNodes, all_patient_infonodes)
    os.chdir('..')
    # MAF for mutations in tumors
    os.chdir('MAF')
    maf_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.maf.gz'):
                maf_files.append(os.path.join(root, f))
    print(f"Parsing {len(maf_files)} maf files")
    for i, f in enumerate(maf_files):
        print(f"{i:3d} ", end='', flush=True)
        parser = TCGA_MAFParser(f)
        parser.parse()
        # provide the patient_barcode_tumor_site so the gnode will have 'info.tumor_tissue_sites'
        patient_barcode_tumor_site = patient_barcode_tumor_site
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes(patient_barcode_tumor_site)
        update_insert_many(GenomeNodes, genome_nodes)
    os.chdir('..')
    # CNV
    os.chdir('CNV')
    cnv_file_caseIDs = dict()
    for d in json.load(open('metadata.json')):
        # Each file only have one case
        cnv_file_caseIDs[d['file_name']] = d['cases'][0]['case_id']
    cnv_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.seg.v2.txt'):
                cnv_files.append(os.path.join(root, f))
    print(f"Parsing {len(cnv_files)} cnv files")
    # we parse 1000 files each time then upload at once
    i_batch, batch_size = 0, 1000
    while True:
        start, end = i_batch*batch_size, (i_batch+1)*batch_size
        parsing_files = cnv_files[start:end]
        if len(parsing_files) == 0: break
        end = start + len(parsing_files)
        print(f"Parsing CNV files {start+1:6d} ~ {end:6d}")
        batch_genome_nodes = []
        for f in parsing_files:
            parser = TCGA_CNVParser(f)
            filebasename = os.path.basename(f)
            tumor_tissue_site = patient_uuid_tumor_site.get(cnv_file_caseIDs[filebasename], None)
            # some patient data are not available because they are in the "controlled access" catogory
            if tumor_tissue_site == None: continue
            parser.parse()
            genome_nodes, info_nodes, edges = parser.get_mongo_nodes(tumor_tissue_site)
            batch_genome_nodes += genome_nodes
        update_insert_many(GenomeNodes, batch_genome_nodes)
        i_batch += 1
    # Add one info node for dataSource
    update_insert_many(InfoNodes, [{
        '_id': 'I' + DATA_SOURCE_TCGA,
        "type": "dataSource",
        'name':DATA_SOURCE_TCGA,
        "source": DATA_SOURCE_TCGA,
        'info': {}
    }])
    # finish
    os.chdir('..')

def build_mongo_index():
    print("\n\n#4. Building index in data base")
    print("GenomeNodes")
    for idx in ['source', 'type', 'contig', 'start', 'end', 'length', 'info.biosample', 'info.accession', 'info.targets',
                'info.variant_tags', 'info.source']:
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
'''

def main():
    print(Instruction)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--starting_step', type=int, default=1, help='Choose a step to start.')
    parser.add_argument('-d', '--del_tmp', action='store_true', help='Delete gene_data_tmp folder after finish.')
    args = parser.parse_args()
    if args.starting_step <= 1:
        download_genome_data()
    if args.starting_step <= 2:
        drop_all_data()
    if args.starting_step <= 3:
        parse_upload_all_datasets()
    if args.starting_step <= 4:
        build_mongo_index()
    if args.del_tmp:
        clean_up()

if __name__ == "__main__":
    main()
