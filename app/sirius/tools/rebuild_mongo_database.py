#!/usr/bin/env python

import os
import shutil
import subprocess
import json
import time
import collections

from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.mongo.upload import update_insert_many, update_skip_insert

from sirius.parsers import GFFParser_ENSEMBL
from sirius.parsers import TSVParser_GWAS, TSVParser_ENCODEbigwig, TSVParser_HGNC
from sirius.parsers import EQTLParser_GTEx
from sirius.parsers import VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers import BEDParser_ENCODE, BEDParser_ROADMAP_EPIGENOMICS
from sirius.parsers import FASTAParser
from sirius.parsers import OBOParser_EFO
from sirius.parsers import TCGA_XMLParser, TCGA_MAFParser, TCGA_CNVParser
from sirius.parsers import KEGG_XMLParser
from sirius.parsers import Parser_NatureCasualVariants

from sirius.helpers.constants import DATA_SOURCE_TCGA, DATA_SOURCE_GWAS, DATA_SOURCE_GTEX, \
    DATA_SOURCE_KEGG, DATA_SOURCE_ROADMAP_EPIGENOMICS, ENSEMBL_GENE_SUBTYPES
from sirius.helpers.tiledb import tilehelper

# By default we will build a small version of the database for dev only
FULL_DATABASE = False

GRCH38_URL = 'ftp://ftp.ensembl.org/pub/release-92/gff3/homo_sapiens/Homo_sapiens.GRCh38.92.chr.gff3.gz'
GRCH38_FASTA_URL = 'ftp://ftp.ensembl.org/pub/release-92/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz'
GWAS_URL = 'gs://sirius_data_source/GWAS/gwas.tsv'
ENCODE_BIGWIG_URL = 'https://storage.googleapis.com/sirius_data_source/ENCODE_bigwig/ENCODE_bigwig_metadata.tsv'
CLINVAR_URL = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/archive_2.0/2018/clinvar_20180128.vcf.gz'
ENCODE_URL = 'gs://sirius_data_source/ENCODE/'
DBSNP_URL = 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/common_all_20180418.vcf.gz'
ExAC_URL = 'https://storage.googleapis.com/gnomad-public/legacy/exacv1_downloads/liftover_grch38/release1/ExAC.r1.sites.liftover.b38.vcf.gz'
GTEx_URL = 'https://storage.googleapis.com/gtex_analysis_v7/single_tissue_eqtl_data/GTEx_Analysis_v7_eQTL.tar.gz'
TCGA_URL = 'https://storage.googleapis.com/sirius_data_source/TCGA/tcga.tar.gz'
EFO_URL = 'https://raw.githubusercontent.com/EBISPOT/efo/master/efo.obo'
HGNC_URL = 'https://storage.googleapis.com/sirius_data_source/HGNC/hgnc_complete_set.txt'
KEGG_URL = 'https://storage.googleapis.com/sirius_data_source/KEGG/kegg_pathways.tar.gz'
NATURE_CASUAL_VARIANTS_URL = 'gs://sirius_data_source/Nature-Causal-Variants/nature13835-s1.csv'
ROADMAP_EPIGENOMICS_URL = 'https://s3.amazonaws.com/layerlab/giggle/roadmap/roadmap_sort.tar.gz'

if FULL_DATABASE:
    DBSNP_URL = 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/VCF/All_20180418.vcf.gz'

def mkchdir(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)
    os.chdir(dir)

def download_not_exist(url, command=None, filename=None):
    if url.startswith('gs://'):
        basename = os.path.basename(url) if url[-1] != '/' else os.path.basename(url[:-1])
        if os.path.exists(basename):
            print(f'{basename} exists, skipping download')
            return
        if command == None:
            command = 'gsutil -m cp -r'
        if filename == None:
            filename = '.'
    else:
        if command == None:
            command = 'wget -N'
        if filename == None:
            filename = ''
    subprocess.run(f'{command} {url} {filename}', shell=True, check=True)

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
    download_not_exist(GWAS_URL)
    os.chdir('..')
    # ClinVar
    print("Downloading ClinVar data into ClinVar folder")
    mkchdir("ClinVar")
    download_not_exist(CLINVAR_URL)
    os.chdir('..')
    # ENCODE
    print("Downloading ENCODE data files into ENCODE folder")
    #mkchdir("ENCODE")
    #from sirius.tools import automate_encode_upload
    #automate_encode_upload.download_search_files()
    # QYD: We use a prepared liftOver set of encode bed files
    download_not_exist(ENCODE_URL)
    #dbSNP
    print("Downloading dbSNP dataset in dbSNP folder")
    mkchdir('dbSNP')
    download_not_exist(DBSNP_URL)
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
    # Nature-Causal-Variants
    print("Downloading Nature-Causal-Variants data file in Nature-Causal-Variants folder")
    mkchdir("Nature-Causal-Variants")
    download_not_exist(NATURE_CASUAL_VARIANTS_URL)
    os.chdir('..')
    ## Roadmap Epigenomics
    print("Downloading Roadmap Epigenomics data in roadmap_epigenomics folder")
    mkchdir("roadmap_epigenomics")
    download_not_exist(ROADMAP_EPIGENOMICS_URL)
    os.chdir('..')
    # EFO
    print("Downloading the EFO Ontology data file")
    mkchdir("EFO")
    download_not_exist(EFO_URL)
    os.chdir('..')
    # HGNC
    print("Downloading HGNC data in HGNC folder")
    mkchdir("HGNC")
    download_not_exist(HGNC_URL)
    os.chdir('..')
    # KEGG
    print("Downloading KEGG data in KEGG folder")
    mkchdir("KEGG")
    download_not_exist(KEGG_URL)
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

def parse_upload_all_datasets(source_start=1):
    source_start = source_start or 1
    print("\n\n#3. Parsing and uploading each data set")
    os.chdir('gene_data_tmp')
    if source_start <= 1:
        print("*** 3.1 ENCODE_bigwig ***")
        os.chdir('encode_bigwig')
        parser = TSVParser_ENCODEbigwig(os.path.basename(ENCODE_BIGWIG_URL), verbose=True)
        parse_upload_data(parser, {"sourceurl": ENCODE_BIGWIG_URL})
        os.chdir('..')
    if source_start <= 2:
        print("\n*** 3.2 GRCh38_fasta ***")
        os.chdir('GRCh38_fasta')
        parser = FASTAParser(os.path.basename(GRCH38_FASTA_URL), verbose=True)
        parse_upload_data(parser, {"sourceurl": GRCH38_FASTA_URL})
        os.chdir('..')
    if source_start <= 3:
        print("\n*** 3.3 GRCh38_gff ***")
        os.chdir('GRCh38_gff')
        parse_upload_gff_chunk()
        os.chdir('..')
    if source_start <= 4:
        print("\n*** 3.4 ClinVar ***")
        os.chdir('ClinVar')
        parser = VCFParser_ClinVar('clinvar_20180128.vcf.gz', verbose=True)
        parse_upload_data(parser, {"sourceurl": CLINVAR_URL})
        os.chdir('..')
    if source_start <= 5:
        print("\n*** 3.5 ENCODE ***")
        os.chdir('ENCODE')
        from sirius.tools import automate_encode_upload
        if FULL_DATABASE:
            automate_encode_upload.parse_upload_files(0, 1070, liftover=False)
        else:
            automate_encode_upload.parse_upload_files(liftover=False)
        os.chdir('..')
    if source_start <= 6:
        print("\n*** 3.6 dbSNP ***")
        os.chdir('dbSNP')
        parse_upload_dbSNP_chunk()
        os.chdir('..')
    if source_start <= 7:
        print("\n*** 3.7 ExAC ***")
        os.chdir('ExAC')
        parse_upload_ExAC_chunk()
        os.chdir('..')
    if source_start <= 8:
        print("\n*** 3.8 TCGA ***")
        os.chdir('TCGA')
        parse_upload_TCGA_files()
        os.chdir('..')
    if source_start <= 9:
        print("\n*** 3.9 Nature-Causal-Variants ***")
        os.chdir('Nature-Causal-Variants')
        parser = Parser_NatureCasualVariants('nature13835-s1.csv', verbose=True)
        parse_upload_data(parser)
        os.chdir('..')
    if source_start <= 10:
        print("\n*** 3.10 Roadmap Epigenomics ***")
        os.chdir('roadmap_epigenomics')
        parse_upload_ROADMAP_EPIGENOMICS()
        os.chdir('..')
    ## The following dataset should be parsed in the end
    ## Because they "Patch" the existing data
    if source_start <= 11:
        print("\n*** 3.11 GWAS ***")
        os.chdir('gwas')
        parse_upload_GWAS()
        os.chdir('..')
    if source_start <= 12:
        print("\n*** 3.12 GTEx ***")
        os.chdir('GTEx')
        parse_upload_GTEx_files()
        os.chdir('..')
    if source_start <= 13:
        print("\n*** 3.13 EFO ***")
        os.chdir('EFO')
        parse_upload_EFO()
        os.chdir('..')
    if source_start <= 14:
        print("\n*** 3.14 HGNC ***")
        os.chdir('HGNC')
        parse_upload_HGNC()
        os.chdir('..')
    if source_start <= 15:
        print("\n*** 3.15 KEGG ***")
        os.chdir('KEGG')
        parse_upload_KEGG()
        os.chdir('..')
    # Finish
    print("All parsing and uploading finished!")
    os.chdir('..')

def parse_upload_gff_chunk():
    filename = os.path.basename(GRCH38_URL)
    parser = GFFParser_ENSEMBL(filename, verbose=True)
    parser.metadata['sourceurl'] = GRCH38_URL
    i_chunk = 0
    finished = False
    while finished is not True:
        finished = parser.parse_chunk()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        update_insert_many(InfoNodes, info_nodes[1:])
        print(f"Data of chunk {i_chunk} uploaded")
        i_chunk += 1
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
    finished = False
    while finished is not True:
        finished = parser.parse_chunk()
        print(f'Parsing and uploading chunk {i_chunk}')
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        i_chunk += 1
    # we only insert the infonode for dbSNP dataSource once
    update_insert_many(InfoNodes, info_nodes)

def parse_upload_ExAC_chunk():
    filename = os.path.basename(ExAC_URL)
    parser = VCFParser_ExAC(filename, verbose=True)
    parser.metadata['sourceurl'] = ExAC_URL
    i_chunk = 0
    finished = False
    while finished is not True:
        finished = parser.parse_chunk(100000)
        print(f'Parsing and uploading chunk {i_chunk}')
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes)
        i_chunk += 1
    # we only insert the infonode for ExAC dataSource once
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
    xml_files.sort()
    all_patient_infonodes = []
    # this is used in MAF parser
    patient_barcode_tumor_site = dict()
    # these are used in CNV parser
    patient_uuid_tumor_site = dict()
    patient_uuid_barcode = dict()
    print(f"Parsing {len(xml_files)} patient xml files")
    for f in xml_files:
        parser = TCGA_XMLParser(f, verbose=True)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        # record the tumor site for each patient barcode
        info = info_nodes[0]['info']
        patient_barcode = info['patient_barcode']
        patient_barcode_tumor_site[patient_barcode] = info['biosample']
        patient_uuid = info['patient_uuid']
        patient_uuid_tumor_site[patient_uuid] = info['biosample']
        patient_uuid_barcode[patient_uuid] = patient_barcode
        # collection individual info_nodes for each patient
        all_patient_infonodes += info_nodes
    # upload all patient info_nodes at once
    update_insert_many(InfoNodes, all_patient_infonodes)
    os.chdir('..')
    # MAF for mutations in tumors
    os.chdir('MAF')
    maf_files = []
    variant_tags = set()
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.maf.gz'):
                maf_files.append(os.path.join(root, f))
    maf_files.sort()
    print(f"Parsing {len(maf_files)} maf files")
    for i, f in enumerate(maf_files):
        parser = TCGA_MAFParser(f)
        # Parse in chunk since MAF files may be too large to fit in 16G memory
        i_chunk = 0
        finished = False
        while finished is not True:
            finished = parser.parse_chunk()
            print(f"{i:3d}-{i_chunk:2d} ", end='', flush=True)
            # provide the patient_barcode_tumor_site so the gnode will have 'info.biosample'
            genome_nodes, info_nodes, edges = parser.get_mongo_nodes(patient_barcode_tumor_site)
            # aggregate variant tags
            for gnode in genome_nodes:
                variant_tags.update(gnode['info']['variant_tags'])
            update_insert_many(GenomeNodes, genome_nodes)
            i_chunk += 1
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
    cnv_files.sort()
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
            patient_uuid = cnv_file_caseIDs[filebasename]
            biosample = patient_uuid_tumor_site.get(patient_uuid, None)
            patient_barcode = patient_uuid_barcode.get(patient_uuid, None)
            # some patient data are not available because they are in the "controlled access" catogory
            if biosample == None or patient_barcode == None: continue
            parser.parse()
            extra_info = {'patient_barcode': patient_barcode, 'biosample': biosample}
            genome_nodes, info_nodes, edges = parser.get_mongo_nodes(extra_info)
            batch_genome_nodes += genome_nodes
        update_insert_many(GenomeNodes, batch_genome_nodes)
        i_batch += 1
    # Add one info node for dataSource
    update_insert_many(InfoNodes, [{
        '_id': 'I' + DATA_SOURCE_TCGA,
        "type": "dataSource",
        'name':DATA_SOURCE_TCGA,
        "source": DATA_SOURCE_TCGA,
        'info': {
            'variant_tags': list(variant_tags)
        }
    }])
    # finish
    os.chdir('..')

def parse_upload_ROADMAP_EPIGENOMICS():
    filename = os.path.basename(ROADMAP_EPIGENOMICS_URL)
    print(f"Decompressing {filename}")
    subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    os.chdir('roadmap_sort')
    bedgz_files = sorted([f for f in os.listdir('.') if f.endswith('.bed.gz')])
    print(f"Parsing {len(bedgz_files)} .bed.gz files")
    for i, fname in enumerate(bedgz_files):
        print(f"{i:3d} {fname[:20]:20s} ", end='', flush=True)
        parser = BEDParser_ROADMAP_EPIGENOMICS(fname)
        parser.parse()
        genome_nodes, _, _ = parser.get_mongo_nodes()
        update_insert_many(GenomeNodes, genome_nodes, update=False)
    # Add one info node for dataSource
    update_insert_many(InfoNodes, [{
        '_id': 'I' + DATA_SOURCE_ROADMAP_EPIGENOMICS,
        "type": "dataSource",
        'name':DATA_SOURCE_ROADMAP_EPIGENOMICS,
        "source": DATA_SOURCE_ROADMAP_EPIGENOMICS,
        'info': {
            'filenames': bedgz_files
        }
    }])
    # finish
    os.chdir('..')

def parse_upload_GWAS():
    filename = 'gwas.tsv'
    parser = TSVParser_GWAS(filename, verbose=True)
    parser.parse()
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    # upload the dataSource info node
    update_insert_many(InfoNodes, info_nodes)
    update_insert_many(Edges, edges)
    # patch the GenomeNodes with the data source
    gids = list(parser.parsed_snp_ids)
    uresult = GenomeNodes.update_many({'_id': {'$in': gids}}, {'$addToSet': {'source': DATA_SOURCE_GWAS}})
    print(f"Prepared {len(gids)} and updated {uresult.matched_count} GenomeNodes with source {DATA_SOURCE_GWAS}")

def parse_upload_GTEx_files():
    filename = os.path.basename(GTEx_URL)
    # the big tar.gz file contains many individual data files
    print(f"Decompressing {filename}")
    subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    foldername = filename.split('.',1)[0]
    # aggregate all biosamples
    distinct_biosamples = set()
    for f in os.listdir(foldername):
        if f.endswith('egenes.txt.gz'):
            fname = os.path.join(foldername, f)
            print(f"Parsing and uploading from {fname}")
            parser = EQTLParser_GTEx(fname, verbose=True)
            parser.parse()
            # the first word in filename is parsed as the biosample
            biosample = f.split('.', 1)[0]
            # reformat to be consistent with ENCODE dataset
            biosample = ' '.join(biosample.lower().split('_'))
            distinct_biosamples.add(biosample)
            genome_nodes, info_nodes, edges = parser.get_mongo_nodes({'biosample': biosample})
            # we only insert the edges here for each file
            update_insert_many(Edges, edges)
            # patch the SNPs with the data source
            gids = list(parser.parsed_snp_ids) + list(parser.parsed_gene_ids)
            uresult = GenomeNodes.update_many({'_id': {'$in': gids}}, {'$addToSet': {'source': DATA_SOURCE_GTEX}})
            print(f"Prepared {len(gids)} and updated {uresult.matched_count} GenomeNodes with source {DATA_SOURCE_GTEX}")
    # change the filename to the big tar.gz file
    info_nodes[0]['info']['filename'] = filename
    info_nodes[0]['info']['biosample'] = list(distinct_biosamples)
    # insert one infonode for the GTEx dataSource
    update_insert_many(InfoNodes, info_nodes)

def parse_upload_EFO():
    filename = os.path.basename(EFO_URL)
    parser = OBOParser_EFO(filename, verbose=True)
    parser.parse()
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    # upload the dataSource info node
    update_skip_insert(InfoNodes, info_nodes)

def parse_upload_HGNC():
    filename = os.path.basename(HGNC_URL)
    parser = TSVParser_HGNC(filename, verbose=True)
    parser.parse()
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    # patch the gene GenomeNodes
    update_skip_insert(GenomeNodes, genome_nodes)
    # upload the dataSource info node
    update_insert_many(InfoNodes, info_nodes)

def parse_upload_KEGG():
    filename = os.path.basename(KEGG_URL)
    # the big tar.gz file contains many individual data files
    print(f"Decompressing {filename}")
    subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    foldername = 'kegg_pathways'
    # aggregate all pathways
    kegg_xmls = sorted([os.path.join(foldername, f) for f in os.listdir(foldername) if f.startswith('path') and f.endswith('.xml')])
    gene_in_paths = collections.defaultdict(list)
    all_pathway_infonodes = []
    for fname in kegg_xmls:
        parser = KEGG_XMLParser(fname)
        parser.parse()
        _, info_nodes, _ = parser.get_mongo_nodes()
        pathway = info_nodes[0]
        # aggregate all pathways for each gene
        for gene in pathway['info']['genes']:
            gene_in_paths[gene].append(pathway['name'])
        all_pathway_infonodes.append(pathway)
    update_insert_many(InfoNodes, all_pathway_infonodes)
    # prepare genome_nodes for patching
    existing_gene_name_id = dict()
    for gnode in GenomeNodes.find({'type': {'$in': ENSEMBL_GENE_SUBTYPES}}, projection=['_id', 'name']):
        existing_gene_name_id[gnode['name']] = gnode['_id']
    print(f"Pulling existing genes finished, total {len(existing_gene_name_id)} genes")
    genome_nodes = []
    for gene_name, path_names in gene_in_paths.items():
        if gene_name in existing_gene_name_id:
            genome_nodes.append({
                '_id': existing_gene_name_id[gene_name],
                'source': DATA_SOURCE_KEGG,
                'info': {
                    'kegg_pathways': path_names,
                }
            })
    update_skip_insert(GenomeNodes, genome_nodes)


def build_mongo_index():
    print("\n\n#4. Building index in data base")
    print("GenomeNodes")
    for idx in ['source', 'type', 'name']:
        print("Creating index %s" % idx)
        GenomeNodes.create_index(idx)
    for idx in ['info.targets', 'info.variant_tags', 'info.patient_barcodes', 'info.kegg_pathways', 'info.filenames']:
        print("Creating sparse index %s" % idx)
        GenomeNodes.create_index(idx, sparse=True)
    print("Creating compound index for 'info.biosample' and 'type'")
    GenomeNodes.create_index([('info.biosample', 1), ('type', 1)])
    # this compound index is created for the all variants track
    print("Creating compound index for 'contig', and 'start', 'type'")
    GenomeNodes.create_index([('contig', 1), ('start', 1), ('type', 1)])
    print("InfoNodes")
    for idx in ['source', 'type']:
        print("Creating index %s" % idx)
        InfoNodes.create_index(idx)
    for idx in ['info.biosample', 'info.targets', 'info.types', 'info.assay', 'info.outtype', 'info.variant_tags']:
        print("Creating sparse index %s" % idx)
        InfoNodes.create_index(idx, sparse=True)
    print("Creating text index 'name'")
    InfoNodes.create_index([('name', 'text')], default_language='english')
    print("Edges")
    for idx in ['from_id', 'to_id', 'type', 'source']:
        print("Creating index %s" % idx)
        Edges.create_index(idx)
    for idx in ['info.p-value', 'info.biosample', 'info.PICS_probability']:
        print("Creating sparse index %s" % idx)
        Edges.create_index(idx, sparse=True)

def patch_additional_info():
    # print("\n\n#5. Patching additional information")
    # from sirius.tools import patch_gene_info
    # patch_gene_info.patch_gene_ID_info()
    # we skip this becasue HGNC dataset can do a better job
    pass

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
5. Patch additional information

In Step 3, datasets are parsed and uploaded, in the following order:
1. ENCODE_bigwig
2. GRCh38_fasta
3. GRCh38_gff
4. ClinVar
5. ENCODE
6. dbSNP
7. ExAC
8. TCGA
9. Nature-Causal-Variants
10. Roadmap Epigenomics
11. GWAS
12. GTEx
13. EFO
14. HGNC
15. Kegg
'''

def main():
    print(Instruction)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--starting_step', type=int, default=1, help='Choose a step to start.')
    parser.add_argument('-c', '--continue_dataset', type=int, help='Choose a dataset to continue parsing and uploading. Will overwrite --starting_step to be 3')
    parser.add_argument('--del_tmp', action='store_true', help='Delete gene_data_tmp folder after finish.')
    parser.add_argument('--full', action='store_true', help='Build the full database (100x larger).')
    args = parser.parse_args()
    t0 = time.time()
    global FULL_DATABASE
    FULL_DATABASE = args.full
    if args.continue_dataset != None:
        args.starting_step = 3
    if args.starting_step <= 1:
        download_genome_data()
    if args.starting_step <= 2:
        drop_all_data()
    if args.starting_step <= 3:
        parse_upload_all_datasets(args.continue_dataset)
    if args.starting_step <= 4:
        build_mongo_index()
    if args.starting_step <= 5:
        patch_additional_info()
    if args.del_tmp:
        clean_up()
    t1 = time.time()

    hours, rem = divmod(int(t1 - t0), 3600)
    minutes, seconds = divmod(rem, 60)

    print("\n*** Congratulations! Rebuilding Entire Database Finished! ***")
    print(f"*** Total time cost:  {hours} hours {minutes} minutes {seconds} seconds ***")

if __name__ == "__main__":
    main()
