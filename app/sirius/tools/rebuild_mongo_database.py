#!/usr/bin/env python

import os, shutil, subprocess
from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.GWASParser import GWASParser
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP

GRCH38_URL = 'ftp://ftp.ncbi.nlm.nih.gov/refseq/H_sapiens/annotation/GRCh38_latest/refseq_identifiers/GRCh38_latest_genomic.gff.gz'
GWAS_URL = 'https://www.ebi.ac.uk/gwas/api/search/downloads/full'
EQTL_URL = 'http://www.exsnp.org/data/GSexSNP_allc_allp_ld8.txt'
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
    subprocess.check_call('gzip -d GRCh38_latest_genomic.gff.gz', shell=True)
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
    subprocess.check_call('wget '+EQTL_URL, shell=True)
    os.chdir('..')
    # ClinVar
    print("Downloading ClinVar data into ClinVar folder")
    os.mkdir("ClinVar")
    os.chdir("ClinVar")
    subprocess.check_call('wget '+CLINVAR_URL, shell=True)
    print("Decompressing")
    subprocess.check_call('gzip -d clinvar_20180128.vcf.gz', shell=True)
    os.chdir('..')
    print("All downloads finished")
    os.chdir('..')

def drop_all_data():
    " Drop all collections from database "
    print("#2. Deleting existing data.")
    for cname in db.list_collection_names():
        print("Dropping %s" % cname)
        db.drop_collection(cname)

def update_insert_many(dbCollection, nodes):
    if not nodes: return
    # append 'G', 'I', 'E' to all _id fields depend on type
    prefix = dbCollection.name[0]
    all_ids = []
    for node in nodes:
        # we require all nodes have _id field start with prefix
        assert node['_id'][0] == prefix
        all_ids.append(node['_id'])
    all_ids_need_update = set()
    # query the database in batches to find existing document with id
    batch_size = 100000
    for i_batch in range(int(len(all_ids) / batch_size)+1):
        batch_ids = all_ids[i_batch*batch_size:(i_batch+1)*batch_size]
        ids_need_update = set([result['_id'] for result in dbCollection.find({'_id': {'$in': batch_ids}}, projection=['_id'])])
        all_ids_need_update |= ids_need_update
    insert_nodes, update_nodes = [], []
    for node in nodes:
        if node['_id'] in all_ids_need_update:
            update_nodes.append(node)
        else:
            node['source'] = [node['source']]
            insert_nodes.append(node)
            # later node with the same id will be put into update_nodes
            all_ids_need_update.add(node['_id'])
    if insert_nodes:
        try:
            dbCollection.insert_many(insert_nodes)
        except Exception as bwe:
            print(bwe.details)
    for node in update_nodes:
        filt = {'_id': node.pop('_id')}
        update = {'$push': {'source': node.pop('source')}}
        # merge the info key instead of overwrite
        if 'info' in node and isinstance(node['info'], dict):
            info_dict = node.pop('info')
            for key, value in info_dict.items():
                node['info.'+key] = value
        update['$set'] = node
        try:
            dbCollection.update_one(filt, update, upsert=True)
        except Exception as bwe:
            print(bwe.details)
    print("%s finished. Updated: %d  Inserted: %d" % (dbCollection.name, len(update_nodes), len(insert_nodes)))

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
    print("All parsing and uploading finished!")
    os.chdir('..')

def parse_upload_gff_chunk():
    filename = 'GRCh38_latest_genomic.gff'
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
    for idx in ['source', 'assembly', 'type', 'chromid', 'start', 'end', 'length']:
        print("Creating index %s" % idx)
        GenomeNodes.create_index(idx)

    print("InfoNodes")
    for idx in ['source', 'type']:
        print("Creating index %s" % idx)
        InfoNodes.create_index(idx)
    print("Creating text index 'name'")
    InfoNodes.create_index([('name', 'text')], default_language='english')

    print("Edges")
    for idx in ['source', 'from_id', 'to_id', 'from_type', 'to_type', 'type', 'info.p-value']:
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
