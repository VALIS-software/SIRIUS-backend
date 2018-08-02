#!/usr/bin/env python

from sirius.parsers import GFFParser_ENSEMBL
from sirius.parsers import TSVParser_GWAS, TSVParser_ENCODEbigwig, TSVParser_HGNC
from sirius.parsers import EQTLParser_GTEx
from sirius.parsers import VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers import BEDParser_ENCODE
from sirius.parsers import FASTAParser
from sirius.parsers import OBOParser_EFO
from sirius.parsers import TCGA_XMLParser, TCGA_MAFParser, TCGA_CNVParser
from sirius.mongo.upload import update_insert_many, update_skip_insert

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument('datatype', choices=['ensembl', 'gwas', 'clinvar', 'dbsnp', 'encode', 'fasta', 'efo', 'encode_bigwig', 'exac',
                                             'gtex', 'bcrxml', 'maf', 'cnv', 'hgnc'], help='What data are we parsing?')
    parser.add_argument("--url", help='sourceurl of data')
    parser.add_argument("--save", action='store_true', help='Save parsed file to disk')
    parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
    parser.add_argument("--skip_insert", action='store_true', help='Only update existing docs in MongoDB')
    args = parser.parse_args()

    ParserClass = {'ensembl': GFFParser_ENSEMBL, 'gwas': TSVParser_GWAS, 'clinvar': VCFParser_ClinVar,
                   'dbsnp': VCFParser_dbSNP, 'encode': BEDParser_ENCODE, 'fasta': FASTAParser, 'efo': OBOParser_EFO,
                   'encode_bigwig': TSVParser_ENCODEbigwig, 'exac': VCFParser_ExAC, 'gtex': EQTLParser_GTEx,
                   'bcrxml': TCGA_XMLParser, 'maf': TCGA_MAFParser, 'cnv': TCGA_CNVParser, 'hgnc': TSVParser_HGNC}

    parser = ParserClass[args.datatype](args.filename, verbose=True)

    parser.parse()

    if args.url:
        parser.metadata['sourceurl'] = args.url

    # set some metadata for demonstration, they should be downloaded from ENCODE website
    if args.datatype == 'encode':
        parser.metadata['assembly'] = 'hg19'
        parser.metadata['biosample'] = '#biosample#'
        parser.metadata['accession'] = '#accession#'
        parser.metadata['description'] = '#description#'
        parser.metadata['targets'] = ['#Target#']

    if args.save:
        parser.save_json()
        parser.save_mongo_nodes()

    if args.upload == True:
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        from sirius.mongo import GenomeNodes, InfoNodes, Edges
        if not args.skip_insert:
            print("Uploading to MongoDB")
            update_insert_many(GenomeNodes, genome_nodes)
            update_insert_many(InfoNodes, info_nodes)
            update_insert_many(Edges, edges)
        else:
            print("Updating existing docs in MongoDB")
            update_skip_insert(GenomeNodes, genome_nodes)
            update_skip_insert(InfoNodes, info_nodes)
            update_skip_insert(Edges, edges)

if __name__ == "__main__":
    main()
