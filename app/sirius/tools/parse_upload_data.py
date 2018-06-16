#!/usr/bin/env python

from sirius.parsers.GFFParser import GFFParser_ENSEMBL
from sirius.parsers.TSVParser import TSVParser_GWAS, TSVParser_ENCODEbigwig
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers.BEDParser import BEDParser_ENCODE
from sirius.parsers.FASTAParser import FASTAParser
from sirius.parsers.OBOParser import OBOParser_EFO
from sirius.parsers.MAFParser import MAFParser
from sirius.mongo.upload import update_insert_many

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument('datatype', choices=['ensembl', 'gwas', 'eqtl', 'clinvar', 'dbsnp', 'encode', 'fasta', 'efo', 'encode_bigwig', 'exac', 'maf'], help='What data are we parsing?')
    parser.add_argument("--url", help='sourceurl of data')
    parser.add_argument("--save", action='store_true', help='Save parsed file to disk')
    parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
    args = parser.parse_args()

    ParserClass = {'ensembl': GFFParser_ENSEMBL, 'gwas': GWASParser, 'eqtl': EQTLParser, 'clinvar': VCFParser_ClinVar,
                   'dbsnp': VCFParser_dbSNP, 'encode': BEDParser_ENCODE, 'fasta': FASTAParser, 'efo': OBOParser_EFO,
                   'encode_bigwig': TSVParser_ENCODEbigwig, 'exac': VCFParser_ExAC, 'maf': MAFParser}

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
        print("Uploading to MongoDB")
        update_insert_many(GenomeNodes, genome_nodes)
        update_insert_many(InfoNodes, info_nodes)
        update_insert_many(Edges, edges)

if __name__ == "__main__":
    main()
