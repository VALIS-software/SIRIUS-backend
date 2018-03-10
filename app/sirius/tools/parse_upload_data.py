#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes
from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.GWASParser import GWASParser
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument('datatype', choices=['gff', 'gwas', 'eqtl', 'clinvar', 'dbsnp'], help='What data are we parsing?')
parser.add_argument("--url", help='sourceurl of data')
parser.add_argument("--save", action='store_true', help='Save parsed file to disk')
parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
args = parser.parse_args()

ParserClass = {'gff': GFFParser, 'gwas': GWASParser, 'eqtl': EQTLParser, 'clinvar': VCFParser_ClinVar, 'dbsnp': VCFParser_dbSNP}

parser = ParserClass[args.datatype](args.filename, verbose=True)

parser.parse()

if args.url:
    parser.metadata['sourceurl'] = args.url

if args.save:
    parser.save_json()
    parser.save_mongo_nodes()

if args.upload == True:
    genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
    from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes
    print("Uploading to MongoDB")
    if genome_nodes:
        GenomeNodes.insert_many(genome_nodes)
        print("GenomeNodes finished")
    if info_nodes:
        InfoNodes.insert_many(info_nodes)
        print("InfoNodes finished")
    if edge_nodes:
        EdgeNodes.insert_many(edge_nodes)
        print("EdgeNodes finished")
