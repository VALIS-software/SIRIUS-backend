#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes
from sirius.parsers.EQTLParser import EQTLParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--url", help='sourceurl of data')
args = parser.parse_args()

parser = EQTLParser(args.filename, verbose=True)
parser.parse()
if args.url:
    parser.metadata['sourceurl'] = args.url
genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
EdgeNodes.insert_many(edge_nodes)
print("EdgeNodes finished")

