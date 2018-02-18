#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes
from sirius.parsers.GWASParser import GWASParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--url", help='sourceurl of data')
args = parser.parse_args()

fileext = os.path.splitext(args.filename)[1].lower()
if fileext == '.tsv':
    parser = GWASParser(args.filename, verbose=True)
else:
    raise NotImplemented("Parser for %s format not available yet." % fileext)
parser.parse()
if args.url:
    parser.metadata['sourceurl'] = args.url
genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
print("Uploading to MongoDB")
GenomeNodes.insert_many(genome_nodes)
print("GenomeNodes finished")
InfoNodes.insert_many(info_nodes)
print("InfoNodes finished")
EdgeNodes.insert_many(edge_nodes)
print("EdgeNodes finished")

InfoNodes.create_index('type')
for idx in ['from_id', 'to_id', 'from_type', 'to_type', 'type']:
    print("Creating index %s" % idx)
    EdgeNodes.create_index(idx)
