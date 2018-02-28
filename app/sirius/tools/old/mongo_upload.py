#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes
from sirius.parsers.GFFParser import GFFParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--url", help='sourceurl of data')
args = parser.parse_args()

fileext = os.path.splitext(args.filename)[1].lower()
if fileext == '.gff':
    parser = GFFParser(args.filename, verbose=True)
else:
    raise NotImplemented("Parser for %s format not available yet." % fileext)
parser.parse()
if args.url:
    parser.metadata['sourceurl'] = args.url
print("Uploading to MongoDB")
GenomeNodes.insert_many(parser.get_genomenodes())
for idx in ['assembly', 'type', 'location', 'start', 'end', 'length', 'info.GeneID']:
    print("Creating index %s" % idx)
    GenomeNodes.create_index(idx)
