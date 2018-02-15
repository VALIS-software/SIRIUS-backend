#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes
from sirius.parsers.GFFParser import GFFParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("-g", "--gnode", help='Save GenomeNode as Json in file')

args = parser.parse_args()

filename = sys.argv[1]
fileext = os.path.splitext(filename)[1].lower()
if fileext == '.gff':
    parser = GFFParser(filename, verbose=True)
else:
    raise NotImplemented("Parser for %s format not available yet." % fileext)
parser.parse()
parser.save_json()
if args.gnode:
    parser.save_gnode(args.gnode)
