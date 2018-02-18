#!/usr/bin/env python

import os, sys
from sirius.parsers.GWASParser import GWASParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
args = parser.parse_args()

filename = sys.argv[1]
fileext = os.path.splitext(filename)[1].lower()
if fileext == '.tsv':
    parser = GWASParser(filename, verbose=True)
else:
    raise NotImplemented("Parser for %s format not available yet." % fileext)
parser.parse()
parser.save_json()
parser.save_mongo_nodes()
