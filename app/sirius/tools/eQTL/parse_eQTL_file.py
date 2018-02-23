#!/usr/bin/env python

import os, sys
from sirius.parsers.EQTLParser import EQTLParser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
args = parser.parse_args()

filename = sys.argv[1]
fileext = os.path.splitext(filename)[1].lower()
parser = EQTLParser(filename, verbose=True)
parser.parse()
parser.save_json()
parser.save_mongo_nodes()
