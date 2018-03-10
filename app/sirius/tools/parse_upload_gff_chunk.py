#!/usr/bin/env python

import os, sys
import argparse
import json
from sirius.parsers.GFFParser import GFFParser
from sirius.mongo import GenomeNodes

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--url", help='sourceurl of data')
parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
args = parser.parse_args()

parser = GFFParser(args.filename, verbose=True)

if args.url:
    parser.metadata['sourceurl'] = args.url

chunk_fnames = parser.parse_save_data_in_chunks()

# parse and upload data in chunks to reduce memory usage
prev_parser = None
for fname in chunk_fnames:
    # we still want the original filename for each chunk
    parser = GFFParser(args.filename)
    # the GFF data set are sequencially depending on each other
    # so we need to inherit some information from previous parser
    if prev_parser != None:
        parser.seqid_loc = prev_parser.seqid_loc
        parser.gene_id_set = prev_parser.gene_id_set
        del prev_parser
    with open(fname) as chunkfile:
        parser.load_json(chunkfile)
        genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
        # GFF data only has genome_nodes
        if args.upload:
            try:
                GenomeNodes.insert_many(genome_nodes)
                print("GenomeNodes from %s uploaded" % fname)
            except Exception as e:
                print(e)
    prev_parser = parser
