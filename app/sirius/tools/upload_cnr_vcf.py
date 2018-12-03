#!/usr/bin/env python

import os
import argparse
from sirius.parsers.vcf_parser import VCFParser_VEP
from sirius.core.user_files import create_collection_user_file

def parse_upload_user_file(filename, username):
    parser = VCFParser_VEP(filename, verbose=True)
    uid = 'user_' + username
    collection = create_collection_user_file(parser, uid, filename, 'vcf')
    collection.create_index('info.variant_tags')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='Input VCF file from CNR pipeline after VEP')
    parser.add_argument('--username', default='cnr@valis.bio', help='valis user that own this data file')
    args = parser.parse_args()

    parse_upload_user_file(args.infile, args.username)

if __name__ == '__main__':
    main()