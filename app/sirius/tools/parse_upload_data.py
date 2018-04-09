#!/usr/bin/env python

from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.GWASParser import GWASParser
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP
from sirius.parsers.BEDParser import BEDParser
from sirius.mongo.upload import update_insert_many

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument('datatype', choices=['gff', 'gwas', 'eqtl', 'clinvar', 'dbsnp','bed'], help='What data are we parsing?')
    parser.add_argument("--url", help='sourceurl of data')
    parser.add_argument("--save", action='store_true', help='Save parsed file to disk')
    parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
    parser.add_argument("--cell_type", help='cell-type for ENCODE data (bed type)')
    args = parser.parse_args()

    ParserClass = {'gff': GFFParser, 'gwas': GWASParser, 'eqtl': EQTLParser, 'clinvar': VCFParser_ClinVar, 'dbsnp': VCFParser_dbSNP, 'bed': BEDParser}

    parser = ParserClass[args.datatype](args.filename, verbose=True)

    parser.parse()

    if args.url:
        parser.metadata['sourceurl'] = args.url

    if args.cell_type:
        parser.metadata['cell-type'] = args.cell_type

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
