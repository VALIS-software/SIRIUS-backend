#!/usr/bin/env python

from sirius.parsers.GFFParser import GFFParser
from sirius.parsers.GWASParser import GWASParser
from sirius.parsers.EQTLParser import EQTLParser
from sirius.parsers.VCFParser import VCFParser_ClinVar, VCFParser_dbSNP

def update_insert_many(dbCollection, nodes):
    if not nodes: return
    all_ids = [node['_id'] for node in nodes if '_id' in node]
    ids_need_update = set([result['_id'] for result in dbCollection.find({'_id': {'$in': all_ids}}, projection=['_id'])])
    insert_nodes, update_nodes = [], []
    for node in nodes:
        if '_id' in node and node['_id'] in ids_need_update:
            update_nodes.append(node)
        else:
            node['source'] = [node['source']]
            insert_nodes.append(node)
    #import IPython
    #IPython.embed()
    #return
    if insert_nodes:
        try:
            dbCollection.insert_many(insert_nodes)
        except Exception as bwe:
            print(bwe.details)
            raise
    for node in update_nodes:
        filt = {'_id': node.pop('_id')}
        update = {'$push': {'source': node.pop('source')}}
        # merge the info key instead of overwrite
        if 'info' in node and isinstance(node['info'], dict):
            info_dict = node.pop('info')
            for key, value in info_dict.items():
                node['info.'+key] = value
        update['$set'] = node
        dbCollection.update_one(filt, update, upsert=True)
    print("%s finished. Updated: %d  Inserted: %d" % (dbCollection.name, len(update_nodes), len(insert_nodes)))

def main():
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
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        from sirius.mongo import GenomeNodes, InfoNodes, Edges
        print("Uploading to MongoDB")
        update_insert_many(GenomeNodes, genome_nodes)
        update_insert_many(InfoNodes, info_nodes)
        update_insert_many(Edges, edges)

if __name__ == "__main__":
    main()
