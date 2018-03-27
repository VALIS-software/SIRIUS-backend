#!/usr/bin/env python

import os, sys
import argparse
import json
from sirius.parsers.GFFParser import GFFParser
from sirius.mongo import GenomeNodes, InfoNodes

def update_insert_many(dbCollection, nodes):
    if not nodes: return
    all_ids = [node['_id'] for node in nodes if '_id' in node]
    ids_need_update = [result['_id'] for result in dbCollection.find({'_id': {'$in': all_ids}}, projection=['_id'])]
    insert_nodes, update_nodes = [], []
    for node in nodes:
        if '_id' in node and node['_id'] in ids_need_update:
            update_nodes.append(node)
        else:
            node['source'] = [node['source']]
            insert_nodes.append(node)
    if insert_nodes:
        dbCollection.insert_many(insert_nodes)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--url", help='sourceurl of data')
    parser.add_argument("--save", action='store_true', help='Save parsed file to disk')
    parser.add_argument("--upload", action='store_true', help='Upload to MongoDB')
    args = parser.parse_args()

    parser = GFFParser(args.filename, verbose=True)
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
        prev_parser = parser
        with open(fname) as chunkfile:
            parser.load_json(chunkfile)
            if args.url:
                parser.metadata['sourceurl'] = args.url
            genome_nodes, info_nodes, edge_nodes = parser.get_mongo_nodes()
            if args.save:
                parser.save_mongo_nodes()
            if args.upload:
                update_insert_many(GenomeNodes, genome_nodes)
        print("Data from %s uploaded" % fname)
    # we only upload info_nodes once here because all the chunks has the same single info node for the dataSource.
    update_insert_many(InfoNodes, info_nodes)
    print("InfoNodes uploaded")

if __name__ == '__main__':
    main()

