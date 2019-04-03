#!/usr/bin/env python

import os
import shutil
import subprocess
import json
import time
import collections

from sirius.mongo import GenomeNodes, InfoNodes, Edges, db
from sirius.mongo.upload import update_insert_many, update_skip_insert

def drop_all_data():
    for cname in db.list_collection_names():
        db.drop_collection(cname)

def build_mongo_index():
    # === Genome Node indexes ===
    for idx in ['type', 'name']:
        GenomeNodes.create_index(idx)
    for idx in ['info.targets', 'info.variant_tags', 'info.patient_id', 'info.gene', 'info.filename']:
        # sparse indexes
        GenomeNodes.create_index(idx, sparse=True)

    # compound index on source, type
    GenomeNodes.create_index([('source', 1), ('type', 1)])
    # compound index on biosample, type
    GenomeNodes.create_index([('info.biosample', 1), ('type', 1)])
    # compound index on genomic interval
    GenomeNodes.create_index([('contig', 1), ('start', 1), ('type', 1)])
    
    # === Info indexes ===
    for idx in ['source', 'type']:
        InfoNodes.create_index(idx)
    for idx in ['info.biosample', 'info.targets', 'info.types', 'info.assay', 'info.variant_tags']:
        # sparse indexes
        InfoNodes.create_index(idx, sparse=True)
    # Text search index
    InfoNodes.create_index([('name', 'text')], default_language='english')
    

    # ==== Edge indexes ==== 
    for idx in ['from_id', 'to_id', 'type', 'source']:
        Edges.create_index(idx)
    for idx in ['info.p-value', 'info.biosample']:
        # sparse indexes
        Edges.create_index(idx, sparse=True)


def main():
    t0 = time.time()
    drop_all_data()
    """
    TODO: Place your data upload code here.
    for dataset in []:
        genome_nodes = []
        info_nodes = []
        edges = []
        update_insert_many(GenomeNodes, genome_nodes)
        update_insert_many(InfoNodes, info_nodes)
        update_insert_many(Edges, edges)
    """
    build_mongo_index()
    t1 = time.time()

    hours, rem = divmod(int(t1 - t0), 3600)
    minutes, seconds = divmod(rem, 60)

    print("\n*** Congratulations! Rebuilding db finished! ***")
    print(f"*** Total time:  {hours} hours {minutes} minutes {seconds} seconds ***")

if __name__ == "__main__":
    main()