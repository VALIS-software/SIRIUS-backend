#!/usr/bin/env python

from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes

print("GenomeNodes")
for idx in ['assembly', 'type', 'chromid', 'start', 'end', 'length', 'info.GeneID']:
    print("Creating index %s" % idx)
    GenomeNodes.create_index(idx)

print("InfoNodes")
print("Creating index type")
InfoNodes.create_index('type')
print("Creating text index name")
InfoNodes.create_index([('name', 'text')], default_language='english')

print("EdgeNodes")
for idx in ['from_id', 'to_id', 'from_type', 'to_type', 'type', 'info.pvalue']:
    print("Creating index %s" % idx)
    EdgeNodes.create_index(idx)
