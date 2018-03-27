#!/usr/bin/env python

from sirius.mongo import GenomeNodes, InfoNodes, Edges

print("GenomeNodes")
for idx in ['source', 'assembly', 'type', 'chromid', 'start', 'end', 'length']:
    print("Creating index %s" % idx)
    GenomeNodes.create_index(idx)

print("InfoNodes")
print("Creating index type")
for idx in ['source', 'type']:
    print("Creating index %s" % idx)
    InfoNodes.create_index(idx)
print("Creating text index 'name'")
InfoNodes.create_index([('name', 'text')], default_language='english')

print("Edges")
for idx in ['source', 'from_id', 'to_id', 'from_type', 'to_type', 'type', 'info.p-value']:
    print("Creating index %s" % idx)
    Edges.create_index(idx)
