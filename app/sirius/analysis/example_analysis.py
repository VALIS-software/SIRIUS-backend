#!/usr/bin/env python
# coding: utf-8

import time
from sirius.analysis.Bed import Bed
from sirius.helpers.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE

print("Analysis starts")
t0 = time.time()

trait = '"breast cancer"'
info_query = {'type': QUERY_TYPE_INFO, 'filters': {'$text': trait}}
edge_query = {'type': QUERY_TYPE_EDGE, 'filters': {'type': 'association', 'info.p-value': {'<': 0.05}}, 'toNode': info_query}
genome_query = {'type': QUERY_TYPE_GENOME, 'filters': {'type': 'SNP'}, 'limit': 0, 'toEdges': [edge_query]}

bed = Bed(genome_query)
t1 = time.time()
print(f'query1 SNPs related to {trait} returns {len(bed)} results')
print('--- %.2f seconds ---' % (t1 - t0))

biosample = 'small intestine'
gtype = 'Promoter-like'
gquery2 = {'type': QUERY_TYPE_GENOME, 'filters': {'type': gtype, 'info.biosample': biosample}, 'limit': 0}
bed2 = Bed(gquery2)
t2 = time.time()
print(f'query2 finding {gtype} in {biosample} returns {len(bed2)} results')
print('--- %.2f seconds ---' % (t2 - t1))

# w=window size, u=only keep intervals in bed
result = bed.window(bed2, w=5000, u=True)
t3 = time.time()
print(f'Intersect query1 with query2+5000window gives {len(result)} results')
print('--- %.2f seconds ---' % (t3 - t2))
