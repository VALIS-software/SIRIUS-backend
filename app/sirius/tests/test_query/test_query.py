#!/usr/bin/env python

import sys
import json
from sirius.core.QueryTree import QueryTree

with open(sys.argv[1]) as jfile:
    query = json.load(jfile)
    print("Query Loaded from %s"%sys.argv[1])
    print(json.dumps(query, indent=2))

print("Printing each query in execution order")
qt = QueryTree(query, verbose=True)
result = list(qt.find())
for r in result:
    print(r)
print("Query finished. Found %d entries" % len(result))
