#!/usr/bin/env python

import json
from sirius.core.QueryTree import QueryTree

with open('post.json') as jfile:
    query = json.load(jfile)
    print("Query Loaded from post.json")
    print(json.dumps(query, indent=2))

print("Printing each query in execution order")
qt = QueryTree(query, verbose=True)
result = list(qt.find())
for r in result:
    print(r)
print("Query finished. Found %d entries" % len(result))
