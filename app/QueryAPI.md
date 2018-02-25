# SIRIUS-backend Query API Specs

Yudong Qiu

Feb. 25th 2018

Notes: 
1. The query API may subject to major change during progress of development.
2. The query API is currently interfaced with endpoint `/annotation`, with a posted JSON dictionary.


## Query Dictionary Format  

The query dictionary represents a Tree structure that is executed from bottom to up.

Node and Edges are both represented by dictionaries, which is nested to a tree.


### Example graph:

                    ROOT
                     ||
                     \/ (Edge1)
                  [Node1]
                 // (or) \\
       (Edge2)  |/        \|  (Edge3)
            [Node2]      [Node3]


### In JSON dictionary:

```
ROOT =  {
          "type"    : <NodeType>,
          "filters" : <Filter>,
          "toEdges" : [ <Edge1> ]
        }

Edge1 = {
          "type"    : <EdgeType>,
          "filters" : <Filter>,
          "toNode"  : <Node1>
        }

Node1 = {
          "type": <NodeType>,
          "filters": <Filter>,
          "edgeRule": <EdgeRule>,
          "toEdges": [ <Edge2>, <Edge3> ]
        }

Edge2 = {
          "type": <EdgeType>,
          "filters": <Filter>,
          "toNode": <Node2>
        }

Node2 = {
          "type": <NodeType>,
          "filters": <Filter>
        }

Edge3 = {
          "type": <EdgeType>,
          "filters": <Filter>,
          "toNode": <Node3>
        }

Node3 = {
          "type": <NodeType>,
          "filters": <Filter>
        }
```

### Notes:

1. In this (pretty complicated) query, 6 dictionaries nested under the ROOT dictionary are used.
2. Current after query execution, the final search results for ROOT is returned. This can be updated later to return all results in each search level, if needed.
3. ROOT can be a Node, or also a Edge.
4. Node can connect to multiple edges, combined with `<edgeRule>`. Each Edge can only connect to one Node, because simple filters for multiple nodes can be combined. In a complicated case, one Edge connecting to multiple Nodes can still be represented by multiple Edges each connecting to one Node.


### Specs for query dictionary

#### Supported keys for Nodes:

`"type" : <NodeType>` | `<str>`, choices are "GenomeNode" or "InfoNode". Each type has its own available filters.

`"filters" : <Filter>` | `<dict>`
  
---------
  
##### Filter specs for GenomeNode
  
```
{
  "_id": ObjectId or <str>, (e.g. "snp_rs4950928" or "geneid_100287102")
  "assembly": "GRCh38",
  "type": "gene" | "exon" | "transcript" | "SNP" | "variant" | ... ,
  "start": <int: startBP>,
  "end": <int: endBP>,
  "length": <int>,
  "info.GeneID": <str>, (e.g. "100287102")
}
```

In addition to matching the exact value, like "start": 123, it is useful to match multiple values, with operators.
    
For example, `"start": {">=": 123}`, will match all results with start >= 123.
    
Supported filter operators for GenomeNodes:
    
```[ ">", "<", ">=", "<=", "==", "!="]```
    
Logical operators can also be used at any level in filter. e.g. `"start": {"$or": [ {">":123}, {"<": 100} ]}`
    
Supported logical operators: (Check MongoDB query operators for more)

```["$and", "$or", "$not", "$nor"]```
    
------------------

##### Filter specs for InfoNode

```
{
  "_id": <str>, (e.g. "trait_ykl-40 levels")
  "type": "trait" | ... (to be added)
  "name": <str>, (e.g. 'ykl-40 levels')
}
```

Same filter operators and logical operators are supported here, with one addition:

for "name", the operator `"$contains"` can be used, to match any string value that contains a certain word. Case insensitive.

For example, `"name": {"$contains": "cancer"}` will match all InfoNodes with "name" like "breast cancer", "lung cancer", or "cancer"
  
-------------------

`"toEdges" : [ <Edge1>, .. ]` | list of <dict>, each dict being an Edge.

`"edgeRule": <EdgeRule>` | `<str>`, choices are `["and", "or", "not", "nor"]`, this is optional, with default being `"and"`




#### Supported keys for Edges:

`<NodeType>: "EdgeNode"` | (to determine this is an Edge)

`<Filter>: Dictionary`

-------------------------

##### Filter specs for Edge

```
{
  "type": <str>, (e.g. "association")
  "info.pvalue": <float> (e.g. 1e-13)
}
```

Same operators as for GenomeNodes can be used here, e.g. `"info.pvalue": {"<": 0.3}`

-------------------------
