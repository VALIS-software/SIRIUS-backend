#!/usr/bin/env python

import os, sys
from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes
from sirius.realdata.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE

class KeyDict(dict):
    def __missing__(self, key):
        return key


class NonDict(dict):
    def __missing__(self, key):
        return None


class QueryNode:
    def __init__(self, pool=None, qfilter=dict(), edges=None, edge_rule=None, limit=0, verbose=False):
        self.pool = pool
        self.filter = qfilter
        self.edges = [] if edges == None else edges
        # edge_rule: 0 means "and", 1 means "or", 2 means "not"
        self.edge_rule = 0 if edge_rule == None else edge_rule
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, sort=None):
        """
        Find all nodes from self.pool, based on self.filter and the edge connected.
        Return a cursor of MongoDB.find() query, or an empty list if none found
        """
        query = self.filter.copy()
        if len(self.edges) > 0:
            first_edgenode = self.edges[0]
            result_ids = set(first_edgenode.find_from_id())
            for edgenode in self.edges[1:]:
                if len(result_ids) == 0: break
                e_ids = set(edgenode.find_from_id())
                if self.edge_rule == 0: # AND
                    result_ids &= e_ids
                elif self.edge_rule == 1: # OR
                    result_ids |= e_ids
                elif self.edge_rule == 2: # NOT
                    result_ids -= e_ids
            if len(result_ids) == 0: return []
            query['_id'] = {"$in": list(result_ids)}
        if self.verbose == True:
            print(query)
        if sort != None:
            return self.pool.find(query, limit=self.limit).sort(sort)
        else:
            return self.pool.find(query, limit=self.limit)

    def findid(self):
        """
        Find all nodes from self.pool, based on self.filter and the edge connected
        Return a set that contain strings of node['_id']
        """
        query = self.filter.copy()
        if len(self.edges) > 0:
            first_edgenode = self.edges[0]
            result_ids = set(first_edgenode.find_from_id())
            for edgenode in self.edges[1:]:
                if len(result_ids) == 0: break
                e_ids = set(edgenode.find_from_id())
                if self.edge_rule == 0: # AND
                    result_ids &= e_ids
                elif self.edge_rule == 1: # OR
                    result_ids |= e_ids
                elif self.edge_rule == 2: # NOT
                    result_ids -= e_ids
            if len(result_ids) == 0: return set()
            query['_id'] = {"$in": list(result_ids)}
        if self.verbose == True:
            print(query)
        return set(d['_id'] for d in self.pool.find(query, {'_id':1}, limit=self.limit))


class QueryEdge:
    def __init__(self, pool=None, qfilter=dict(), nextnode=None, limit=0, verbose=False):
        self.pool = pool
        self.filter = qfilter
        self.nextnode = nextnode
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, sort=None):
        """
        Find all EdgeNodes from self.pool, based on self.filter, and the next node I connect to
        Return a cursor of MongoDB.find() query, or an empty list if Nothing found
        """
        query = self.filter.copy()
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0:
                return []
            query['to_id'] = {'$in': target_ids}
        if self.verbose == True:
            print(query)
        if sort != None:
            return self.pool.find(query, limit=self.limit).sort(sort)
        else:
            return self.pool.find(query, limit=self.limit)

    def find_from_id(self):
        """
        Find all EdgeNodes from self.pool, based on self.filter, and the next node I connect to
        Return a set that contain strings of edgenode['from_id']
        """
        query = self.filter.copy()
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0: return set()
            query['to_id'] = {'$in': target_ids}
        if self.verbose == True:
            print(query)
        return set(d['from_id'] for d in self.pool.find(query, {'from_id':1}, limit=self.limit))


class QueryTree:
    operator_translate = [('>', '$gt'), ('>=', '$gte'), ('<', '$lt'), ('<=', '$lte'), ('=', '$eq'), ('==', '$eq'), ('!=', '$ne')]
    Query_operators = KeyDict(operator_translate)
    EdgeRules = {'and': 0, 'or': 1, 'not': 2, None: 0}
    def __init__(self, query=dict(), verbose=False):
        self.verbose = verbose
        if query:
            self.constuct(query)

    def constuct(self, query):
        self.head = self.build_recur(query)

    def build_recur(self, query):
        if not query: return None
        query = NonDict(query)
        typ = query['type']
        qfilter = self.build_filter(query['filters'])
        limit = query['limit'] if 'limit' in query else 100000 # 100000 by default can finish in 1s
        if typ == QUERY_TYPE_EDGE:
            nextnode = self.build_recur(query['toNode'])
            resultNode = QueryEdge(EdgeNodes, qfilter, nextnode, limit)
        elif typ == QUERY_TYPE_GENOME or typ == QUERY_TYPE_INFO:
            edgeRule = self.EdgeRules[query['edgeRule']]
            if 'toEdges' in query:
                edges = [self.build_recur(d) for d in query['toEdges']]
            else:
                edges = []
            if typ == QUERY_TYPE_GENOME:
                resultNode = QueryNode(GenomeNodes, qfilter, edges, edgeRule, limit)
            else:
                resultNode = QueryNode(InfoNodes, qfilter, edges, edgeRule, limit)
        else:
            raise NotImplementedError("Query with type %s not implemented yet." % query['type'])
        resultNode.verbose = self.verbose
        return resultNode

    def build_filter(self, dfilter=None):
        """ Parse a filter dictionary to match MongoDB query language """
        if not dfilter: return dict()
        result = dict()
        text_key = None
        for key, value in dfilter.items():
            #key = key.lower()
            if isinstance(value, dict):
                new_value = dict()
                for k,v in value.items():
                    # handle "$contain" operator here
                    if k.startswith('$contain'):
                        if text_key == None:
                            result['$text'] = {'$search': '\"'+v+'\"'}
                            text_key = key
                        else:
                            print("Error, only one text key can exist in a filter")
                    else:
                        new_k = self.Query_operators[k]
                        new_v = v#.lower() if isinstance(v, str) else v
                        new_value[new_k] = new_v
                result[key] = new_value
            else:
                result[key] = value
        if text_key in result:
            result.pop(text_key)
        return result

    def find(self, sort=None):
        return self.head.find(sort)


