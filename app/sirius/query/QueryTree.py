import os, sys
from sirius.helpers.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE
from sirius.query import GenomeQueryNode, InfoQueryNode, QueryEdge

class QueryTree(object):
    Query_operators = {'>':'$gt', '>=':'$gte', '<':'$lt', '<=':'$lte', '=':'$eq', '==':'$eq', '!=':'$ne'}
    EdgeRules = {'and': 0, 'or': 1, 'not': 2}
    def __init__(self, query=dict(), verbose=False):
        self.verbose = verbose
        if query:
            self.head = self.build_recur(query)

    def build_recur(self, query):
        if not query: return None
        typ = query['type']
        qfilter = self.build_filter(query['filters'])
        limit = query.get('limit', 100000) # default limit can finish in 1s
        if typ == QUERY_TYPE_GENOME:
            edgeRule = self.EdgeRules[query.get('edgeRule', 'and')]
            edges = [self.build_recur(d) for d in query.get('toEdges', [])]
            # genome arithmetics
            arithmetics = self.build_arithmetics(query.get('arithmetics', []))
            resultNode = GenomeQueryNode.GenomeQueryNode(qfilter, edges, edgeRule, arithmetics, limit)
        elif typ == QUERY_TYPE_INFO:
            edgeRule = self.EdgeRules[query.get('edgeRule', 'and')]
            edges = [self.build_recur(d) for d in query.get('toEdges', [])]
            resultNode = InfoQueryNode.InfoQueryNode(qfilter, edges, edgeRule, limit)
        elif typ == QUERY_TYPE_EDGE:
            nextnode = self.build_recur(query.get('toNode', None))
            resultNode = QueryEdge.QueryEdge(qfilter, nextnode, limit)
        else:
            raise NotImplementedError("Query with type %s not implemented yet." % query['type'])
        resultNode.verbose = self.verbose
        return resultNode

    def build_filter(self, dfilter=None):
        """ Parse a filter dictionary to match MongoDB query language """
        if not dfilter: return dict()
        result = dict()
        for key, value in dfilter.items():
            if isinstance(value, dict):
                new_value = dict()
                for k,v in value.items():
                    new_k = self.Query_operators.get(k, k)
                    new_v = v
                    new_value[new_k] = new_v
                result[key] = new_value
            elif key == '$text':
                result['$text'] = {'$search': '\"' + value + '\"'}
            else:
                result[key] = value
        return result

    def build_arithmetics(self, arithmetics):
        """ Parse a list of arithemics dictionary and build target Query Objects """
        result = []
        for orig_ar in arithmetics:
            ar = dict()
            assert orig_ar['operator'] in {'intersect', 'window', 'union'}
            ar['operator'] = orig_ar['operator']
            ar['targets'] = [self.build_recur(query) for query in orig_ar.get('target_queries', [])]
            # operator-specific settings and checks
            if ar['operator'] == 'window':
                assert len(ar['targets']) > 0
                ar['windowSize'] = orig_ar.get('windowSize', 1000)
            elif ar['operator'] in ('intersect', 'union'):
                assert len(ar['targets']) > 0
            result.append(ar)
        return result

    def find(self, projection=None):
        return self.head.find(projection=projection)

    def distinct(self, key):
        return self.head.distinct(key)

