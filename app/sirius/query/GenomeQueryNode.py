from sirius.mongo import GenomeNodes

class GenomeQueryNode(object):
    def __init__(self, qfilter=dict(), edges=None, edge_rule=None, limit=0, verbose=False):
        self.filter = qfilter
        self.edges = [] if edges == None else edges
        # edge_rule: 0 means "and", 1 means "or", 2 means "not"
        self.edge_rule = 0 if edge_rule == None else edge_rule
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, projection=None):
        """
        Find all nodes from GenomeNodes, based on self.filter and the edge connected.
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
        if projection != None:
            return GenomeNodes.find(query, limit=self.limit, projection=projection)
        else:
            return GenomeNodes.find(query, limit=self.limit)

    def findid(self):
        """
        Find all nodes from GenomeNodes, based on self.filter and the edge connected
        Return a set that contain strings of node['_id']
        """
        mongo_filter = self.filter.copy()
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
            mongo_filter['_id'] = {"$in": list(result_ids)}
        if self.verbose == True:
            print(mongo_filter)
        # this is faster but do not have the limit option
        # return set(GenomeNodes.distinct('_id', mongo_filter)
        return set(d['_id'] for d in GenomeNodes.find(mongo_filter, {'_id':1}, limit=self.limit))