import copy
from sirius.mongo import InfoNodes

def intersect_id_filter_set(id_filter, id_set):
    """ Intersect the '_id' field of a mongo filter with a set of ids """
    assert isinstance(id_set, set)
    if not id_set:
        return []
    elif id_filter is None:
        return list(id_set)
    elif isinstance(id_filter, str):
        return [id_filter] if id_filter in id_set else None
    elif isinstance(id_filter, dict):
        if '$in' in id_filter:
            id_set.intersection(id_filter.pop('$in'))
        return list(id_set)
    else:
        return []

class InfoQueryNode(object):
    def __init__(self, mongo_collection=None, qfilter=None, edges=None, edge_rule=None, limit=0, verbose=False):
        self.mongo_collection = mongo_collection if mongo_collection else InfoNodes
        self.filter = qfilter if qfilter else dict()
        self.edges = [] if edges == None else edges
        # edge_rule: 0 means "and", 1 means "or", 2 means "not"
        self.edge_rule = 0 if edge_rule == None else edge_rule
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, projection=None):
        """
        Find all nodes from self.mongo_collection, based on self.filter and the edge connected.
        Return a cursor of MongoDB.find() query, or an empty list if none found
        """
        mongo_filter = copy.deepcopy(self.filter)
        if len(self.edges) > 0:
            first_edgenode = self.edges[0]
            result_id_set = first_edgenode.find_from_id()
            if len(result_id_set) == 0 and self.edge_rule != 1:
                return []
            for edgenode in self.edges[1:]:
                e_ids = edgenode.find_from_id()
                if self.edge_rule == 0: # AND
                    result_id_set &= e_ids
                    if len(result_id_set) == 0: return []
                elif self.edge_rule == 1: # OR
                    result_id_set |= e_ids
                elif self.edge_rule == 2: # NOT
                    result_id_set -= e_ids
                    if len(result_id_set) == 0: return []
            if len(result_id_set) == 0: return []
            # intersect the ids from edges with the ids from filter
            id_filter = mongo_filter.pop('_id', None)
            intersect_ids = intersect_id_filter_set(id_filter, result_id_set)
            if not intersect_ids:
                return []
            elif len(intersect_ids) == 1:
                mongo_filter['_id'] = intersect_ids[0]
            else:
                mongo_filter['_id'] = {"$in": intersect_ids}
        if self.verbose == True:
            print(mongo_filter)
        return self.mongo_collection.find(mongo_filter, limit=self.limit, projection=projection, no_cursor_timeout=True)

    def distinct(self, key):
        if not self.edges:
            result = self.mongo_collection.distinct(key, self.filter, maxTimeMS=15000)
        else:
            result = self.find().distinct(key)
        return result

    def findid(self):
        """
        Find all nodes from self.mongo_collection, based on self.filter and the edge connected
        Return a set that contain strings of node['_id']
        """
        mongo_filter = self.filter.copy()
        if len(self.edges) > 0:
            first_edgenode = self.edges[0]
            result_ids = first_edgenode.find_from_id()
            if len(result_ids) == 0 and self.edge_rule != 1:
                return set()
            for edgenode in self.edges[1:]:
                e_ids = edgenode.find_from_id()
                if self.edge_rule == 0: # AND
                    result_ids &= e_ids
                    if len(result_ids) == 0:
                        return set()
                elif self.edge_rule == 1: # OR
                    result_ids |= e_ids
                elif self.edge_rule == 2: # NOT
                    result_ids -= e_ids
                    if len(result_ids) == 0:
                        return set()
            # intersect the ids from edges with the ids from filter
            id_filter = mongo_filter.pop('_id', None)
            intersect_ids = intersect_id_filter_set(id_filter, result_ids)
            if not intersect_ids:
                return set()
            elif len(intersect_ids) == 1:
                mongo_filter['_id'] = intersect_ids[0]
            else:
                mongo_filter['_id'] = {"$in": intersect_ids}
        if self.verbose == True:
            print(mongo_filter)
        return set(d['_id'] for d in self.mongo_collection.find(mongo_filter, {'_id':1}, limit=self.limit))

    def export(self, filename, ftype):
        raise NotImplementedError("Exporting InfoQuery is not implemented yet.")