from sirius.mongo import Edges

class QueryEdge(object):
    def __init__(self, qfilter=dict(), nextnode=None, limit=0, verbose=False):
        self.filter = qfilter
        self.nextnode = nextnode
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, projection=None):
        """
        Find all Edges from Edges, based on self.filter, and the next node I connect to
        Return a cursor of MongoDB.find() query, or an empty list if Nothing found
        """
        mongo_filter = self.filter.copy()
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0:
                return []
            mongo_filter['to_id'] = {'$in': target_ids}
        if self.verbose == True:
            print(query)
        return Edges.find(mongo_filter, limit=self.limit, projection=projection, no_cursor_timeout=True)

    def distinct(self, key):
        return self.find().distinct(key)

    def find_from_id(self):
        """
        Find all Edges from Edges, based on self.filter, and the next node I connect to
        Return a set that contain strings of edgenode['from_id']
        """
        mongo_filter = self.filter.copy()
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0: return set()
            mongo_filter['to_id'] = {'$in': target_ids}
        if self.verbose == True:
            print(mongo_filter)
        return set(d['from_id'] for d in Edges.find(mongo_filter, {'from_id':1}, limit=self.limit))
