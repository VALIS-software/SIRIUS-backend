from sirius.mongo import Edges

class QueryEdge(object):
    def __init__(self, mongo_collection=None, qfilter=None, nextnode=None, reverse=False, limit=0, verbose=False):
        self.mongo_collection = mongo_collection if mongo_collection else Edges
        self.filter = qfilter if qfilter else dict()
        self.nextnode = nextnode
        self.reverse = reverse
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, projection=None):
        """
        Find all Edges from self.mongo_collection, based on self.filter, and the next node I connect to
        Return a cursor of MongoDB.find() query, or an empty list if Nothing found
        """
        mongo_filter = self.filter.copy()
        target_id_key = 'from_id' if self.reverse else 'to_id'
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0:
                return []
            mongo_filter[target_id_key] = {'$in': target_ids}
        if self.verbose == True:
            print(mongo_filter)
        return self.mongo_collection.find(mongo_filter, limit=self.limit, projection=projection, no_cursor_timeout=True)

    def distinct(self, key):
        if self.nextnode is None:
            result = self.mongo_collection.distinct(key, self.filter, maxTimeMS=15000)
        else:
            result = self.find().distinct(key)
        return result

    def find_from_id(self):
        """
        Find all self.mongo_collection from self.mongo_collection, based on self.filter, and the next node I connect to
        Return a set that contain strings of edgenode['from_id']
        """
        mongo_filter = self.filter.copy()
        from_id_key, to_id_key = 'from_id', 'to_id'
        if self.reverse:
            from_id_key, to_id_key = to_id_key, from_id_key
        if self.nextnode != None:
            target_ids = list(self.nextnode.findid())
            if len(target_ids) == 0: return set()
            batch_size = 100000
            result_ids = set()
            for i_batch in range(int(len(target_ids) / batch_size)+1):
                batch_ids = target_ids[i_batch*batch_size:(i_batch+1)*batch_size]
                mongo_filter[to_id_key] = {"$in": batch_ids}
                result_ids.update(d[from_id_key] for d in self.mongo_collection.find(mongo_filter, {from_id_key:1}, limit=self.limit))
        return result_ids

    def export(self, filename, ftype):
        raise NotImplementedError("Exporting query Edge is not implemented yet")