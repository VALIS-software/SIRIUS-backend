import time
import copy
from sirius.core.utilities import HashableDict
from sirius.analysis.bed import Bed
from sirius.mongo import GenomeNodes
from sirius.mongo.utils import doc_generator

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

class GenomeQueryNode(object):
    def __init__(self, mongo_collection=None, qfilter=None, edges=None, edge_rule=0, arithmetics=None, limit=0, verbose=False):
        self.mongo_collection = mongo_collection if mongo_collection else GenomeNodes
        self.filter = qfilter if qfilter else dict()
        self.edges = edges if edges is not None else []
        # edge_rule: 0 means "and", 1 means "or", 2 means "not"
        self.edge_rule = edge_rule
        self.arithmetics = arithmetics if arithmetics is not None else []
        self.limit = int(limit)
        self.verbose = verbose

    def find(self, projection=None):
        """
        Find all nodes from self.mongo_collection, based on self.filter and the edge connected.
        Return a generator for MongoDB.find() query, or an empty list if none found
        """
        if self.verbose:
            print(self.filter, self.edges, self.arithmetics)

        if not self.arithmetics:
            mongo_filter = copy.deepcopy(self.filter)
            if len(self.edges) > 0:
                first_edgenode = self.edges[0]
                result_id_set = first_edgenode.find_from_id()
                if len(result_id_set) == 0 and self.edge_rule != 1:
                    return
                for edgenode in self.edges[1:]:
                    e_ids = edgenode.find_from_id()
                    if self.edge_rule == 0: # AND
                        result_id_set &= e_ids
                        if len(result_id_set) == 0:
                            return
                    elif self.edge_rule == 1: # OR
                        result_id_set |= e_ids
                    elif self.edge_rule == 2: # NOT
                        result_id_set -= e_ids
                        if len(result_id_set) == 0:
                            return
                # merge the id_filter with the edge ids
                id_filter = mongo_filter.pop('_id', None)
                intersect_ids = intersect_id_filter_set(id_filter, result_id_set)
                if not intersect_ids:
                    return
                elif len(intersect_ids) == 1:
                    mongo_filter['_id'] = intersect_ids[0]
                    yield self.mongo_collection.find_one(mongo_filter, projection=projection)
                else:
                    batch_size = 100000
                    for i_batch in range(int(len(intersect_ids) / batch_size)+1):
                        batch_ids = intersect_ids[i_batch*batch_size:(i_batch+1)*batch_size]
                        mongo_filter['_id'] = {"$in": batch_ids}
                        for d in self.mongo_collection.find(mongo_filter, limit=self.limit, projection=projection, no_cursor_timeout=True):
                            yield d
            else:
                for d in self.mongo_collection.find(mongo_filter, limit=self.limit, projection=projection, no_cursor_timeout=True):
                    yield d
        else:
            t0 = time.time()
            result_ids = list(self.findid())
            t1 = time.time()
            if self.verbose:
                print(f"Find id result {len(result_ids)} took {t1-t0:.2f} s")
            # Here result_ids may exceed the limit of BSON document size for MongoDB
            # Therefore we generate the documents by batches
            batch_size = 100000
            for i_batch in range(int(len(result_ids) / batch_size)+1):
                batch_ids = result_ids[i_batch*batch_size:(i_batch+1)*batch_size]
                query = {'_id' : {'$in': batch_ids}}
                for d in self.mongo_collection.find(query, limit=self.limit, projection=projection, no_cursor_timeout=True):
                    yield d

    def find_ids_without_arithmetics(self):
        mongo_filter = copy.deepcopy(self.filter)
        if len(self.edges) > 0:
            first_edgenode = self.edges[0]
            result_id_set = first_edgenode.find_from_id()
            if len(result_id_set) == 0 and self.edge_rule != 1:
                return set()
            for edgenode in self.edges[1:]:
                e_ids = edgenode.find_from_id()
                if self.edge_rule == 0: # AND
                    result_id_set &= e_ids
                    if len(result_id_set) == 0:
                        return set()
                elif self.edge_rule == 1: # OR
                    result_id_set |= e_ids
                elif self.edge_rule == 2: # NOT
                    result_id_set -= e_ids
                    if len(result_id_set) == 0:
                        return set()
            # merge the id_filter with the edge ids
            id_filter = mongo_filter.pop('_id', None)
            intersect_ids = intersect_id_filter_set(id_filter, result_id_set)
            if not intersect_ids:
                return set()
            elif len(intersect_ids) == 1:
                mongo_filter['_id'] = intersect_ids[0]
                return set([self.mongo_collection.find_one(mongo_filter, projection=['_id'])['_id']])
            else:
                batch_size = 100000
                result_ids = set()
                for i_batch in range(int(len(intersect_ids) / batch_size)+1):
                    batch_ids = intersect_ids[i_batch*batch_size:(i_batch+1)*batch_size]
                    mongo_filter['_id'] = {"$in": batch_ids}
                    for d in self.mongo_collection.find(mongo_filter, limit=self.limit, projection=['_id']):
                        result_ids.add(d['_id'])
                return result_ids
        else:
            return set(d['_id'] for d in self.mongo_collection.find(mongo_filter, projection=['_id'], limit=self.limit))

    def distinct(self, key):
        """
        Find all distinct values for a key
        """
        if not self.edges and not self.arithmetics:
            result = self.mongo_collection.distinct(key, self.filter)
        else:
            result_ids = list(self.findid())
            batch_size = 100000
            result = set()
            for i_batch in range(int(len(result_ids) / batch_size)+1):
                batch_ids = result_ids[i_batch*batch_size:(i_batch+1)*batch_size]
                query = {'_id' : {'$in': batch_ids}}
                result.update(self.mongo_collection.distinct(key, query))
        return list(result)


    def findid(self):
        """
        Find all nodes from self.mongo_collection, based on self.filter and the edge connected
        Return a set that contain strings of node['_id']
        """
        # get the results for all edges
        #t0 = time.time()
        result_ids = self.find_ids_without_arithmetics()
        #t1 = time.time()
        #print(f'find_ids_without_arithmetics returns {len(result_ids)} data in {t1-t0:.3f} s')
        if not self.arithmetics:
            return result_ids
        # Use the bedtools to do arithmics
        # do the arithmetics one by one
        for ar in self.arithmetics:
            operator = ar['operator']
            if operator == 'union':
                for target in ar['targets']:
                    result_ids |= target.findid()
            elif operator == 'window':
                if len(result_ids) == 0:
                    continue
                bed = self.load_ids_to_bed(result_ids)
                window_size = ar['windowSize']
                for target in ar['targets']:
                    target_bed = target.convert_results_to_Bed()
                    bed = bed.window(target_bed, window=window_size)
                result_ids = bed.gids()
            elif operator == 'intersect':
                if len(result_ids) == 0:
                    continue
                bed = self.load_ids_to_bed(result_ids)
                for target in ar['targets']:
                    target_bed = target.convert_results_to_Bed()
                    bed = bed.intersect(target_bed)
                result_ids = bed.gids()
            elif operator == 'diff':
                if len(result_ids) == 0:
                    continue
                for target in ar['targets']:
                    bed = self.load_ids_to_bed(result_ids)
                    target_bed = target.convert_results_to_Bed()
                    intersect_bed = bed.intersect(target_bed)
                    intersect_bed_ids = intersect_bed.gids()
                    result_ids -= intersect_bed_ids
                    if len(result_ids) == 0:
                        continue
        return result_ids

    def convert_results_to_Bed(self):
        """ Convert the results of GenomeQuery to a Bed object """
        projection=['_id', 'contig', 'start', 'end', 'info.score', 'info.strand']
        gen = self.find(projection=projection)
        return Bed(gen)

    def load_ids_to_bed(self, result_ids):
        """ Read information of a set of ids, and load them in to a Bed object """
        projection=['_id', 'contig', 'start', 'end', 'info.score', 'info.strand']
        gen = doc_generator(self.mongo_collection, result_ids, projection=projection)
        return Bed(gen)
