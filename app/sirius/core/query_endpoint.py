import threading

from sirius.query.query_tree import QueryTree
from sirius.query.query_edge import QueryEdge
from sirius.query.genome_query_node import GenomeQueryNode
from sirius.core.utilities import threadsafe_lru

class QueryResultsCache:
    """
    Class that implemented dynamic caching for query results
    """
    def __init__(self, query, projection=None):
        self.qt = QueryTree(query)
        self.data_generator = self.qt.find(projection=projection)
        self.loaded_data = []
        self.load_finished = False
        self.lock = threading.Lock()

    def __getitem__(self, key):
        if self.load_finished is True:
            return self.loaded_data[key]
        elif isinstance(key, slice):
            if key.step is not None and key.step <= 0:
                raise ValueError("slice step cannot be zero or negative")
            self.load_data_until(key.stop)
            return self.loaded_data[key]
        elif isinstance(key, int):
            self.load_data_until(key + 1)
            return self.loaded_data[key]
        else:
            raise TypeError(f"list indices must be integers or slices, not {type(key)}")

    def load_data_until(self, index=None):
        # if we already have that many results
        if index is not None and index < len(self.loaded_data): return
        # iteratively load data into cache
        with self.lock:
            for data in self.data_generator:
                # convert "_id" to "id" for frontend
                data['id'] = data.pop('_id')
                self.loaded_data.append(data)
                # here we load one more data than requested, so we know if all data loaded
                if index is not None and len(self.loaded_data) > index:
                    break
            else:
                # all data loaded
                self.load_finished = True


@threadsafe_lru(maxsize=1024)
def get_query_full_results(query):
    """ Cached function for getting full query results """
    if not query: return []
    return QueryResultsCache(query)

@threadsafe_lru(maxsize=1024)
def get_query_basic_results(query):
    """ Cached function for getting basic query results """
    if not query: return []
    basic_projection = ['_id', 'source', 'type', 'name', 'contig', 'start', 'end', 'info.description']
    return QueryResultsCache(query, projection=basic_projection)

@threadsafe_lru(maxsize=1024)
def get_query_gwas_results(query):
    """ Cached function for getting gwas query results 
    The GWAS query are different from regular query in two ways:
    1.  Each of the returning SNPs will have a property info.p-value, which is aggregated as "the lowest p-value among all resulting Edges (associations)". 
        Note that for the same SNP this property could be different based on which trait it is searched to be associated to.
    2.  The returning SNPs will be sorted by the info.p-value from lowest to highest. 
        The SNPs that do not have any p-value from associations, they will have info.p-value = None
    """
    if not query: return []
    
    
    return QueryResultsCache(query)
