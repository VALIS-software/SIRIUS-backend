import threading
from pymongo.errors import CursorNotFound

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
        self.projection = projection
        self.loaded_data = []
        self.load_finished = False
        self.data_generator = self.qt.find(projection=self.projection)
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
            try:
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
            except CursorNotFound:
                print(f"*** Cursor not found for query {self.qt}, restart loading")
                self.loaded_data = []
                self.load_finished = False
                self.data_generator = self.qt.find(projection=self.projection)
                self.load_data_until(index)


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
    The GWAS query are different from regular query in these ways:
    1.  Each of the returning SNPs will have a property info.p-value, which is aggregated as "the lowest p-value among all resulting Edges (associations)".
        Note that for the same SNP this property could be different based on which trait it is searched to be associated to.
    2.  The returning SNPs will be sorted by the info.p-value from lowest to highest.
        The SNPs that do not have any p-value from associations, they will have info.p-value = None
    3.  The limit is ignored for the resulting SNPs.
    """
    if not query: return []
    # build a QueryTree
    qt = QueryTree(query)
    # here we manually execute the to_edges in the query tree
    genome_query_node = qt.head
    # assert len(genome_query_node.arithmetics) == 0, "No arithmetics supported for GWAS SNP query"
    query_edges = genome_query_node.edges
    assert len(query_edges) == 1, "GWAS SNP query should have exactly one edge"
    assert genome_query_node.edge_rule == 0, "The edge rule of GWAP SNP query should be 0 (and)"
    query_edge = query_edges[0]
    # build a edges dictionary with their p-value recorded, keep the lowest one
    gid_score = dict()
    for edge in query_edge.find(projection=['from_id', 'info.p-value']):
        if 'p-value' in edge['info']:
            gid = edge['from_id']
            pvalue = edge['info']['p-value']
            if gid in gid_score:
                gid_score[gid] = min(gid_score[gid], pvalue)
            else:
                gid_score[gid] = pvalue
    # run the SNP genome query and store the lowest p-values
    gwas_SNP_projection = None # this may be updated later to reduce data transfer
    result_with_pvalue = []
    result_no_pvalue = []
    for gnode in genome_query_node.find(projection=gwas_SNP_projection):
        # append the lowest p-value to the SNP node
        gid = gnode.pop('_id')
        # replace _id by id
        gnode['id'] = gid
        if gid in gid_score:
            gnode['info']['p-value'] = gid_score[gid]
            result_with_pvalue.append(gnode)
        else:
            result_no_pvalue.append(gnode)
    # sort the resulting gnodes by their p-value
    result_with_pvalue.sort(key=lambda d: d['info']['p-value'])
    # append the SNP gnodes without a p-value at the end
    result = result_with_pvalue + result_no_pvalue
    return result
