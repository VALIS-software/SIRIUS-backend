import numpy as np
import time
from sirius.core.utilities import HashableDict, threadsafe_lru
from sirius.query.query_tree import QueryTree
from sirius.helpers.loaddata import loaded_genome_contigs

def get_intervals_in_range(contig, start_bp, end_bp, query, fields=None, verbose=True):
    # default fields to empty list
    if fields is None: fields = []
    # lode cached data
    t0 = time.time()
    query_genome_data, query_start_bps = get_interval_query_results(HashableDict(query), tuple(sorted(fields)))
    contig_genome_data = query_genome_data[contig]
    contig_start_bps = query_start_bps[contig]
    total_query_count = len(contig_genome_data)
    t1 = time.time()
    if verbose:
        print(f"{total_query_count} interval_results; {t1-t0:.3f} s \n Query: {query} \n {get_interval_query_results.cache_info()}")
    if len(contig_genome_data) == 0:
        return []
    # find data in view range
    start_idx, end_idx = np.searchsorted(contig_start_bps, [start_bp, end_bp])
    genome_data_in_range = contig_genome_data[start_idx:end_idx]
    count_in_range = len(genome_data_in_range)
    t2 = time.time()
    if verbose:
        print(f"Found {count_in_range} interval_results in range; {t2-t1:.3f} seconds")
    # form return format
    result = []
    for d in genome_data_in_range:
        ret_d = {
            'id': d['_id'],
            'start': d['start'],
            'length': d['length'],
            'type': d['type'],
            'name': d['name'],
        }
        if 'info' in d:
            ret_d['info'] = d['info']
        result.append(ret_d)
    return result

@threadsafe_lru(maxsize=8192)
def get_interval_query_results(query, fields):
    print(f'-----thread running {query}')
    qt = QueryTree(query)
    # we split the results into contigs
    genome_data = {contig: [] for contig in loaded_genome_contigs}
    # put the results in to cache
    projection = ['_id', 'contig', 'start', 'length', 'type', 'name'] + list(fields)
    for gnode in qt.find(projection=projection):
        contig = gnode.pop('contig')
        genome_data[contig].append(gnode)
    # sort the results based on the start
    start_bps = dict()
    for contig, genome_data_list in genome_data.items():
        genome_data_list.sort(key=lambda d: d['start'])
        # save the 1-D array of starting loation
        start_bps[contig] = np.array([d['start'] for d in genome_data_list])
    return genome_data, start_bps
