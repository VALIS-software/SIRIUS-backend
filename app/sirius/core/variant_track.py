import numpy as np
import time
from sirius.core.utilities import HashableDict, threadsafe_lru
from sirius.query.QueryTree import QueryTree
from sirius.helpers.loaddata import loaded_genome_contigs

def get_variants_in_range(contig, start_bp, end_bp, query, verbose=True):
    # lode cached data
    t0 = time.time()
    query_genome_data, query_start_bps = get_interval_query_results(HashableDict(query))
    contig_genome_data = query_genome_data[contig]
    contig_start_bps = query_start_bps[contig]
    total_query_count = len(contig_genome_data)
    t1 = time.time()
    if len(contig_genome_data) == 0:
        return []
    # find data in view range
    start_idx, end_idx = np.searchsorted(contig_start_bps, [start_bp, end_bp])
    genome_data_in_range = contig_genome_data[start_idx:end_idx]
    count_in_range = len(genome_data_in_range)
    t2 = time.time()
    if verbose:
        print(f"**** get_variants_in_range debug info ***")
        print(f"-- {total_query_count} variant_results; {t1-t0:.3f} s")
        print(f"-- Query: {query}")
        print(f"-- Cache Info {get_variant_query_results.cache_info()}")
        print(f"-- Found {count_in_range} variant_results in range; {t2-t1:.3f} seconds")
    # form return format
    result = []
    for d in genome_data_in_range:
        result.append({
            'id': d['_id'],
            'start': d['start'],
            'length': d['length'],
            'type': d['type'],
            'name': d['name']
        })
    return result

@threadsafe_lru(maxsize=8192)
def get_variant_query_results(query):
    qt = QueryTree(query)
    # we split the results into contigs
    genome_data = {contig: [] for contig in loaded_genome_contigs}
    # put the results in to cache
    for gnode in qt.find(projection=['_id', 'contig', 'start', 'info.variant_ref', 'info.allele_frequencies']):
        contig = gnode.pop('contig')
        genome_data[contig].append(gnode)
    # sort the results based on the start
    start_bps = dict()
    for contig, genome_data_list in genome_data.items():
        genome_data_list.sort(key=lambda d: d['start'])
        # save the 1-D array of starting loation
        start_bps[contig] = np.array([d['start'] for d in genome_data_list])
    return genome_data, start_bps
