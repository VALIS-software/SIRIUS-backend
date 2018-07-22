from sirius.core.utilities import threadsafe_lru
from sirius.query.QueryTree import QueryTree

def merge_query_range(contig, start_bp, end_bp, query):
    """
    Merge the contig, start and end range specs into query
    Taking into account 'contig' 'start' in filters
    """
    filters = query['filters']
    # check the 'contig' filter
    if 'contig' in filters:
        if filters['contig'] != contig:
            return None
    else:
        filters['contig'] = contig
    # merge the 'start' filter
    if 'start' in filters:
        fst = filters['start']
        if isinstance(fst, int):
            if fst > end_bp:
                return None
        elif isinstance(fst, dict):
            # intersect the start range
            s_start = fst.pop('>=', None)
            s_start = fst.pop('$gte', s_start)
            s_start_1 = fst.pop('>', None)
            s_start_1 = fst.pop('$gt', s_start_1)
            if s_start is not None:
                fst['>='] = max(start_bp, s_start)
            elif s_start_1 is not None:
                fst['>='] = max(start_bp, s_start_1 + 1)
            else:
                fst['>='] = start_bp
            # intersect the end range
            s_end = fst.pop('<=', None)
            s_end = fst.pop('$lte', s_end)
            s_end_1 = fst.pop('<', None)
            s_end_1 = fst.pop('$lt', s_end_1)
            if s_end is not None:
                fst['<='] = min(end_bp, s_end)
            elif s_end_1 is not None:
                fst['<='] = min(end_bp, s_end_1 - 1)
            else:
                fst['<='] = end_bp
            filters['start'] = fst
    else:
        filters['start'] = {'>=': start_bp, '<=': end_bp}
    if 'end' in filters:
        print("Warning, merge_query_range don't support filters['end'], please use filters['start']")
        return None
    # check if the range still make sense
    try:
        if filters['start']['>='] > filters['start']['<=']:
            return None
    except KeyError:
        pass
    query['filters'] = filters
    return query

@threadsafe_lru(maxsize=8192)
def get_variant_query_results(query):
    qt = QueryTree(query)
    result = []
    for d in qt.find(projection=['_id', 'start', 'info.variant_ref', 'info.allele_frequencies']):
        d['id'] = d.pop('_id')
        result.append(d)
    return result
