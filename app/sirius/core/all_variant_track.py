from sirius.core.utilities import threadsafe_lru
from sirius.mongo import GenomeNodes

@threadsafe_lru(maxsize=128)
def get_all_variants_in_range(contig, start_bp, end_bp):
    qfilt = {
        'contig': contig,
        'start': {
            '$gte': start_bp,
            '$lte': end_bp
        }
    }
    result = []
    for d in GenomeNodes.find(qfilt, projection=['_id', 'start', 'info.variant_ref', 'info.allele_frequencies']):
        d['id'] = d.pop('_id')
        result.append(d)
    return result
