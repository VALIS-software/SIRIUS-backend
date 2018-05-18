import time, json, random
import numpy as np
from collections import defaultdict
from scipy.spatial.distance import pdist
from scipy.cluster import hierarchy
from functools import lru_cache

from sirius.query.QueryTree import QueryTree
from sirius.core.utilities import HashableDict
from sirius.helpers.constants import AGGREGATION_THRESH
from sirius.helpers.loaddata import loaded_genome_contigs

@lru_cache(maxsize=10000)
def get_annotation_query_results(query):
    qt = QueryTree(query)#, verbose=True)
    # we split the results into contigs
    contig_genome_data, contig_start_bps = dict(), dict()
    for contig in loaded_genome_contigs:
        contig_genome_data[contig] = []
        contig_start_bps[contig] = []
    # put the results in to cache
    for gnode in qt.find(projection=['_id', 'contig', 'start', 'end', 'name']):
        genome_data = (gnode['start'], gnode['end'], gnode['_id'], gnode['name'])
        contig_genome_data[gnode['contig']].append(genome_data)
    # sort the results based on the start
    for contig, genome_data_list in contig_genome_data.items():
        genome_data_list.sort()
        # save the 1-D array of starting loation
        contig_start_bps[contig] = np.array([d[0] for d in genome_data_list])
    return contig_genome_data, contig_start_bps

def get_annotation_query(annotation_id, contig, start_bp, end_bp, sampling_rate, track_height_px, query, verbose=True):
    t0 = time.time()
    query_genome_data, query_start_bps = get_annotation_query_results(HashableDict(query))
    contig_genome_data = query_genome_data[contig]
    contig_start_bps = query_start_bps[contig]
    total_query_count = len(contig_genome_data)
    t1 = time.time()
    if verbose:
        print(f"{total_query_count} gnome_query_results; {t1-t0:.3f} seconds \n Query: {query} \n {get_annotation_query_results.cache_info()}")
    # find the data in range
    start_idx, end_idx = np.searchsorted(contig_start_bps, [start_bp, end_bp])
    genome_data_in_range = contig_genome_data[start_idx:end_idx]
    count_in_range = len(genome_data_in_range)
    t2 = time.time()
    if verbose:
        print(f"Found {count_in_range} data in range; {t2-t1:.3f} seconds")
    aggregation_on = sampling_rate > AGGREGATION_THRESH
    if aggregation_on: # turn on aggregation!
        pos_in_range = contig_start_bps[start_idx:end_idx]
        ret = get_aggregation_segments(pos_in_range, sampling_rate, track_height_px)
    else:
        ret = get_genome_segments(genome_data_in_range, sampling_rate, track_height_px)
    t3 = time.time()
    if verbose:
        print(f"{len(ret)} after data aggregation; {t3-t2:.3f} seconds")
    return json.dumps({
        "contig": contig,
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationId": annotation_id,
        "values": ret,
        "countInRange": count_in_range,
        "aggregation": aggregation_on
    })

def get_genome_segments(genome_data_list, sampling_rate, track_height_px):
    ret = []
    ANNOTATION_HEIGHT_PX = int(track_height_px / 3) - 1
    last_end, last_y = -float('inf'), 0
    padding = 20 * sampling_rate
    for gnome_data in genome_data_list:
        (start_bp, end_bp, fid, name) = gnome_data
        will_append = False
        if start_bp > last_end + padding:
            will_append = True
            last_end = end_bp
            yOffset = 0
            last_y = 0
        elif last_y <= track_height_px - 2 * ANNOTATION_HEIGHT_PX + 1:
            will_append = True
            last_end = max(last_end, end_bp)
            yOffset = last_y + ANNOTATION_HEIGHT_PX + 1
            last_y = yOffset
        if will_append == True:
            color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
            r_data = {
                'id': fid,
                'startBp': start_bp,
                'endBp': end_bp,
                'labels': [[name, True, 0, 0, 0]],
                'yOffsetPx': yOffset,
                'heightPx': ANNOTATION_HEIGHT_PX,
                "segments": [[0, end_bp-start_bp+1, None, color, 20]],
                'title': name,
                'aggregation': False
            }
            ret.append(r_data)
    return ret

def get_aggregation_segments(coords, sampling_rate, track_height_px):
    """ Optimized clustering algorithm that handles arbitrary size of data, with a certain resolution """
    ndata = len(coords)
    if ndata == 0: return []
    elif ndata == 1:
        cluster_ends = [ndata]
    else:
        # clustering when more than 2 data points are provided
        # normalize the resolution of data
        pos = (coords/sampling_rate).astype(int)
        pos -= pos.min()
        norm_factor = max(ndata / 1000, 1)
        bc = np.bincount(pos)
        bc = ( bc / norm_factor).astype(int)
        dist_mat = build_bin_count_dist_mat(bc)
        linkage = hierarchy.average(dist_mat)
        cluster_results = hierarchy.fcluster(linkage, t=100, criterion='distance')
        cluster_ends = np.nonzero(np.diff(cluster_results))[0]
        # scale back
        cluster_ends = (cluster_ends * norm_factor).astype(int)
        # add the last end
        cluster_ends = list(cluster_ends) + [ndata]
    # build the return blocks
    ret = []
    c_begin = 0
    for c_end in cluster_ends:
        cluster_coords = coords[c_begin:c_end]
        c_size = len(cluster_coords)
        if c_size == 0: continue
        startBp = int(cluster_coords[0])
        endBp = int(cluster_coords[-1])
        label = str(c_size)
        color_level = max(min(c_size / 100, 1.0), 0.2) # between (0.2~1.0)
        color = [0.15, 0.55, 1.0, color_level]
        r_data = {'id': 'cluster',
                  'count': c_size,
                  'startBp': startBp,
                  'endBp': endBp,
                  'labels': [[label, True, 4, 0, 0]],
                  'yOffsetPx': 0,
                  'heightPx': track_height_px,
                  'segments': [[0, endBp-startBp+1, None, color, 20]],
                  'aggregation': True
                 }
        ret.append(r_data)
        c_begin = c_end+1
    return ret

def build_bin_count_dist_mat(bincount):
    data = np.repeat(np.arange(bincount.size), bincount).reshape(-1,1)
    return pdist(data, 'chebyshev')
