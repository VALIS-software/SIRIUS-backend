
import collections
import numpy as np
from scipy.spatial.distance import pdist
from scipy.cluster import hierarchy

def cluster_r_data(r_data_in_range, sampling_rate, track_height_px):
    """ Optimized clustering algorithm that handles arbitrary size of data, with a certain resolution """
    ndata = len(r_data_in_range)
    if ndata == 0: return []
    elif ndata == 1:
        # convert the single annotation to a length 1 block
        cluster_ends = [ndata]
    else:
        # clustering when more than 2 data points are provided
        pos = np.array([0.5*(r['startBp']+r['endBp']) for r in r_data_in_range]).astype(int)
        # normalize the resolution of data
        pos = (pos/sampling_rate).astype(int)
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
        r_cluster = r_data_in_range[c_begin: c_end+1]
        c_size = len(r_cluster)
        label = str(c_size)
        startBp = r_cluster[0]['startBp']
        endBp = r_cluster[-1]['endBp']
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




# below are some other functions that was used to try fast aggregations

from scipy.ndimage.filters import gaussian_filter1d
from scipy.signal import argrelmin

def aggregate_r_data(r_data_in_range, sampling_rate, track_height_px):
    """ New way of grouping data, faster than the clustering """
    ndata = len(r_data_in_range)
    if ndata == 0: return []
    coords = np.array([[r['startBp'], r['endBp']] for r in r_data_in_range], dtype=int).reshape(-1,2)
    # normalize the positions of each data
    pos = (np.mean(coords, axis=-1) / sampling_rate).astype(int)
    pos -= pos[0]
    # count the density
    bc = np.bincount(pos)
    print(len(bc))
    # smoothed bincount
    sigma = int(ndata**0.3 / 2)
    smoothed = gaussian_filter1d(bc, sigma=sigma)
    # find the local minima to be the split location
    split_loc = find_local_min(smoothed)
    # since the data is sorted, the clusters can be defined sequencially
    cluster_ends = np.cumsum(bc)[split_loc]
    # add the last cluster
    cluster_ends = list(cluster_ends) + [ndata]
    print(cluster_ends)
    # build the return data
    ret = []
    c_begin = 0
    for c_end in cluster_ends:
        r_cluster = r_data_in_range[c_begin: c_end+1]
        c_size = c_end - c_begin
        label = str(c_size)
        startBp = r_cluster[0]['startBp']
        endBp = r_cluster[-1]['endBp']
        color_level = max(min(c_size / 100, 1.0), 0.2) # between (0.2~1.0)
        color = [0.15, 0.55, 1.0, color_level]
        r_data = {'id': 'cluster',
                  'startBp': startBp,
                  'endBp': endBp,
                  'labels': [[label, True, 4, 0, 0]],
                  'yOffsetPx': 0,
                  'heightPx': track_height_px,
                  "segments": [[0, endBp-startBp+1, None, color, 20]],
                  'aggregation': True
                  # 'entity': [r['entity'] for r in r_cluster] # QYD: we don't want all these details yet.
                 }
        ret.append(r_data)
        c_begin = c_end+1
    return ret

def find_local_min(data):
    if len(data) == 0: return []
    going_down = False
    local_min = []
    for i in range(1, len(data)):
        if data[i] < data[i-1]:
            going_down = True
        elif data[i] > data[i-1]:
            if going_down == True:
                local_min.append(i-1)
            going_down = False
    return local_min


def cluster_r_data_old(r_data_in_range, sampling_rate, track_height_px):
    if len(r_data_in_range) == 0: return []
    coords = np.array([[r['startBp'], r['endBp']] for r in r_data_in_range], dtype=int).reshape(-1,2)
    dist_mat = pdist(coords, 'chebyshev')
    linkage = hierarchy.average(dist_mat)
    cluster_results = hierarchy.fcluster(linkage, t=sampling_rate*100, criterion='distance')
    clusters = collections.defaultdict(list)
    for i,c in enumerate(cluster_results):
        clusters[c].append(i)
    ret = []
    for cluster in sorted(clusters.values()):
        r_cluster = [r_data_in_range[i] for i in cluster]
        c_size = len(cluster)
        label = str(c_size)
        startBp = r_cluster[0]['startBp']
        endBp = r_cluster[-1]['endBp']
        color_level = max(min(c_size / 100, 1.0), 0.2) # between (0.2~1.0)
        color = [0.15, 0.55, 1.0, color_level]
        r_data = {'id': 'cluster',
                  'startBp': startBp,
                  'endBp': endBp,
                  'labels': [[label, True, 4, 0, 0]],
                  'yOffsetPx': 0,
                  'heightPx': track_height_px,
                  "segments": [[0, endBp-startBp+1, None, color, 20]],
                  'aggregation': True
                  # 'entity': [r['entity'] for r in r_cluster] # QYD: we don't want all these details yet.
                 }
        ret.append(r_data)
    return ret
