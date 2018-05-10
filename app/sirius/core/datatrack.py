import os, json, time
import numpy as np
from functools import lru_cache
from flask import abort
import pyBigWig

from sirius.helpers.loaddata import loaded_data_track_info_dict
from sirius.helpers.tiledb import tilehelper

def get_sequence_data(track_id, contig, start_bp, end_bp, sampling_rate, verbose=True):
    t0 = time.time()
    # check the inputs
    track_info = loaded_data_track_info_dict.get(track_id, None)
    if track_info == None:
        return abort(404, f'{track_id} not found')
    contig_info = track_info['contig_info'].get(contig, None)
    if contig_info == None:
        return 'contig not found'
    if sampling_rate < 1:
        return abort(404, f'sampling_rate {sampling_rate} should be at least 1')
    # find the best resolution data
    best_resolution = 0
    best_res_data = None
    for data in contig_info['stored_data']:
        if data['resolution'] <= sampling_rate and data['resolution'] > best_resolution:
            best_resolution = data['resolution']
            best_res_data = data
    t1 = time.time()
    # compute the best start-end range
    start_bp = max(start_bp-1, 0) # convert to starting index at 0
    end_bp = min(end_bp, contig_info['length'])
    i_start = int(np.floor(start_bp / best_resolution))
    i_end = int(np.ceil(end_bp / best_resolution)) + 1
    # load the data from tiledb
    dataarray = tilehelper.load_dense_array(best_res_data['tiledbID'])[i_start: i_end]['value']
    t2 = time.time()
    # prepare the return data
    num_bins = int((end_bp - start_bp) / sampling_rate) + 1
    if sampling_rate == 1:
        # the atgc array is different, so we calc the distribution here
        sampledata = np.zeros([num_bins, 4], dtype=np.float32)
        # put the atgc in place
        for i in range(4):
            sampledata[:, i] = (dataarray == i+1)
        # add 'n' as [1/4, 1/4, 1/4, 1/4]
        sampledata += (dataarray == 5)[:, np.newaxis] * 0.25
    elif best_resolution == sampling_rate:
        sampledata = dataarray
    else:
        # re-sample to the nearest neighbor
        sample_bps = np.arange(num_bins) * sampling_rate + start_bp
        closest_idxs = np.round(sample_bps / best_resolution).astype(int) - i_start
        sampledata = dataarray[closest_idxs]
    t3 = time.time()
    # make the return data
    header = {
        'trackID': track_id,
        'contig': contig,
        'startBp': start_bp+1,
        'endBp': end_bp,
        'samplingRate': sampling_rate,
        'numSamples': num_bins,
        'aggregations': ['p_a', 'p_t', 'p_g', 'p_c']
    }
    response = json.dumps(header).encode('utf-8')
    response += b'\x00'
    response += sampledata.tobytes()
    t4 = time.time()
    if verbose:
        print(f"Info {t1-t0:.2f}s; Loading {t2-t1:.2f}s; resample {t3-t2:.2f}s; format {t4-t3:.2f}s")
    return response

def get_signal_data(track_id, contig, start_bp, end_bp, sampling_rate, aggregations, verbose=True):
    t0 = time.time()
    bw = get_remote_bigwig(track_id)
    t1 = time.time()
    if contig not in bw.chroms():
        return 'contig not found'
    data_arrays = []
    num_bins = int((end_bp - start_bp + 1) / sampling_rate)
    for ag in aggregations:
        if ag == 'raw':
            data_arrays.append(bw.values(contig, start_bp, end_bp+1))
        elif ag == 'min':
            data_arrays.append(bw.stats(contig, start_bp, end_bp+1, type='min', nBins=num_bins))
        elif ag == 'max':
            data_arrays.append(bw.stats(contig, start_bp, end_bp+1, type='min', nBins=num_bins))
        elif ag == 'avg':
            data_arrays.append(bw.stats(contig, start_bp, end_bp+1, type='mean', nBins=num_bins))
        else:
            return f'aggregation {ag} is not known'
    sampledata = np.vstack(data_arrays).astype(np.float32).T
    t2 = time.time()
    # make the return data
    header = {
        'trackID': track_id,
        'contig': contig,
        'startBp': start_bp,
        'endBp': end_bp,
        'samplingRate': sampling_rate,
        'numSamples': num_bins,
        'aggregations': aggregations
    }
    response = json.dumps(header).encode('utf-8')
    response += b'\x00'
    response += sampledata.tobytes()
    t3 = time.time()
    if verbose:
        print(f"Load {t1-t0:.2f}s; Parse {t2-t1:.2f}s; Format {t3-t2:.2f}s")
    return response


@lru_cache(maxsize=10000)
def get_remote_bigwig(track_id):
    encode_url = f'https://www.encodeproject.org/files/{track_id}/@@download/{track_id}.bigWig'
    bw = pyBigWig.open(encode_url)
    return bw
