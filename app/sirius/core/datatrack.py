import os, json, time
import numpy as np
from flask import abort
import pyBigWig

from sirius.core.utilities import threadsafe_lru
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
        return abort(404, f'contig {contig} not found')
    if sampling_rate < 1:
        return abort(404, f'sampling_rate {sampling_rate} should be at least 1')
    if start_bp > contig_info['length'] or end_bp <= 0:
        # return empty result if range out of scope
        return json.dumps({
            'trackID': track_id,
            'contig': contig,
            'startBp': start_bp,
            'endBp': end_bp,
            'samplingRate': sampling_rate,
            'numSamples': 0,
            'aggregations': ['p_a', 'p_t', 'p_g', 'p_c']
        })
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
    i_end = int(np.ceil(end_bp / best_resolution))
    num_bins = int((end_bp - start_bp) / sampling_rate)
    if sampling_rate == 1:
        # load the data from tiledb
        dataarray = tilehelper.load_dense_array(best_res_data['tiledbID'])[i_start: i_end]
        t2 = time.time()
        # prepare the return data
        # the atgc array is different, so we calc the distribution here
        sampledata = np.zeros([num_bins, 4], dtype=np.float32)
        # put the atgc in place
        for i in range(4):
            sampledata[:, i] = (dataarray == i+1)
        # add 'n' as [1/4, 1/4, 1/4, 1/4]
        sampledata += (dataarray == 5)[:, np.newaxis] * 0.25
    else:
        # load the data from tiledb
        dataarray = tilehelper.load_dense_array(best_res_data['tiledbID'])[:, i_start: i_end].T
        t2 = time.time()
        # prepare the return data
        if best_resolution == sampling_rate:
            sampledata = dataarray
        else:
            # re-sample to the nearest neighbor
            sample_bps = np.arange(num_bins) * sampling_rate + start_bp
            closest_idxs = (sample_bps / best_resolution).astype(int) - i_start
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
    empty_result = json.dumps({
            'trackID': track_id,
            'contig': contig,
            'startBp': start_bp,
            'endBp': end_bp,
            'samplingRate': sampling_rate,
            'numSamples': 0,
            'aggregations': aggregations
    })
    if contig not in bw.chroms():
        return empty_result
    # check range
    contig_size = bw.chroms(contig)
    if start_bp > contig_size or end_bp < 0:
        return empty_result
    start_bp = max(start_bp, 1)
    end_bp = min(end_bp, contig_size)
    # parse data
    data_arrays = []
    num_bins = int((end_bp - start_bp + 1) / sampling_rate)
    for ag in aggregations:
        if ag == 'none':
            data_arrays.append(bw.values(contig, start_bp-1, end_bp))
        elif ag == 'min':
            data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='min', nBins=num_bins))
        elif ag == 'max':
            data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='max', nBins=num_bins))
        elif ag == 'avg' or ag == 'mean':
            data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='mean', nBins=num_bins))
        else:
            return abort(404, f'aggregation {ag} is not known')
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

@threadsafe_lru(maxsize=8192)
def get_remote_bigwig(track_id):
    encode_url = f'https://www.encodeproject.org/files/{track_id}/@@download/{track_id}.bigWig'
    bw = pyBigWig.open(encode_url)
    return bw


#############################
#       Old APIs            #
#############################

def old_api_track_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate):
    if track_id == 'sequence':
        return old_sequence_get_contig_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate)
    else:
        return old_signal_get_contig_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate)

def old_sequence_get_contig_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate, verbose=True):
    t0 = time.time()
    # check the inputs
    track_info = loaded_data_track_info_dict.get(track_id, None)
    if track_info == None:
        return abort(404, f'{track_id} not found')
    contig_info = track_info['contig_info'].get(contig, None)
    if contig_info == None:
        return f'contig {contig} not found'
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
    num_bins = int((end_bp - start_bp -1) / sampling_rate)
    i_start = int(np.floor(start_bp / best_resolution))
    i_end = int(np.ceil(end_bp / best_resolution))
    # load the data from tiledb
    dataarray = tilehelper.load_dense_array(best_res_data['tiledbID'])[i_start: i_end]
    t2 = time.time()
    # prepare the return data
    if sampling_rate == 1:
        track_data_type = 'basepairs'
        dimensions = ['value']
        # n: 0, a:0.25, t: 0.5, g: 0.75, c: 1.0
        format_data = np.array(dataarray, dtype=np.float32) % 5 # convert 'n' from 5 to 0
        format_data *= 0.25
    else:
        track_data_type = 'gbands'
        dimensions = ['gc']
        # re-sample to the nearest neighbor
        sample_bps = np.arange(num_bins) * sampling_rate + start_bp
        closest_idxs = (sample_bps / best_resolution).astype(int) - i_start
        # print(f"start_bp {start_bp}, end_bp {end_bp}; i_start {i_start}; i_end {i_end}")
        # print(f"len(dataarray) {len(dataarray)}, closest_idxs {closest_idxs}")
        sampledata = dataarray[closest_idxs]
        # combine the distribution of 'gc' to form 'gbands'
        format_data = sampledata[:, 2] + sampledata[:, 3]
    ret = [float(v) for v in format_data]
    num_bins = len(ret)
    t3 = time.time()
    # make the return data
    response = {
      "contig": contig,
      "startBp" : start_bp+1,
      "endBp" : end_bp,
      "samplingRate": sampling_rate,
      "numSamples": num_bins,
      "trackHeightPx": track_height_px,
      "values": ret,
      "dimensions": dimensions,
      "dataType": track_data_type
    }
    t4 = time.time()
    if verbose:
        print(f"Info {t1-t0:.2f}s; Loading {t2-t1:.2f}s; resample {t3-t2:.2f}s; format {t4-t3:.2f}s")
    return json.dumps(response)

def old_signal_get_contig_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate, verbose=True):
    t0 = time.time()
    bw = get_remote_bigwig(track_id)
    t1 = time.time()
    empty_result = json.dumps({
            "contig": contig,
            "startBp" : start_bp,
            "endBp" : end_bp,
            "samplingRate": sampling_rate,
            "numSamples": 0,
            "trackHeightPx": track_height_px,
            "values": [],
            "dimensions": [],
            "dataType": 'value'
    })
    if contig not in bw.chroms():
        return empty_result
    # check range
    contig_size = bw.chroms(contig)
    if start_bp > contig_size or end_bp < 0:
        return empty_result
    start_bp = max(start_bp, 1)
    end_bp = min(end_bp, contig_size)
    # get data
    data_arrays = []
    num_bins = int((end_bp - start_bp + 1) / sampling_rate)
    if sampling_rate == 1:
        track_data_type = 'value'
        dimensions = ['value']
        data_arrays.append(bw.values(contig, start_bp-1, end_bp))
    else:
        track_data_type = 'value'
        dimensions = ['min', 'max', 'avg']
        for ag in dimensions:
            if ag == 'min':
                data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='min', nBins=num_bins))
            elif ag == 'max':
                data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='max', nBins=num_bins))
            elif ag == 'avg' or ag == 'mean':
                data_arrays.append(bw.stats(contig, start_bp-1, end_bp, type='mean', nBins=num_bins))
    sampledata = np.vstack(data_arrays).astype(np.float32).T
    ret = [float(v) for v in sampledata.flatten()]
    t2 = time.time()
    # make the return data
    response = {
      "contig": contig,
      "startBp" : start_bp,
      "endBp" : end_bp,
      "samplingRate": sampling_rate,
      "numSamples": num_bins,
      "trackHeightPx": track_height_px,
      "values": ret,
      "dimensions": dimensions,
      "dataType": track_data_type
    }
    t3 = time.time()
    if verbose:
        print(f"Load {t1-t0:.2f}s; Parse {t2-t1:.2f}s; Format {t3-t2:.2f}s")
    return json.dumps(response)

