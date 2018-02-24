#!/usr/bin/env python

from flask import abort, request, send_from_directory
import random
import json
import math
import os
import shutil
from sirius.main import app

from sirius.realdata.loaddata import loaded_annotations
from sirius.realdata.constants import chromo_idxs, chromo_names
from sirius.core.QueryTree import QueryTree
import numpy as np
from scipy.spatial.distance import pdist
from scipy.cluster import hierarchy
import collections


#**************************
#*     static urls        *
#**************************
# These could be served by Nginx
# Provided here for debugging
@app.route('/')
@app.route('/index')
def index():
    return send_from_directory("valis-dist", "index.html")

@app.route('/<path:path>')
def send_file(path):
    return app.send_static_file(path)


#**************************
#*     /annotations       *
#**************************

from sirius.mockData.mock_util import getMockAnnotations, get_mock_annotation_data

@app.route("/annotations")
def annotations():
    MOCK_ANNOTATIONS = getMockAnnotations()
    return json.dumps(list(MOCK_ANNOTATIONS.keys()) + list(loaded_annotations.keys()))

@app.route("/annotations/<string:annotation_id>")
def annotation(annotation_id):
    MOCK_ANNOTATIONS = getMockAnnotations()
    """Return the annotation metadata"""
    if  annotation_id in loaded_annotations:
        return loaded_annotations[annotation_id].json_data()
    elif annotation_id in MOCK_ANNOTATIONS:
        return json.dumps(MOCK_ANNOTATIONS[annotation_id])
    else:
        abort(404, "Annotation not found")

test_query = {
  "type": "GenomeNode",
  "filters": {
    "assembly": "GRCh38",
    "type": "SNP"
  },
  "edgeRule": "or",
  "toEdges": [
    {
      "type": "EdgeNode",
      "filters": {
        "type": "association",
        "info.pvalue": {"<": 0.5}
      },
      "toNode": {
        "type": "InfoNode",
        "filters": {
          "type": "trait",
          "name": "cancer"
        }
      }
    },
    {
      "type": "EdgeNode",
      "filters": {
        "type": "association"
      },
      "toNode": {
        "type": "InfoNode",
        "filters": {
          "type": "trait",
          "name": {"==": "breast cancer"}
        }
      }
    }
  ]
}

ANNOTATION_HEIGHT_PX = 22

@app.route("/annotations/<string:annotation_ids>/<int:start_bp>/<int:end_bp>", methods=['GET','POST'])
def get_annotation_data(annotation_ids, start_bp, end_bp):
    annotation_id = annotation_ids.split(",", 1)[0] # we don't know when there are multiple annotations yet
    start_bp = int(start_bp)
    end_bp = int(end_bp)
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    track_height_px = int(request.args.get('track_height_px', default=0))
    if annotation_id == 'GWASCatalog':
        #query = request.get_json()
        query = test_query
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    elif annotation_id in loaded_annotations:
        result = get_real_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px)
    else:
        result = get_mock_annotation_data(annotation_ids, start_bp, end_bp, sampling_rate, track_height_px)
    #print(result)
    return result


# temporary solution for caching queries
query_result_cache = dict()
def get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query):
    query_cach_key = str(query)
    try:
        query_result = query_result_cache[query_cach_key]
    except KeyError:
        qt = QueryTree(query)
        query_result_cache[query_cach_key] = query_result = list(qt.find().sort([("location",1), ("start",1)]))
    print("Query returns %s results" % len(query_result))
    annotation = loaded_annotations['GRCh38']
    aggregation_thresh = 100000
    r_data_in_range = []
    for gnode in query_result:
        abs_pos = annotation.location_to_bp(gnode['location'], gnode['start'])
        fid = gnode['_id'] = str(gnode['_id'])
        try:
            fname = gnode['info']['Name'][:8] # limit name length
        except KeyError:
            fname = 'Unknown'
        if start_bp <= abs_pos <= end_bp:
            color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
            r_data = {'id': fid,
                      'startBp': abs_pos,
                      'endBp': abs_pos,
                      'labels': [[fname, True, 0, 0, 0]],
                      'yOffsetPx': 0,
                      'heightPx': ANNOTATION_HEIGHT_PX,
                      "segments": [[0, gnode['length'], None, color, 20]],
                      'entity': gnode
                     }
            r_data_in_range.append(r_data)
    if sampling_rate > aggregation_thresh: # turn on aggregation!
        print("Clustering results")
        ret = cluster_r_data(r_data_in_range, sampling_rate, track_height_px)
    else:
        ret = fit_results_in_track(r_data_in_range, sampling_rate, track_height_px, ANNOTATION_HEIGHT_PX)
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationIds": annotation_id,
        "values": ret
    })

def cluster_r_data(r_data_in_range, sampling_rate, track_height_px):
    if len(r_data_in_range) == 0: return []
    coords = np.array([[r['startBp'], r['endBp']] for r in r_data_in_range], dtype=int).reshape(-1,2)
    dist_max = pdist(coords, 'chebyshev')
    linkage = hierarchy.average(dist_max)
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
                  'entity': [r['entity'] for r in r_cluster]
                 }
        ret.append(r_data)
    return ret


def fit_results_in_track(r_data_in_range, sampling_rate, track_height_px, ANNOTATION_HEIGHT_PX):
    if len(r_data_in_range) == 0: return []
    padding = 20 * sampling_rate
    last = r_data_in_range[0]
    ret = [last]
    for r_data in r_data_in_range[1:]:
        if r_data["startBp"] > last['endBp'] + padding:
            ret.append(r_data)
            last = r_data
        elif last["yOffsetPx"] < track_height_px - ANNOTATION_HEIGHT_PX:
            r_data["yOffsetPx"] = last["yOffsetPx"] + ANNOTATION_HEIGHT_PX + 1
            ret.append(r_data)
            last = r_data
    return ret


def get_real_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px):
    annotation = loaded_annotations[annotation_id]
    start_bp = max(start_bp, annotation.start_bp)
    end_bp = min(end_bp, annotation.end_bp)
    annotation_results = []
    gene_count = 0
    cursor = annotation.db_find(start_bp, end_bp, types=['gene','exon'], min_length=sampling_rate*20)
    ret = []
    padding = 20 * sampling_rate
    last = None
    for feature_data in cursor:
        color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
        feature_data['_id'] = str(feature_data['_id']) # convert to string for json
        try:
            fid = feature_data['info']['attributes']["ID"]
        except:
            fid = feature_data['_id']
        try:
            fname = feature_data['info']['attributes']['Name']
        except:
            fname = 'Unknown'
        r_data = {'id': fid,
                  'labels': [[fname, True, 0, 0, 0]],
                  'yOffsetPx': 0,
                  'heightPx': ANNOTATION_HEIGHT_PX,
                  "segments": [[0, feature_data['length'], None, color, 20]],
                  'entity': feature_data # all infomation here for frontend to display
                 }
        i_ch = chromo_idxs[feature_data['location']]
        ch_start = annotation.chromo_end_bps[i_ch-1] if i_ch > 0 else 0
        r_data['startBp'] = ch_start + feature_data['start']
        r_data['endBp'] = ch_start + feature_data['end']
        if last == None or r_data["startBp"] > last['endBp'] + padding:
            ret.append(r_data)
            last = r_data
        elif last["yOffsetPx"] < track_height_px - ANNOTATION_HEIGHT_PX:
            r_data["yOffsetPx"] = last["yOffsetPx"] + ANNOTATION_HEIGHT_PX + 1
            ret.append(r_data)
            last = r_data
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationIds": annotation_id,
        "values": ret
    })



#**************************
#*       /tracks          *
#**************************

from sirius.mockData.mock_util import getMockData, get_mock_track_data

@app.route("/tracks")
def tracks():
    """Return a list of all track_ids"""
    MOCK_DATA = getMockData()
    return json.dumps(list(MOCK_DATA.keys()))

@app.route("/tracks/<string:track_id>")
def track(track_id):
    MOCK_DATA = getMockData()
    """Return the track metadata"""
    if track_id in MOCK_DATA:
        return json.dumps(MOCK_DATA[track_id])
    else:
        abort(404, "Track not found")

@app.route("/tracks/<string:track_id>/<int:start_bp>/<int:end_bp>")
def get_track_data(track_id, start_bp, end_bp):
    """Return the data for the given track and base pair range"""
    start_bp = int(start_bp)
    end_bp = int(end_bp)
    track_data_type = 'signal'
    track_height_px = int(request.args.get('track_height_px', default=0))
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    aggregations = request.args.get('aggregations', default='none').split(',')
    return get_mock_track_data(track_id, start_bp, end_bp, track_data_type, track_height_px, sampling_rate, aggregations)



# This part is still mock
#**************************
#*        /graphs         *
#**************************

@app.route("/graphs")
def graphs():
    return json.dumps(["ld_score"])

@app.route("/graphs/<string:graph_id>/<string:annotation_id1>/<string:annotation_id2>/<int:start_bp>/<int:end_bp>")
def graph(graph_id, annotation_id1, annotation_id2, start_bp, end_bp):
    start_bp = int(start_bp)
    end_bp = int(end_bp)

    sampling_rate = 1
    if request.args.get('sampling_rate'):
        sampling_rate = int(float(request.args.get('sampling_rate')))

    base_pair_offset = 0
    if request.args.get('base_pair_offset'):
        base_pair_offset = int(float(request.args.get('base_pair_offset')))

    if graph_id != "ld_score":
        abort(500, "Unknown graph : %s", graph_id)

    if annotation_id1 != "cross-track-test-1" or annotation_id2 != "cross-track-test-2":
        abort(500, "no graph available")

    # send edge scores
    set1 = []
    set2 = []
    if sampling_rate < 1000000:
        count = 0
        for i in range(0, 100000000, 500000):
            if i >= start_bp and i <= end_bp:
                annotation_name = "X%d" % count
                random.seed(annotation_name)
                set1.append(random.randint(0,1000000000))
            count += 1
        count = 0
        for i in range(0, 100000000, 500000):
            if i >= start_bp + base_pair_offset and i <= end_bp + base_pair_offset:
                annotation_name = "Y%d" % count
                random.seed(annotation_name)
                set2.append(random.randint(0,1000000000))
            count += 1

    edges = []
    for e1 in set1:
        for e2 in set2:
            random.seed("%d|%d" % (e1,e2))
            edges.append([e1, e2, random.random()])
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "graphId": graph_id,
        "annotationIds": [annotation_id1, annotation_id2],
        "values": edges
    })



# this part hasn't been implemented in the frontend yet
#**************************
#*       /query           *
#**************************

query_cache = dict()
@app.route('/query/<string:query_id>', methods=['POST', 'PUT', 'DELETE'])
def change_query(query_id):
    """ Create, replace or delete a query """
    if request.method == 'POST':
        query = request.get_json()
        result = create_query(query_id, query)
        return (201, "Query %s created with %d result" % (query_id, len(result)))
    elif request.method == 'PUT':
        query = request.get_json()
        result = replace_query(query_id, query)
        return (200, "Query %s replaced with %d result" % (query_id, len(result)))
    elif request.method == 'DELETE':
        delete_query(query_id)
        return (200, "Query %s deleted" % query_id)


def create_query(query_id, query):
    """ Delete a query to free memory """
    try:
        result = query_cache[query_id]
    except KeyError:
        qt = QueryTree(query)
        query_cache[query_id] = result = list(qt.find().sort([("location",1), ("start",1)]))
    return result

def replace_query(query_id, query):
    qt = QueryTree(query)
    query_cache[query_id] = result = list(qt.find().sort([("location",1), ("start",1)]))
    return result

def delete_query(query_id):
    """ Delete a query to free memory """
    try:
        query_result = query_cache.pop(query_id)
    except:
        pass

@app.route('/query/<string:query_id>/<int:start_bp>/<int:end_bp>')
def get_query(query_id, start_bp, end_bp):
    try:
        query_result = query_result_cache[query_id]
    except KeyError:
        print("Warning! Query %s was not created! Please call /query/query_id/create first")
        return abort(404, "Query not found")
    print("Query returns %s results" % len(query_result))
    start_bp, end_bp = int(start_bp), int(end_bp)
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    track_height_px = int(request.args.get('track_height_px', default=0))
    # assume we are working with GRCh38 for now. Frontend may need redesign later to show more assembly
    annotation = loaded_annotations['GRCh38']
    aggregation_thresh = 100000
    r_data_in_range = []
    for gnode in query_result:
        abs_pos = annotation.location_to_bp(gnode['location'], gnode['start'])
        fid = gnode['_id'] = str(gnode['_id'])
        try:
            fname = gnode['info']['Name'][:8] # limit name length
        except KeyError:
            fname = 'Unknown'
        if start_bp <= abs_pos <= end_bp:
            color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
            r_data = {'id': fid,
                      'startBp': abs_pos,
                      'endBp': abs_pos,
                      'labels': [[fname, True, 0, 0, 0]],
                      'yOffsetPx': 0,
                      'heightPx': ANNOTATION_HEIGHT_PX,
                      "segments": [[0, gnode['length'], None, color, 20]],
                      'entity': gnode
                     }
            r_data_in_range.append(r_data)
    if sampling_rate > aggregation_thresh: # turn on aggregation!
        print("Clustering results")
        ret = cluster_r_data(r_data_in_range, sampling_rate, track_height_px)
    else:
        ret = fit_results_in_track(r_data_in_range, sampling_rate, track_height_px, ANNOTATION_HEIGHT_PX)
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationIds": annotation_id,
        "values": ret
    })
