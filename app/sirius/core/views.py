#!/usr/bin/env python

from flask import abort, request, send_from_directory
import random
import json
from functools import lru_cache
import time
from sirius.main import app
from sirius.realdata.loaddata import loaded_annotations, loaded_track_info
from sirius.realdata.constants import chromo_idxs, chromo_names
from sirius.core.QueryTree import QueryTree
from sirius.core.aggregations import cluster_r_data

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
        return json.dumps({'annotationId': annotation_id, 'startBp': 1, 'endBp': 3088269832})

test_query = {
  "type": "GenomeNode",
  "filters": {
    "assembly": "GRCh38",
    "type": "SNP"
  },
  "edgeRule": "and",
  "toEdges": [
    {
      "type": "EdgeNode",
      "filters": {
        "type": "association",
        "info.p-value": {"<": 0.5}
      },
      "toNode": {
        "type": "InfoNode",
        "filters": {
          "type": "trait",
          "$text": "cancer"
        }
      }
    },
    {
      "type": "EdgeNode",
      "filters": {
        "type": "association",
      },
      "toNode": {
        "type": "GenomeNode",
        "filters": {
           "type": "gene",
        }
      }
    }
  ]
}

@app.route("/annotations/<string:annotation_id>/<int:start_bp>/<int:end_bp>", methods=['GET','POST'])
def get_annotation_data(annotation_id, start_bp, end_bp):
    start_bp = int(start_bp)
    end_bp = int(end_bp)
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    track_height_px = int(request.args.get('track_height_px', default=0))

    query = request.get_json()
    if query:
        # let show some real data!!
        print(query)
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    elif annotation_id == 'GWASCatalog':
        query = test_query
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    elif annotation_id == "GRCh38":
        query = {'type': 'GenomeNode', 'filters':{'assembly': 'GRCh38', 'type':'gene'}}
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    elif annotation_id in loaded_annotations:
        result = get_real_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px)
    else:
        result = get_mock_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px)
    #print(result)
    return result

class HashableDict(dict):
    def __hash__(self):
        return hash(json.dumps(self, sort_keys=True))

@lru_cache(maxsize=10000)
def get_query_results(query):
    qt = QueryTree(query)#, verbose=True)
    return sorted(list(qt.find(projection=['_id','chromid','start','length','name'])), key=lambda d: (d['chromid'], d['start']))

def get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query):
    t0 = time.time()
    query_result = get_query_results(HashableDict(query))
    t1 = time.time()
    print("Query returns %s results in %.3f seconds" % (len(query_result), t1-t0), get_query_results.cache_info())
    annotation = loaded_annotations['GRCh38']
    aggregation_thresh = 5000
    chr_r_data_in_range = [[] for _ in range(len(chromo_idxs)+1)] # r_data for each chromosome, initial one is empty
    ANNOTATION_HEIGHT_PX = int(track_height_px / 3) - 1
    count_in_range = 0
    for gnode in query_result:
        chr_id = gnode['chromid']
        if chr_id == None: continue # Genome with unknown locations are ignored for now
        abs_start = annotation.location_to_bp(chr_id, gnode['start'])
        abs_end = abs_start + gnode['length'] - 1
        fid = gnode['_id'] = str(gnode['_id'])
        try:
            fname = gnode['name']
        except KeyError:
            fname = 'Unknown'
        if start_bp <= abs_start and abs_end <= end_bp:
            color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
            r_data = {'id': fid,
                      'startBp': abs_start,
                      'endBp': abs_end,
                      'labels': [[fname, True, 0, 0, 0]],
                      'yOffsetPx': 0,
                      'heightPx': ANNOTATION_HEIGHT_PX,
                      "segments": [[0, gnode['length'], None, color, 20]],
                      'title': fname,
                      'aggregation': False
                     }
            chr_r_data_in_range[chr_id].append(r_data)
            count_in_range += 1
    t2 = time.time()
    print("Data arrangement take %.3f second" % (t2 - t1))
    ret = []
    for i_ch in range(1, len(chromo_idxs)+1):
        r_data_in_range = chr_r_data_in_range[i_ch]
        if sampling_rate > aggregation_thresh: # turn on aggregation!
            ret += cluster_r_data(r_data_in_range, sampling_rate, track_height_px)
        else:
            ret += fit_results_in_track(r_data_in_range, sampling_rate, track_height_px)
    print("Data aggregation take %.3f second" % (time.time() - t2))
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationId": annotation_id,
        "values": ret,
        "countInRange": count_in_range
    })

def fit_results_in_track(r_data_in_range, sampling_rate, track_height_px):
    if len(r_data_in_range) == 0: return []
    padding = 20 * sampling_rate
    last = r_data_in_range[0]
    ANNOTATION_HEIGHT_PX = last['heightPx']
    ret = [last]
    for r_data in r_data_in_range[1:]:
        if r_data["startBp"] > last['endBp'] + padding:
            ret.append(r_data)
            last = r_data
        elif last["yOffsetPx"] < track_height_px - 2 * ANNOTATION_HEIGHT_PX:
            r_data["yOffsetPx"] = last["yOffsetPx"] + ANNOTATION_HEIGHT_PX + 1
            ret.append(r_data)
            last = r_data
    return ret


def get_real_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px):
    annotation = loaded_annotations[annotation_id]
    start_bp = max(start_bp, annotation.start_bp)
    end_bp = min(end_bp, annotation.end_bp)
    cursor = annotation.db_find(start_bp, end_bp, types=['gene','exon'], min_length=sampling_rate*20)
    ret = []
    padding = 20 * sampling_rate
    ANNOTATION_HEIGHT_PX = int(track_height_px/3) - 1
    last = None
    for feature_data in cursor:
        color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
        feature_data['_id'] = str(feature_data['_id']) # convert to string for json
        try:
            fname = feature_data['name']
        except:
            fname = 'Unknown'
        r_data = {'id': feature_data['_id'],
                  'labels': [[fname, True, 0, 0, 0]],
                  'yOffsetPx': 0,
                  'heightPx': ANNOTATION_HEIGHT_PX,
                  "segments": [[0, feature_data['length'], None, color, 20]],
                  'title': fname
                 }
        i_ch = feature_data['chromid']
        r_data['startBp'] = annotation.location_to_bp(i_ch, feature_data['start'])
        r_data['endBp'] = annotation.location_to_bp(i_ch, feature_data['end'])
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
        "annotationId": annotation_id,
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

#**************************
#*      /track_info       *
#**************************
from sirius.realdata.constants import TRACK_TYPE_SEQUENCE, TRACK_TYPE_FUNCTIONAL, TRACK_TYPE_3D, TRACK_TYPE_NETWORK
@app.route("/track_info")
def track_info():
    """Return a list of track information"""
    mock_track_info = [
        {
            'track_type': TRACK_TYPE_SEQUENCE,
            'title': 'Sequence Tracks',
            'description': 'Raw sequence data'
        },
        {
            'track_type': TRACK_TYPE_FUNCTIONAL,
            'title': 'Functional Tracks',
            'description': 'DNase, RNASeq, ChIP, ATAC-Seq and more'
        },
        {
            'track_type': TRACK_TYPE_3D,
            'title': '3D Structure',
            'description': 'Contact Maps, Chromatin Structure and more'
        },
        {
            'track_type': TRACK_TYPE_NETWORK,
            'title': 'Network Tracks',
            'description': 'Relationships between variants or genes: co-expression, co-inheritance, co-regulation'
        }
    ]
    return json.dumps(mock_track_info + loaded_track_info)


#**************************
#*     /distince_values   *
#**************************
from sirius.realdata.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE

@app.route("/distinct_values/<string:index>", methods=['POST'])
def distinct_values(index):
    """ Return all possible values for a certain index for certain query """
    query = request.get_json()
    if not query:
        print("No query is posted, returning empty list")
        return json.dumps([])
    # We restrict the choices here to prevent crashing the server with sth like index = '_id'
    allowed_query_indices = {
        QUERY_TYPE_GENOME: {'type', 'chromid', 'assembly', 'source', 'info.biosample', 'info.targets'},
        QUERY_TYPE_INFO: {'type', 'source', 'info.description'},
        QUERY_TYPE_EDGE: {'type', 'source'}
    }
    if index not in allowed_query_indices[query['type']]:
        print("Query of index %s is not allowed for %s" % (index, query['type']))
        return json.dumps([])
    query = HashableDict(query)
    result = get_query_distinct_values(query, index)
    print("/distinct_values/%s for query %s returns %d results. " % (index, query, len(result)), get_query_distinct_values.cache_info())
    return json.dumps(result)

@lru_cache(maxsize=10000)
def get_query_distinct_values(query, index):
    qt = QueryTree(query)
    result = qt.find().distinct(index)
    return result

#*******************************
#*         /details            *
#*******************************
from sirius.mongo import GenomeNodes, InfoNodes, Edges

@app.route("/details/<string:data_id>")
def details(data_id):
    if not data_id: return
    data = get_data_with_id(data_id)
    if not data:
        return abort(404, 'data with _id %s not found' % data_id)
    if data_id[0] == 'G' or data_id[0] == 'I':
        relations = node_relations(data_id)
    elif data_id[0] == 'E':
        relations = edge_relations(data)
    else:
        print("Invalid data_id %s, ID should start with G, I or E" % data_id)
    result = {'details': data, 'relations': relations}
    return json.dumps(result)

def get_data_with_id(data_id):
    prefix = data_id[0]
    data = None
    if prefix == 'G':
        data = GenomeNodes.find_one({'_id': data_id})
    elif prefix == 'I':
        data = InfoNodes.find_one({'_id': data_id})
    elif prefix == 'E':
        data = Edges.find_one({'_id': data_id})
    else:
        print("Invalid data_id %s, ID should start with G, I or E" % data_id)
    # format source into string
    if data:
        data['source'] = '/'.join(data['source'])
    else:
        print("Data not found for _id %s" % data_id)
    return data

def node_relations(data_id):
    result = []
    for edge in Edges.find({'from_id': data_id}, limit=100):
        target_data = get_data_with_id(edge['to_id'])
        if target_data:
            description = target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': edge['type'].capitalize() + ' To',
            'source': edge['source'],
            'description': description,
            'id': edge['_id']
        })
    for edge in Edges.find({'to_id': data_id}, limit=100):
        target_data = get_data_with_id(edge['from_id'])
        if target_data:
            description = target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': edge['type'].capitalize() + ' From',
            'source': edge['source'],
            'description': description,
            'id': edge['_id']
        })
    return result

def edge_relations(edge):
    result = []
    from_data = get_data_with_id(edge['from_id'])
    if from_data:
        result.append({
            'title': 'From ' + from_data['type'],
            'source': from_data['source'],
            'description': from_data['name'],
            'id': edge['from_id']
        })
    to_data = get_data_with_id(edge['to_id'])
    if to_data:
        result.append({
            'title': 'To ' + to_data['type'],
            'source': to_data['source'],
            'description': to_data['name'],
            'id': edge['to_id']
        })
    return result


#**************************
#*       /query           *
#**************************

@app.route('/query', methods=['POST'])
def query_api():
    if request.method != 'POST':
        print("/query endpoint works only with post method")
        return ""
    query = HashableDict(request.get_json())
    results = get_query_raw_results(query, index)
    print("/query for query %s returns %d results. " % (query, len(results)), get_query_raw_results.cache_info())
    return json.dumps(results)

@lru_cache(maxsize=10000)
def get_query_raw_results(query):
    qt = QueryTree(query)
    results = list(qt.find())
    return results

