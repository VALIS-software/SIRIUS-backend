#!/usr/bin/env python

from flask import abort, request, send_from_directory
import random
import json
from functools import lru_cache
import time
from sirius.main import app
from sirius.realdata.loaddata import loaded_annotations, loaded_track_info
from sirius.realdata.constants import CHROMO_IDXS
from sirius.core.QueryTree import QueryTree
from sirius.core.aggregations import get_aggregation_segments

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

@app.route("/annotations/<string:annotation_id>/<int:start_bp>/<int:end_bp>", methods=['GET','POST'])
def get_annotation_data(annotation_id, start_bp, end_bp):
    start_bp = int(start_bp)
    end_bp = int(end_bp)
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    track_height_px = int(request.args.get('track_height_px', default=0))
    query = request.get_json()
    if query:
        # let us show some real data!!
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    elif annotation_id == "GRCh38":
        # this was the default track
        query = {'type': 'GenomeNode', 'filters':{'assembly': 'GRCh38', 'type':'gene'}}
        result = get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query)
    else:
        result = get_mock_annotation_data(annotation_id, start_bp, end_bp, sampling_rate, track_height_px)
    return result

class HashableDict(dict):
    def __hash__(self):
        return hash(json.dumps(self, sort_keys=True))

@lru_cache(maxsize=10000)
def get_gnome_query_results(query):
    qt = QueryTree(query)#, verbose=True)
    try:
        annotation = loaded_annotations[query['filters']['assembly']]
    except:
        annotation = loaded_annotations['GRCh38']
    # we split the results into each of the 24 chromosomes
    results = [[] for _ in range(len(CHROMO_IDXS)+1)]
    for gnode in qt.find(projection=['_id', 'chromid','start','length','name']):
        chr_id = gnode['chromid']
        if chr_id == None: continue # Genome with unknown locations are ignored for now
        abs_start = annotation.location_to_bp(chr_id, gnode['start'])
        abs_end = abs_start + gnode['length'] - 1
        fid = gnode['_id']
        name = gnode['name']
        genome_data = (abs_start, abs_end, fid, name)
        results[chr_id].append(genome_data)
    return list(map(sorted, results))

def get_annotation_query(annotation_id, start_bp, end_bp, sampling_rate, track_height_px, query, verbose=True):
    t0 = time.time()
    gnome_query_results = get_gnome_query_results(HashableDict(query))
    total_query_count = sum(len(g) for g in gnome_query_results)
    t1 = time.time()
    if verbose:
        print("%d gnome_query_results; %.3f seconds; query %s" % (total_query_count, t1-t0, query), get_gnome_query_results.cache_info())
    # get annotation information for computing abs location
    try:
        annotation = loaded_annotations[query['filters']['assembly']]
    except:
        annotation = loaded_annotations['GRCh38']
    # find the chromosomes in range
    # here we use a naive filter, the performance will be improved if we use interval tree
    all_gnome_in_range = []
    for i_ch in range(1, len(annotation.chromo_end_bps)):
        ch_start = annotation.chromo_end_bps[i_ch-1] + 1
        ch_end = annotation.chromo_end_bps[i_ch]
        gnome_query_ch = gnome_query_results[i_ch]
        if ch_start >= start_bp:
            if ch_start < end_bp:
                if ch_end <= end_bp:
                    # if the entire chromosome is in range, append it
                    all_gnome_in_range.append(gnome_query_ch)
                else:
                    # if the first half of chromosome is in range
                    ch_gnome_in_range = list(filter(lambda g: (g[1] < end_bp), gnome_query_ch))
                    all_gnome_in_range.append(ch_gnome_in_range)
                    break
        elif ch_start < start_bp:
            if ch_end > start_bp:
                if ch_end <= end_bp:
                    # if the later half of the chromosome is in range
                    ch_gnome_in_range = list(filter(lambda g: (g[0] > start_bp), gnome_query_ch))
                    all_gnome_in_range.append(ch_gnome_in_range)
                else:
                    # if the chromosome covers the range
                    ch_gnome_in_range = list(filter(lambda g: (g[0] > start_bp and g[1] < end_bp), gnome_query_ch))
                    all_gnome_in_range.append(ch_gnome_in_range)
                    break
    count_in_range = sum(len(g) for g in all_gnome_in_range)
    t2 = time.time()
    if verbose:
        print("Intersect filter: %d results; %.3f seconds" % (count_in_range, t2 - t1))
    aggregation_thresh = 5000
    ret = []
    if sampling_rate > aggregation_thresh: # turn on aggregation!
        for gnome_in_range in all_gnome_in_range:
            ret += get_aggregation_segments(gnome_in_range, annotation, sampling_rate, track_height_px)
    else:
        for gnome_in_range in all_gnome_in_range:
            ret += get_genome_segments(gnome_in_range, annotation, sampling_rate, track_height_px)
    if verbose:
        print("Data aggregation: assembly %s, %.3f seconds" % (annotation.name, time.time() - t2))
    return json.dumps({
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "trackHeightPx": track_height_px,
        "annotationId": annotation_id,
        "values": ret,
        "countInRange": count_in_range
    })

def get_genome_segments(gnome_in_range, annotation, sampling_rate, track_height_px):
    ret = []
    ANNOTATION_HEIGHT_PX = int(track_height_px / 3) - 1
    last_end, last_y = -float('inf'), 0
    padding = 20 * sampling_rate
    for gnome_data in gnome_in_range:
        (abs_start, abs_end, fid, name) = gnome_data
        will_append = False
        if abs_start > last_end + padding:
            will_append = True
            last_end = abs_end
            yOffset = 0
            last_y = 0
        elif last_y <= track_height_px - 2 * ANNOTATION_HEIGHT_PX + 1:
            will_append = True
            last_end = max(last_end, abs_end)
            yOffset = last_y + ANNOTATION_HEIGHT_PX + 1
            last_y = yOffset
        if will_append == True:
            color = [random.random()*0.5, random.random()*0.5, random.random()*0.5, 1.0]
            r_data = {
                'id': fid,
                'startBp': abs_start,
                'endBp': abs_end,
                'labels': [[name, True, 0, 0, 0]],
                'yOffsetPx': yOffset,
                'heightPx': ANNOTATION_HEIGHT_PX,
                "segments": [[0, abs_end-abs_start+1, None, color, 20]],
                'title': name,
                'aggregation': False
            }
            ret.append(r_data)
    return ret



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
        QUERY_TYPE_INFO: {'type', 'source', 'info.description', 'info.biosample', 'info.targets', 'info.types'},
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
        source_str = '/'.join(edge['source'])
        target_data = get_data_with_id(edge['to_id'])
        if target_data:
            description = 'To ' + target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': source_str + ' ' + edge['type'].capitalize(),
            'source': edge['source'],
            'description': description,
            'id': edge['_id']
        })
    for edge in Edges.find({'to_id': data_id}, limit=100):
        source_str = '/'.join(edge['source'])
        target_data = get_data_with_id(edge['from_id'])
        if target_data:
            description = 'From ' + target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': source_str + ' ' + edge['type'].capitalize(),
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

@app.route('/query/full', methods=['POST'])
def query_full():
    """ Returns results for a query, with only basic information, useful for search """
    if request.method != 'POST':
        print("/query endpoint works only with post method")
        return ""
    query = HashableDict(request.get_json())
    results = get_query_full_results(query)
    print("%d results for full query %s" % (len(results), query), get_query_full_results.cache_info())
    return json.dumps(results)

@lru_cache(maxsize=10000)
def get_query_full_results(query):
    """ Cached function for getting full query results """
    if not query: return []
    qt = QueryTree(query)
    results = list(qt.find())
    return results

@app.route('/query/basic', methods=['POST'])
def query_basic():
    """ Returns results for a query, with only basic information, useful for search """
    if request.method != 'POST':
        print("/query endpoint works only with post method")
        return ""
    query = HashableDict(request.get_json())
    results = get_query_basic_results(query)
    print("%d results for basic query %s" % (len(results), query), get_query_basic_results.cache_info())
    return json.dumps(results)

@lru_cache(maxsize=10000)
def get_query_basic_results(query):
    """ Cached function for getting basic query results """
    if not query: return []
    basic_projection = ['_id', 'source', 'type', 'name', 'chromid', 'start', 'end', 'info.description']
    qt = QueryTree(query)
    results = list(qt.find(projection=basic_projection))
    return results
