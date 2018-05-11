#==================================#
#            views.py              #
#----------------------------------#
#  Here sits all the api endpoints #
#==================================#

from flask import abort, request, send_from_directory
import json, time
from functools import lru_cache

from sirius.main import app
from sirius.core.QueryTree import QueryTree
from sirius.core.utilities import get_data_with_id, HashableDict
from sirius.helpers.loaddata import loaded_contig_info, loaded_track_types_info, loaded_data_track_info_dict, loaded_data_tracks
from sirius.helpers.constants import TRACK_TYPE_SEQUENCE, TRACK_TYPE_FUNCTIONAL, TRACK_TYPE_3D, TRACK_TYPE_NETWORK, QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE
from sirius.core.annotationtrack import get_annotation_query
from sirius.core.datatrack import get_sequence_data, get_signal_data, old_api_track_data
from sirius.mongo import GenomeNodes, InfoNodes, Edges

#**************************
#*     static urls        *
#**************************
# These urls will be served by Nginx if possible
@app.route('/')
@app.route('/index')
def index():
    return send_from_directory("valis-dist", "index.html")

@app.route('/<path:path>')
def send_file(path):
    return app.send_static_file(path)



#**************************
#*      /contig_info      *
#**************************

@app.route("/contig_info")
def contig_info():
    """
    Endpoint for frontend to pre-fetch the available contigs and their dimensions

    Returns
    -------
    contig_info_list: list (json)
        A list of contig information, each being a dictionary

    Examples
    --------

    >>> print(contig_info())
    [
        {
            'name': 'chr1',
            'length': '248956422',
            'chromosome': 'chr1'
        },
        {
            'name': 'NT_187636.1',
            'length': '248807',
            'chromosome': '19'
        }
    ]

    """
    return json.dumps(loaded_contig_info)



#**************************
#*      /track_info       *
#**************************

@app.route("/track_info")
def track_info():
    """
    Endpoint for rendering the selections in DataBrowser side panel.

    Returns
    -------
    track_info_list: list (json)
        The list of available track types.
        In the list, each track_info is a dictionary with three keys: 'track_type', 'title', 'description'

    """
    mock_track_types_info = [
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
    return json.dumps(mock_track_types_info + loaded_track_types_info)



# This endpoint servers the old version of front end
#**************************
#*     /annotations       *
#**************************

@app.route("/annotations/<string:annotation_id>/<string:contig>/<int:start_bp>/<int:end_bp>", methods=['POST'])
def get_annotationtrack_data(annotation_id, contig, start_bp, end_bp):
    """
    Endpoint for rendering the selections in DataBrowser side panel.

    Returns
    -------
    annotation_data: dictionary (json)
        The return data for the range of annotation track

    Notes
    -----
    The annotation_id is not used at all, but simply returned in the json.

    """
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    track_height_px = int(request.args.get('track_height_px', default=0))
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    return get_annotation_query(annotation_id, contig, start_bp, end_bp, sampling_rate, track_height_px, query)



# This endpoint is left as mock before front end refactoring is done
#**************************
#*       /tracks          *
#**************************
from sirius.mockData.mock_util import getMockData, get_mock_track_data

@app.route("/tracks")
def tracks():
    """Return a list of all track_ids"""
    MOCK_DATA = getMockData()
    return json.dumps(list(MOCK_DATA.values()))

@app.route("/tracks/<string:track_id>")
def track(track_id):
    MOCK_DATA = getMockData()
    """Return the track metadata"""
    if track_id in MOCK_DATA:
        return json.dumps(MOCK_DATA[track_id])
    else:
        abort(404, "Track not found")

@app.route("/tracks/<string:track_id>/<string:contig>/<int:start_bp>/<int:end_bp>")
def get_track_data(track_id, contig, start_bp, end_bp):
    """Return the data for the given track and base pair range"""
    track_height_px = int(request.args.get('track_height_px', default=0))
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    aggregations = request.args.get('aggregations', default='none').split(',')
    return old_api_track_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate)

# This is the new endpoint that will replace /tracks to return real data
#**************************
#*       /datatracks      *
#**************************

@app.route("/datatracks")
def datatracks():
    """
    Endpoint for getting all available data tracks

    Returns
    -------
    loaded_data_tracks: list (json)
        The list of available track types.
        In the list, each track_info is a dictionary with keys: 'id', 'name'

    """
    return json.dumps([{'id': t['id'], 'name': t['name']} for t in loaded_data_tracks])

@app.route("/datatracks/<string:track_id>")
def datatrack_info_by_id(track_id):
    """
    Endpoint for getting all available data tracks

    Returns
    -------
    track_info_dict: dictionary (json)
        The InfoNode that contains the metadata for track_id.

    """
    info = loaded_data_track_info_dict.get(track_id, None)
    if info == None:
        return abort(404, f'track {track_id} not found')
    track_info = {
        'id': info['_id'],
        'type': info['type']
    }
    return json.dumps(track_info)

@app.route("/datatracks/<string:track_id>/<string:contig>/<int:start_bp>/<int:end_bp>")
def datatrack_get_data(track_id, contig, start_bp, end_bp):
    """Return the data for the given track and base pair range"""
    if start_bp > end_bp:
        return abort(404, 'start_bp > end_bp not allowed')
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    if track_id == 'sequence':
        return get_sequence_data(track_id, contig, start_bp, end_bp, sampling_rate)
    else:
        aggregations = request.args.get('aggregations', default='raw').split(',')
        return get_signal_data(track_id, contig, start_bp, end_bp, sampling_rate, aggregations)

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
#*     /distince_values   *
#**************************

@app.route("/distinct_values/<string:index>", methods=['POST'])
def distinct_values(index):
    """ Return all possible values for a certain index for certain query """
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    # We restrict the choices here to prevent crashing the server with sth like index = '_id'
    allowed_query_indices = {
        QUERY_TYPE_GENOME: {'type', 'contig', 'source', 'info.biosample', 'info.targets'},
        QUERY_TYPE_INFO: {'type', 'source', 'info.description', 'info.biosample', 'info.targets', 'info.types'},
        QUERY_TYPE_EDGE: {'type', 'source'}
    }
    if index not in allowed_query_indices[query['type']]:
        return abort(404, f"Query of {index} is not allowed for {query['type']}")
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

@app.route("/details/<string:data_id>")
def details(data_id):
    if not data_id: return
    data = get_data_with_id(data_id)
    if not data:
        return abort(404, f'data with _id {data_id} not found')
    if data_id[0] == 'G' or data_id[0] == 'I':
        relations = node_relations(data_id)
    elif data_id[0] == 'E':
        relations = edge_relations(data)
    else:
        print(f"Invalid data_id {data_id}, ID should start with G, I or E")
    result = {'details': data, 'relations': relations}
    return json.dumps(result)

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
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    results = get_query_full_results(HashableDict(query))
    print(f"{len(results)} results for full query {query} {get_query_full_results.cache_info()}")
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
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    results = get_query_basic_results(HashableDict(query))
    print("%d results for basic query %s" % (len(results), query), get_query_basic_results.cache_info())
    return json.dumps(results)

@lru_cache(maxsize=10000)
def get_query_basic_results(query):
    """ Cached function for getting basic query results """
    if not query: return []
    basic_projection = ['_id', 'source', 'type', 'name', 'contig', 'start', 'end', 'info.description']
    qt = QueryTree(query)
    results = list(qt.find(projection=basic_projection))
    return results
