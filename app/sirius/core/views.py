#==================================#
#            views.py              #
#----------------------------------#
#  Here sits all the api endpoints #
#==================================#

from flask import abort, request, send_from_directory
import json, time
from functools import lru_cache

from sirius.main import app
from sirius.core.utilities import get_data_with_id, HashableDict
from sirius.query.QueryTree import QueryTree
from sirius.helpers.loaddata import loaded_contig_info, loaded_track_types_info, loaded_data_track_info_dict, loaded_data_tracks
from sirius.helpers.constants import TRACK_TYPE_SEQUENCE, TRACK_TYPE_FUNCTIONAL, TRACK_TYPE_3D, TRACK_TYPE_NETWORK, TRACK_TYPE_BOOLEAN, \
                                     QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE
from sirius.core.annotationtrack import get_annotation_query
from sirius.core.datatrack import get_sequence_data, get_signal_data, old_api_track_data
from sirius.core.searchindex import get_suggestions
from sirius.mongo import GenomeNodes, InfoNodes, Edges
from sirius.core.auth0 import requires_auth

#**************************
#*     static urls        *
#**************************
# These urls will be served by Nginx if possible
@app.route('/')
@app.route('/index')
@requires_auth
def index():
    return send_from_directory("valis-dist", "index.html")

@app.route('/<path:path>')
@requires_auth
def send_file(path):
    return app.send_static_file(path)



#**************************
#*      /contig_info      *
#**************************

@app.route("/contig_info")
@requires_auth
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
@requires_auth
def track_info():
    """
    Endpoint for rendering the selections in DataBrowser side panel.

    Returns
    -------
    track_info_list: list (json)
        The list of available track types.
        In the list, each track_info is a dictionary with three keys: 'track_type', 'title', 'description'

    """
    return json.dumps(loaded_track_types_info)



# This endpoint servers the old version of front end
#**************************
#*     /annotations       *
#**************************

@app.route("/annotations/<string:annotation_id>/<string:contig>/<int:start_bp>/<int:end_bp>", methods=['POST'])
@requires_auth
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
@requires_auth
def tracks():
    """Return a list of all track_ids"""
    MOCK_DATA = getMockData()
    return json.dumps(list(MOCK_DATA.values()))

@app.route("/tracks/<string:track_id>")
@requires_auth
def track(track_id):
    MOCK_DATA = getMockData()
    """Return the track metadata"""
    if track_id in MOCK_DATA:
        return json.dumps(MOCK_DATA[track_id])
    else:
        abort(404, "Track not found")

@app.route("/tracks/<string:track_id>/<string:contig>/<int:start_bp>/<int:end_bp>")
@requires_auth
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
@requires_auth
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
@requires_auth
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
@requires_auth
def datatrack_get_data(track_id, contig, start_bp, end_bp):
    """Return the data for the given track and base pair range"""
    if start_bp > end_bp:
        return abort(404, 'start_bp > end_bp not allowed')
    sampling_rate = int(request.args.get('sampling_rate', default=1))
    if track_id == 'sequence':
        return get_sequence_data(track_id, contig, start_bp, end_bp, sampling_rate)
    else:
        aggregations = request.args.get('aggregations', default='none').split(',')
        return get_signal_data(track_id, contig, start_bp, end_bp, sampling_rate, aggregations)

# This part is still mock
#**************************
#*        /graphs         *
#**************************

@app.route("/graphs")
@requires_auth
def graphs():
    return json.dumps(["ld_score"])

@app.route("/graphs/<string:graph_id>/<string:annotation_id1>/<string:annotation_id2>/<int:start_bp>/<int:end_bp>")
@requires_auth
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
@requires_auth
def distinct_values(index):
    """ Return all possible values for a certain index for certain query """
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    # We restrict the choices here to prevent crashing the server with sth like index = '_id'
    allowed_query_indices = {
        QUERY_TYPE_GENOME: {'type', 'contig', 'source', 'info.biosample', 'info.targets', 'info.variant_tags', 'info.source'},
        QUERY_TYPE_INFO: {'type', 'source', 'name', 'info.biosample', 'info.targets', 'info.types', 'info.assay', 'info.outtype', 'info.variant_tags'},
        QUERY_TYPE_EDGE: {'type', 'source', 'info.biosample'}
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
    result = qt.distinct(index)
    return result



#*******************************
#*         /details            *
#*******************************

@app.route("/details/<string:data_id>")
@requires_auth
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
        target_data = get_data_with_id(edge['to_id'])
        if target_data:
            description = 'To ' + target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': edge['name'],
            'type': edge['type'],
            'source': edge['source'],
            'description': description,
            'id': edge['_id']
        })
    for edge in Edges.find({'to_id': data_id}, limit=100):
        target_data = get_data_with_id(edge['from_id'])
        if target_data:
            description = 'From ' + target_data['type'] + ' ' + target_data['name']
        else:
            description = "data not found"
        result.append({
            'title': edge['name'],
            'type': edge['type'],
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
            'id': edge['from_id'],
            'type': from_data['type']
        })
    to_data = get_data_with_id(edge['to_id'])
    if to_data:
        result.append({
            'title': 'To ' + to_data['type'],
            'source': to_data['source'],
            'description': to_data['name'],
            'id': edge['to_id'],
            'type': to_data['type']
        })
    return result


#**************************
#*       /suggestions     *
#**************************
@app.route('/suggestions', methods=['POST'])
@requires_auth
def suggestions():
    """ Returns results for a query, with only basic information, useful for search """
    query_json = request.get_json()
    if not query_json or not "term_type" in query_json:
        return abort(500, 'no term_type')
    if not query_json or not "search_text" in query_json:
        return abort(500, 'no search_text')
    max_results = int(query_json.get('max_results', 100))
    results = {
        "results": get_suggestions(query_json["term_type"], query_json["search_text"], max_results)
    }
    print(query_json)
    print(results['results'])
    return json.dumps(results)

#**************************
#*       /query           *
#**************************

class QueryResultsCache:
    """
    Class that implemented dynamic caching for query results
    """
    def __init__(self, query, projection=None):
        self.qt = QueryTree(query)
        self.data_generator = self.qt.find(projection=projection)
        self.projection = projection
        self.loaded_data = []
        self.load_finished = False

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.step != None and key.step <= 0:
                raise ValueError("slice step cannot be zero or negative")
            self.load_data_until(key.stop)
            return self.loaded_data[key]
        elif isinstance(key, int):
            self.load_data_until(slice.stop + 1)
            return self.loaded_data[key]
        else:
            raise TypeError(f"list indices must be integers or slices, not {type(key)}")

    def load_data_until(self, index=None):
        # if we already have that many results
        if index != None and index <= len(self.loaded_data): return
        # iteratively load data into cache
        for data in self.data_generator:
            # convert "_id" to "id" for frontend
            data['id'] = data.pop('_id')
            self.loaded_data.append(data)
            # here we load one more data than requested, so we know if all data loaded
            if len(self.loaded_data) == index:
                break
        else:
            # all data loaded
            self.load_finished = True

@app.route('/query/full', methods=['POST'])
@requires_auth
def query_full():
    """ Returns results for a query, with only basic information, useful for search """
    t0 = time.time()
    result_start = request.args.get('result_start', default=None)
    result_end = request.args.get('result_end', default=None)
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    result_start = int(result_start) if result_start != None else 0
    if result_start < 0:
        return abort(404, 'result_start should >= 0')
    if result_end != None:
        result_end = int(result_end)
        if result_end <= result_start:
            return abort(404, 'result_end should > result_start')
    results_cache = get_query_full_results(HashableDict(query))
    results = results_cache[result_start:result_end]
    t1 = time.time()
    print(f"full query {query} {len(results)} cache_info: {get_query_full_results.cache_info()} {t1-t0:.1f} s")
    result_end = result_start + len(results)
    reached_end = False
    if results_cache.load_finished and result_end >= len(results_cache.loaded_data):
        reached_end = True
    return_dict = {
        "result_start": result_start,
        "result_end": result_end,
        "reached_end": reached_end,
        "data": results,
        "query": query
    }
    return json.dumps(return_dict)

@lru_cache(maxsize=1000)
def get_query_full_results(query):
    """ Cached function for getting full query results """
    if not query: return []
    return QueryResultsCache(query)

@app.route('/query/basic', methods=['POST'])
@requires_auth
def query_basic():
    """ Returns results for a query, with only basic information, useful for search """
    t0 = time.time()
    result_start = request.args.get('result_start', default=None)
    result_end = request.args.get('result_end', default=None)
    query = request.get_json()
    if not query:
        return abort(404, 'no query posted')
    result_start = int(result_start) if result_start != None else 0
    if result_start < 0:
        return abort(404, 'result_start should >= 0')
    if result_end != None:
        result_end = int(result_end)
        if result_end <= result_start:
            return abort(404, 'result_end should > result_start')
    results_cache = get_query_basic_results(HashableDict(query))
    results = results_cache[result_start:result_end]
    t1 = time.time()
    print(f"basic query {query} {len(results)} cache_info: {get_query_full_results.cache_info()} {t1-t0:.1f} s")
    result_end = result_start + len(results)
    reached_end = False
    if results_cache.load_finished and result_end >= len(results_cache.loaded_data):
        reached_end = True
    return_dict = {
        "result_start": result_start,
        "result_end": result_end,
        "reached_end": reached_end,
        "data": results,
        "query": query
    }
    return json.dumps(return_dict)

@lru_cache(maxsize=1000)
def get_query_basic_results(query):
    """ Cached function for getting basic query results """
    if not query: return []
    basic_projection = ['_id', 'source', 'type', 'name', 'contig', 'start', 'end', 'info.description']
    return QueryResultsCache(query, projection=basic_projection)


#**************************
#*       /reference       *
#**************************


@app.route("/reference/<string:contig>/<int:start_bp>/<int:end_bp>", methods=['GET'])
def reference_annotation_track(contig, start_bp, end_bp):
    include_transcript = bool(request.args.get('include_transcript', default=False))
    # get data from cache
    if include_transcript == True:
        data = get_reference_hierarchy_data(contig)
    else:
        data = get_reference_gene_data(contig)
    # filter the ones out of range
    result_data = [g for g in data if g['start'] <= end_bp and g['start'] + g['length'] >= start_bp]
    if len(result_data) > 0:
        start_bp = min(start_bp, result_data[0]['start'])
        end_bp = max(end_bp, result_data[-1]['start'] + result_data[-1]['length'] - 1)
    result = {
        'contig': contig,
        'start_bp': start_bp,
        'end_bp': end_bp,
        'data': result_data
    }
    return json.dumps(result)

@lru_cache(maxsize=1000)
def get_reference_gene_data(contig):
    """ Find all genes in a contig """
    result = []
    # First we find all the genes
    gene_types = ['gene', 'pseudogene']
    gene_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand']
    all_genes = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': gene_types}}, projection=gene_projection), key=lambda x: x['start'])
    # second we convert `_id` to `id`
    for gene in all_genes:
        gene['id'] = gene.pop('_id')
        gene['strand'] = gene.pop('info').pop('strand')
    return all_genes

@lru_cache(maxsize=1000)
def get_reference_hierarchy_data(contig):
    """ Find all genes in a contig, then build the gene->transcript->exon hierarchy """
    # First we find all the genes
    gene_types = ['gene', 'pseudogene']
    gene_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand']
    all_genes = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': gene_types}}, projection=gene_projection), key=lambda x: x['start'])
    # Second store their index
    gene_idx_dict = dict()
    for i, gene in enumerate(all_genes):
        gene['id'] = gid = gene.pop('_id')
        gene['strand'] = gene.pop('info').pop('strand')
        gene['transcripts'] = []
        gene_idx_dict[gid] = i
    # Third we find all the transcripts
    transcript_types = ['transcript', 'pseudogenic_transcript', 'miRNA', 'lnc_RNA', 'mRNA']
    transcript_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand', 'info.Parent']
    all_transcripts = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': transcript_types}}, projection=transcript_projection), key=lambda x: x['start'])
    # Fourth we put the transcripts into genes and store their parent genes
    gene_transcript_idx_dict = dict()
    for transcript in all_transcripts:
        parent = transcript['info'].pop('Parent', None)
        if parent != None:
            parent_id = 'G' + parent.split(':')[-1]
            gene_idx = gene_idx_dict.get(parent_id, None)
            if gene_idx != None:
                transcript['id'] = gid = transcript.pop('_id')
                transcript['strand'] = transcript.pop('info').pop('strand')
                transcript['components'] = []
                gene_transcript_idx_dict[gid] = (gene_idx, len(all_genes[gene_idx]['transcripts']))
                all_genes[gene_idx]['transcripts'].append(transcript)
    # Fifth we find all the exons
    exon_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand', 'info.Parent']
    all_exons = sorted(GenomeNodes.find({'contig': contig, 'type': 'exon'}, projection=exon_projection), key=lambda x: x['start'])
    # Sixth we put all the exons into their parent transcripts
    for exon in all_exons:
        parent = exon['info'].pop('Parent', None)
        if parent != None:
            parent_id = 'G' + parent.split(':')[-1]
            gene_idx, transcript_idx = gene_transcript_idx_dict.get(parent_id, (None, None))
            if gene_idx != None:
                exon['id'] = exon.pop('_id')
                exon['strand'] = exon.pop('info').pop('strand')
                all_genes[gene_idx]['transcripts'][transcript_idx]['components'].append(exon)
    return all_genes

@app.route('/sleep')
def sleep():
    time.sleep(2)
    return f'waking up at {time.ctime()}'

