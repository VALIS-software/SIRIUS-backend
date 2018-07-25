from sirius.mongo import Edges
from sirius.core.utilities import get_data_with_id

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
