#**************************
#*   Utility Functions    *
#**************************

import json
from sirius.mongo import GenomeNodes, InfoNodes, Edges

def get_data_with_id(data_id):
    """
    Get one document from MongoDB.

    Parameters
    ----------
    data_id: string
        The _id of the document

    Returns
    -------
    data: dictionary
        The full document found from database
        The 'source' field has been converted from list to a string for easier display

    Notes
    -----
    If the document is not found, will return None

    """
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
    if data != None:
        data['source'] = '/'.join(data['source'])
    else:
        print("Data not found for _id %s" % data_id)
    return data

class HashableDict(dict):
    def __hash__(self):
        return hash(json.dumps(self, sort_keys=True))
