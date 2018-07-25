#**************************
#*   Utility Functions    *
#**************************

import json
from threading import Lock
from sirius.mongo import GenomeNodes, InfoNodes, Edges
import functools

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
    if data == None:
        print("Data not found for _id %s" % data_id)
    return data

class HashableDict(dict):
    def __hash__(self):
        return hash(json.dumps(self, sort_keys=True))


import threading
from collections import defaultdict
from functools import lru_cache, _make_key

def threadsafe_lru(*lru_args, **lru_kwargs):
    def real_threadsafe_func(func):
        func = lru_cache(*lru_args, **lru_kwargs)(func)
        lock_dict = defaultdict(threading.Lock)
        def _thread_lru(*args, **kwargs):
            key = _make_key(args, kwargs, typed=False)  
            with lock_dict[key]:
                return func(*args, **kwargs)
        _thread_lru.cache_info = func.cache_info
        _thread_lru.cache_clear = func.cache_clear
        return _thread_lru
    return real_threadsafe_func

