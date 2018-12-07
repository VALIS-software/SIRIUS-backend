#**************************
#*   Utility Functions    *
#**************************

import json
import threading
from collections import defaultdict
import cachetools.keys
import cachetools.func

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
    if data == None:
        print("Data not found for _id %s" % data_id)
    return data

class HashableDict(dict):
    def __hash__(self):
        return hash(json.dumps(self, sort_keys=True))


def threadsafe_lru(*lru_args, **lru_kwargs):
    def real_threadsafe_func(func):
        func = cachetools.func.lru_cache(*lru_args, **lru_kwargs)(func)
        lock_dict = defaultdict(threading.Lock)
        def _thread_safe_func(*args, **kwargs):
            key = cachetools.keys.hashkey(*args, **kwargs)
            with lock_dict[key]:
                return func(*args, **kwargs)
        _thread_safe_func.cache_info = func.cache_info
        _thread_safe_func.cache_clear = func.cache_clear
        return _thread_safe_func
    return real_threadsafe_func

def threadsafe_ttl_cache(*ttl_args, **ttl_kwargs):
    def real_threadsafe_func(func):
        func = cachetools.func.ttl_cache(*ttl_args, **ttl_kwargs)(func)
        lock_dict = defaultdict(threading.Lock)
        def _thread_safe_func(*args, **kwargs):
            key = cachetools.keys.hashkey(*args, **kwargs)
            with lock_dict[key]:
                return func(*args, **kwargs)
        _thread_safe_func.cache_info = func.cache_info
        _thread_safe_func.cache_clear = func.cache_clear
        return _thread_safe_func
    return real_threadsafe_func
