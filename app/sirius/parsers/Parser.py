#!/usr/bin/env python

import os, json, hashlib

class Parser(object):

    @property
    def metadata(self):
        return self.data['metadata']

    @metadata.setter
    def metadata(self, value):
        self.data['metadata'] = value

    def __init__(self, filename, verbose=False):
        self.filename = filename
        self.verbose = verbose
        _, self.ext = os.path.splitext(os.path.basename(filename))
        self.data = {'metadata': {'filename': filename}}

    def parse(self):
        raise NotImplementedError

    def jsondata(self):
        return json.dumps(self.data)

    def save_json(self, filename=None):
        if filename == None:
            filename = self.filename + '.json'
        with open(filename, 'w') as out:
            json.dump(self.data, out, indent=2)

    def load_json(self, filename):
        self.data = json.load(filename)

    def get_mongo_nodes(self):
        raise NotImplementedError

    def save_mongo_nodes(self, filename=None):
        if filename == None: filename = self.filename + '.mongonodes'
        genome_nodes, info_nodes, edge_nodes = self.get_mongo_nodes()
        d = {'genome_nodes': genome_nodes, 'info_nodes': info_nodes, 'edge_nodes': edge_nodes}
        json.dump(d, open(filename, 'w'), indent=2)

    def hash(self, string):
        """ Return a consistent hash string for a string """
        return hashlib.sha256(string.encode('utf-8')).hexdigest()
