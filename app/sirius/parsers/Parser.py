#!/usr/bin/env python

import os, sys
import json

class Parser(object):

    def __init__(self, filename, verbose=False):
        self.filename = filename
        self.verbose = verbose
        _, self.ext = os.path.splitext(os.path.basename(filename))
        self.data = None

    def parse(self):
        if self.ext == '.gff':
            self.parse_gff(self.filename)
        else:
            raise NotImplementedError("Parser for %s type not available yet." % self.ext)

    def parse_gff(self, filename):
        gff_labels = ['sequence', 'source', 'feature', 'start', 'end', 'score', 'strand', 'phase', 'Attributes']
        self.metadata = {'filename': self.filename}
        self.data_list = []
        for line in open(filename):
            line = line.strip()
            if line[0] == '#':
                ls = line[2:].split(maxsplit=1)
                if len(ls) == 2:
                    self.metadata[ls[0]] = ls[1]
                elif len(ls) == 1:
                    self.metadata[ls[0]] = None
            else:
                ls = line.split('\t')
                assert len(ls) == 9, "Error reading this line:\n%s"%line
                d = dict(zip(gff_labels, ls))
                d['start'], d['end'] = int(d['start']), int(d['end'])
                attributes = dict()
                for attr in d['Attributes'].split(';'):
                    key, value = attr.split('=', maxsplit=1)
                    attributes[key] = value
                d['Attributes'] = attributes
                self.data_list.append(d)
                if self.verbose and len(self.data_list) % 100000 == 0:
                    print("%d data parsed" % len(self.data_list), end='\r')
        self.data = {'metadata': self.metadata, 'data': self.data_list}

    def jsondata(self):
        return json.dumps(self.data)

    def save_json(self, filename=None):
        if filename == None:
            filename = self.filename + '.json'
        with open(filename, 'w') as out:
            json.dump(self.data, out, indent=2)

    def get_genomenodes(self):
        genomenodes = []
        location = None
        for d in self.data_list:
            ft = d['feature']
            if ft == 'region':
                if 'chromosome' in d['Attributes']:
                    location = 'chr' + d['Attributes']['chromosome']
            else:
                gd = dict()
                gd['location'] = location
                gd['info'] = dict()
                for k,v in d.items():
                    if k == 'start' or k == 'end':
                        gd[k] = v
                    elif k == 'source':
                        gd['datasource'] = v
                    elif k != 'feature':
                        gd['info'][k] = v
                genomenodes.append(gd)
                if self.verbose and len(genomenodes) % 100000 == 0:
                    print("%d GenomeNodes prepared" % len(genomenodes), end='\r')
        return genomenodes


def test():
    parser = Parser('test.gff')
    parser.parse()
    print(parser.jsondata)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        parser = Parser(sys.argv[1], verbose=True)
        parser.parse()
        parser.save_json()
