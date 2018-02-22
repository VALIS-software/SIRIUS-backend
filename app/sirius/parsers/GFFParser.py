#!/usr/bin/env python

from sirius.parsers.Parser import Parser
import os, sys
import json

class GFFParser(Parser):
    #def __init__(self, **args):
    #    super.__init__(**args)

    def parse(self):
        """ Parse the GFF3 data format. Ref: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md """
        gff_labels = ['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase', 'attributes']
        self.metadata = {'filename': self.filename}
        self.features = []
        for line in open(self.filename):
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        self.metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        self.metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                self.features.append(d)
                if self.verbose and len(self.features) % 100000 == 0:
                    print("%d data parsed" % len(self.features), end='\r')
        self.data = {'metadata': self.metadata, 'features': self.features}

    def parse_one_line_data(self, line):
        d = dict()
        ls = line.split('\t')
        assert len(ls) == 9, "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d['seqid'], d['source'], d['type'] = ls[0:3]
        d['start'], d['end'] = int(ls[3]), int(ls[4])
        d['score'], d['strand'], d['phase'] = ls[5:8]
        d['attributes'] = dict()
        for attr in ls[8].split(';'):
            key, value = attr.split('=', maxsplit=1)
            d['attributes'][key] = value
        return d

    def get_genomenodes(self):
        genomenodes = []
        gene_id_set = set()
        if 'assembly' in self.metadata:
            assembly = self.metadata['assembly']
        else:
            # we expect the gff file has a comment line like  #!genome-build GRCh38.p10
            assembly = self.metadata['genome-build'].split('.')[0]
        if 'sourceurl' in self.metadata:
            sourceurl = self.metadata['sourceurl']
        else:
            sourceurl = self.filename
        seqid_loc = dict()
        for d in self.features:
            ft = d['type']
            if ft == 'region':
                if 'chromosome' in d['attributes']:
                    seqid_loc[d['seqid']] = 'Chr' + d['attributes']['chromosome']
            gnode = {'assembly': assembly, 'sourceurl': sourceurl, 'type': ft}
            try:
                gnode['location'] = seqid_loc[d['seqid']]
            except KeyError:
                gnode['location'] = "Unknown"
            gnode['info'] = dict()
            for k,v in d.items():
                if k == 'start' or k == 'end':
                    gnode[k] = v
                elif k != 'type':
                    gnode['info'][k] = v
            # get geneID fron Dbxref, e.g GeneID:100287102,Genbank:NR_046018.2,HGNC:HGNC:37102
            try:
                dbxref = gnode['info']['attributes'].pop('Dbxref')
                for ref in dbxref.split(','):
                    refname, ref_id = ref.split(':', 1)
                    gnode['info']['attributes'][refname] = ref_id
                    # use GeneID as the ID for this gene
                    if refname == 'GeneID' and gnode['type'] == 'gene':
                        if ref_id not in gene_id_set:
                            gnode['_id'] = 'geneid_' + ref_id
                            gene_id_set.add(ref_id) # make sure it's unique
                        else:
                            print("Warning, gene with GeneID %s already exists!" % ref_id)
            except KeyError:
                pass
            gnode['length'] = gnode['end'] - gnode['start'] + 1
            genomenodes.append(gnode)
            if self.verbose and len(genomenodes) % 100000 == 0:
                print("%d GenomeNodes prepared" % len(genomenodes), end='\r')
        return genomenodes

    def save_gnode(self, filename=None):
        if filename == None:
            filename = self.filename + '.gnode'
        json.dump(self.get_genomenodes(), open(filename, 'w'), indent=2)
