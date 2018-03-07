#!/usr/bin/env python

import os, sys
import json
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs

class GFFParser(Parser):

    @property
    def features(self):
        return self.data['features']

    @features.setter
    def features(self, value):
        self.data['features'] = value

    def parse(self):
        """ Parse the GFF3 data format. Ref: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md """
        # Example: {
        #           "seqid": "NC_000001.11",
        #           "source": "BestRefSeq",
        #           "type": "gene",
        #           "start": 11874,
        #           "end": 14409,
        #           "score": ".",
        #           "strand": "+",
        #           "phase": ".",
        #           "attributes": {
        #             "ID": "gene0",
        #             "Dbxref": "GeneID:100287102,HGNC:HGNC:37102",
        #             "Name": "DDX11L1",
        #             "description": "DEAD/H-box helicase 11 like 1",
        #             "gbkey": "Gene",
        #             "gene": "DDX11L1",
        #             "gene_biotype": "misc_RNA",
        #             "pseudo": "true"
        #           }
        #          }
        gff_labels = ['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase', 'attributes']
        metadata = {'filename': self.filename}
        features = []
        for line in open(self.filename):
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                features.append(d)
                if self.verbose and len(features) % 100000 == 0:
                    print("%d data parsed" % len(features), end='\r')
        self.data = {'metadata': metadata, 'features': features}

    def parse_save_data_in_chunks(self, file_prefix='dataChunk', chunk_size=100000):
        """ Specializd function to parse and safe data in chunks to reduce memory usage """
        gff_labels = ['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase', 'attributes']
        metadata = {'filename': self.filename}
        features = []
        i_chunk = 0
        out_filenames = []
        for line in open(self.filename):
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                features.append(d)
                if len(features) == chunk_size:
                    filename = file_prefix + "_%04d.json" % i_chunk
                    chunk_data = {'metadata': metadata, 'features': features}
                    with open(filename, 'w') as out:
                        json.dump(chunk_data, out, indent=2)
                    out_filenames.append(filename)
                    print("%s parsed and saved" % filename)
                    features = []
                    i_chunk += 1
        # add the lask chunk
        if len(features) > 0:
            filename = file_prefix + "_%04d.json" % i_chunk
            chunk_data = {'metadata': metadata, 'features': features}
            with open(filename, 'w') as out:
                json.dump(chunk_data, out, indent=2)
            out_filenames.append(filename)
            print("%s parsed and saved" % filename)
        return out_filenames

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

    def get_mongo_nodes(self):
        # Example:
        # {
        #   "assembly": "GRCh38",
        #   "sourceurl": "test.gff",
        #   "type": "gene",
        #   "chromid": 1,
        #   "info": {
        #     "seqid": "NC_000001.11",
        #     "source": "BestRefSeq",
        #     "score": ".",
        #     "strand": "+",
        #     "phase": ".",
        #     "attributes": {
        #       "ID": "gene0",
        #       "description": "DEAD/H-box helicase 11 like 1",
        #       "gbkey": "Gene",
        #       "gene": "DDX11L1",
        #       "gene_biotype": "misc_RNA",
        #       "pseudo": "true",
        #       "GeneID": "100287102",
        #       "HGNC": "HGNC:37102"
        #     },
        #     "name": "DDX11L1"
        #   },
        #   "start": 11874,
        #   "end": 14409,
        #   "_id": "geneid_100287102",
        #   "length": 2536
        # }
        if hasattr(self, 'mongonodes'): return self.mongonodes
        metadata, features = self.data['metadata'], self.data['features']
        genome_nodes, info_nodes, edge_nodes = [], [], []
        if 'assembly' in metadata:
            assembly = metadata['assembly']
        else:
            # we expect the gff file has a comment line like  #!genome-build GRCh38.p10
            assembly = metadata['genome-build'].split('.')[0]
        if 'sourceurl' in metadata:
            sourceurl = metadata['sourceurl']
        else:
            sourceurl = self.filename
        if not hasattr(self, 'seqid_loc'): self.seqid_loc = dict()
        if not hasattr(self, 'gene_id_set'): self.gene_id_set = set()
        for d in features:
            ft = d['type']
            # we are skipping the contigs for now, since we don't know where they are
            if not d['seqid'].startswith("NC_000"): continue
            if ft == 'region':
                if 'chromosome' in d['attributes']:
                    try:
                        self.seqid_loc[d['seqid']] = chromo_idxs[d['attributes']['chromosome']]
                    except:
                        self.seqid_loc[d['seqid']] = None
            gnode = {'assembly': assembly, 'sourceurl': sourceurl, 'type': ft}
            try:
                gnode['chromid'] = self.seqid_loc[d['seqid']]
            except KeyError:
                gnode['chromid'] = None
            gnode['info'] = dict()
            for k,v in d.items():
                if k == 'start' or k == 'end':
                    gnode[k] = v
                elif k != 'type':
                    gnode['info'][k] = v
            # try to set the info.Name
            try:
                name = gnode['info']['attributes'].pop('Name')
                gnode['info']['name'] = name
            except:
                pass
            # get geneID fron Dbxref, e.g GeneID:100287102,Genbank:NR_046018.2,HGNC:HGNC:37102
            try:
                dbxref = gnode['info']['attributes'].pop('Dbxref')
                for ref in dbxref.split(','):
                    refname, ref_id = ref.split(':', 1)
                    gnode['info']['attributes'][refname] = ref_id
                    # use GeneID as the ID for this gene
                    if refname == 'GeneID' and gnode['type'] == 'gene':
                        if ref_id not in self.gene_id_set:
                            gnode['_id'] = 'geneid_' + ref_id
                            self.gene_id_set.add(ref_id) # make sure it's unique
                        else:
                            print("Warning, gene with GeneID %s already exists!" % ref_id)
            except KeyError:
                pass
            gnode['length'] = gnode['end'] - gnode['start'] + 1
            genome_nodes.append(gnode)
            if self.verbose and len(genome_nodes) % 100000 == 0:
                print("%d GenomeNodes prepared" % len(genome_nodes), end='\r')
        self.mongonodes = (genome_nodes, info_nodes, edge_nodes)
        return self.mongonodes
