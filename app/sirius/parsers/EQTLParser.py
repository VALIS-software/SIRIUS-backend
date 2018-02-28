#!/usr/bin/env python

from sirius.parsers.Parser import Parser
import os, sys
import json
import re

class EQTLParser(Parser):

    def parse(self):
        """ Parse the high quality eQTL data format. Ref: http://www.exsnp.org/eQTL """
        # Example data
        #  {
        #     "exSNP": "945418",
        #     "exGENEID": "1187",
        #     "exGENE": "CLCNKA",
        #     "High_confidence": "N",
        #     "Population": "CEU",
        #     "CellType": "LCL",
        #     "DataSet": "EGEUV_EUR",
        #     "StudySet": "EGEUV",
        #     "SameSet": "0",
        #     "DiffSet": "1",
        #     "TotalSet": "1"
        #  },

        self.metadata = {'filename': self.filename}
        self.eqtls = []
        with open(self.filename) as infile:
            title = infile.readline().strip()
            labels = title.split('\t')
            for line in infile:
                line = line.strip() # remove '\n'
                if line:
                    d = dict(zip(labels, line.split('\t')))
                    self.eqtls.append(d)
                    if self.verbose and len(self.eqtls) % 100000 == 0:
                        print("%d data parsed" % len(self.eqtls), end='\r')
        self.data = {'metadata': self.metadata, 'eqtls': self.eqtls}


    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        # EdgeNode: Study that connects SNP and gene
        #     Example: { 'from_id': 'snp_rs945418', to_id: 'geneid_1187',
        #                'from_type': 'SNP', 'to_type': 'gene',
        #                ]]'type': 'association',
        #                'sourceurl': 'eQTL',
        #                'info': { "High_confidence": "N",
        #                          "Population": "CEU",
        #                          "CellType": "LCL",
        #                          "DataSet": "EGEUV_EUR",
        #                          "StudySet": "EGEUV",
        #                          "SameSet": "0",
        #                          "DiffSet": "1",
        #                          "TotalSet": "1"
        #                        }
        #              }
        genome_nodes, info_nodes, edge_nodes = [], [], []
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        if 'sourceurl' in self.metadata:
            sourceurl = self.metadata['sourceurl']
        else:
            sourceurl = self.filename
        for d in self.eqtls:
            # create EdgeNode
            edgenode = {'from_id': 'snp_rs'+d['exSNP'], 'to_id': 'geneid_'+d['exGENEID'],
                        'from_type': 'SNP', 'to_type': 'gene',
                        'type': 'association',
                        'sourceurl': sourceurl,
                        'info': dict()
                       }
            for k,v in d.items():
                if k not in ('exSNP', 'exGENEID', 'exGENE'):
                    edgenode['info'][k] = v
            edge_nodes.append(edgenode)
            if self.verbose and len(edge_nodes) % 100000 == 0:
                print("%d varients parsed" % len(edge_nodes), end='\r')
        return genome_nodes, info_nodes, edge_nodes
