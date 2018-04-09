#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import DATA_SOURCE_EQTL

class EQTLParser(Parser):

    @property
    def eqtls(self):
        return self.data['eqtls']

    @eqtls.setter
    def eqtls(self, value):
        self.data['eqtls'] = value

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


    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        # EdgeNode: Study that connects SNP and gene
        #     Example: { 'from_id': 'Gsnp_rs945418', to_id: 'Ggeneid_1187',
        #                'from_type': 'SNP', 'to_type': 'gene',
        #                'type': 'association',
        #                'source': 'eQTL',
        #                'name': 'eQTL',
        #                'info': { "High_confidence": "N",
        #                          "Population": "CEU",
        #                          "CellType": "LCL",
        #                          "DataSet": "EGEUV_EUR",
        #                          "StudySet": "EGEUV",
        #                          "SameSet": "0",
        #                          "DiffSet": "1",
        #                          "TotalSet": "1"
        #                        },
        #                '_id': 'Ejfiea12131294012j12903'
        #              }
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_EQTL, "type": "dataSource", 'name': DATA_SOURCE_EQTL, "source": DATA_SOURCE_EQTL}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_edge_ids = set()
        # the eQTL data entries does not provide any useful information about the SNPs, so we will not add GenomeNodes
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        for d in self.eqtls:
            # create EdgeNode
            from_id = 'Gsnp_rs'+d['exSNP']
            to_id = 'Ggeneid_'+d['exGENEID']
            edge = {'from_id': from_id, 'to_id': to_id,
                    'from_type': 'SNP', 'to_type': 'gene',
                    'type': 'association',
                    'source': DATA_SOURCE_EQTL,
                    'name': DATA_SOURCE_EQTL,
                    'info': dict()
                   }
            for k,v in d.items():
                if k not in ('exSNP', 'exGENEID', 'exGENE'):
                    edge['info'][k] = v
            edge['_id'] = 'E'+self.hash(str(edge))
            if edge['_id'] not in known_edge_ids:
                known_edge_ids.add(edge['_id'])
                edges.append(edge)
            if self.verbose and len(edges) % 100000 == 0:
                print("%d varients parsed" % len(edges), end='\r')
        return genome_nodes, info_nodes, edges
