#!/usr/bin/env python

from sirius.parsers.Parser import Parser
import os, sys
import json
import re

def str_to_type(s):
    s = s.strip().lower()
    if s == 'string' or s == 'str':
        return str
    elif s == 'float' or s == 'double':
        return float
    elif s == 'integer' or s == 'int':
        return int

class VCFParser(Parser):

    def parse(self):
        """ Parse the vcf data format. Ref: http://www.internationalgenome.org/wiki/Analysis/vcf4.0/ """
        # "varients": [
        #   {
        #     "CHROM": "1",
        #     "POS": "1014042",
        #     "ID": "475283",
        #     "REF": "G",
        #     "ALT": "A",
        #     "QUAL": ".",
        #     "FILTER": ".",
        #     "INFO": {
        #       "ALLELEID": 446939,
        #       "CLNDISDB": "MedGen:C4015293,OMIM:616126,Orphanet:ORPHA319563",
        #       "CLNDN": "Immunodeficiency_38_with_basal_ganglia_calcification",
        #       "CLNHGVS": "NC_000001.11:g.1014042G>A",
        #       "CLNREVSTAT": "criteria_provided,_single_submitter",
        #       "CLNSIG": "Benign",
        #       "CLNVC": "single_nucleotide_variant",
        #       "CLNVCSO": "SO:0001483",
        #       "GENEINFO": "ISG15:9636",
        #       "MC": "SO:0001583|missense_variant",
        #       "ORIGIN": "1",
        #       "RS": "143888043"
        #     }
        #   },
        # ...]
        self.metadata = {'filename': self.filename, 'INFO':dict()}
        self.varients = []
        # for splitting the metadata info line
        pattern = re.compile(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''')
        for line in open(self.filename):
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#':
                    ls = line[2:].split('=', 1)
                    if len(ls) == 2:
                        if ls[0] == 'INFO':
                            fmtstr = ls[1].strip()
                            if fmtstr[0] == '<' and fmtstr[-1] == '>':
                                keyvalues = pattern.split(fmtstr[1:-1])
                                fmtdict = dict(kv.split('=',1) for kv in keyvalues)
                                name = fmtdict["ID"]
                                descpt = fmtdict["Description"]
                                # remove the \" in Description
                                if descpt[0] == '\"' and descpt[-1] == '\"':
                                    descpt = descpt[1:-1]
                                self.metadata['INFO'][name] = {'Type': fmtdict['Type'], "Description": descpt}
                        else:
                            self.metadata[ls[0]] = ls[1]
                else:
                    # title line
                    self.labels = line[1:].split()
            elif line:
                d = self.parse_one_line_data(line)
                self.varients.append(d)
                if self.verbose and len(self.varients) % 100000 == 0:
                    print("%d data parsed" % len(self.varients), end='\r')
        self.data = {'metadata': self.metadata, 'varients': self.varients}

    def parse_one_line_data(self, line):
        ls = line.strip().split('\t')
        assert len(ls) == len(self.labels), "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d = dict(zip(self.labels, ls))
        dinfo = dict()
        for keyandvalue in d['INFO'].split(';'):
            k, v = keyandvalue.split('=', 1)
            vtype = self.metadata['INFO'][k]['Type']
            v = str_to_type(vtype)(v)
            dinfo[k] = v
        d['INFO'] = dinfo
        return d


class ClinVarVCFParser(VCFParser):
    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes:
        GenomeNode: Information about SNP, unique ID is defined based on rs number
            Example:     {
                           "_id": "snp_rs143888043",
                           "assembly": "GRCh38",
                           "location": "Chr1",
                           "sourceurl": "test.vcf",
                           "type": "SNP",
                           "start": 1014042,
                           "end": 1014042,
                           "length": 1,
                           "info": {
                             "variant_ref": "G",
                             "variant_alt": "A",
                             "ALLELEID": 446939,
                             "CLNVCSO": "SO:0001483",
                             "GENEINFO": "ISG15:9636",
                             "MC": "SO:0001583|missense_variant",
                             "ORIGIN": "1",
                             "CLNHGVS": "NC_000001.11:g.1014042G>A"
                           }
                         },

        InfoNode: Information about trait, unique ID is defined based on trait name
            Example:   {
                         "_id": "trait_Immunodeficiency_38_with_basal_ganglia_calcification",
                         "type": "trait",
                         "name": "Immunodeficiency 38 with basal ganglia calcification",
                         "sourceurl": "test.vcf",
                         "info": {
                           "MedGen": "C4015293",
                           "OMIM": "616126",
                           "Orphanet": "ORPHA319563"
                             }
                        },

        EdgeNode: Study that connects SNP and trait
            Example:     {
                            "from_id": "snp_rs143888043",
                            "to_id": "trait_Immunodeficiency_38_with_basal_ganglia_calcification",
                            "from_type": "SNP",
                            "to_type": "trait",
                            "type": "association",
                            "sourceurl": "test.vcf",
                            "info": {
                              "CLNREVSTAT": "criteria_provided,_single_submitter",
                              "QUAL": ".",
                              "FILTER": "."
                            }
                        },
        """
        genome_nodes, info_nodes, edge_nodes = [], [], []
        known_vid, known_traits = set(), set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        if 'sourceurl' in self.metadata:
            sourceurl = self.metadata['source']
        else:
            sourceurl = self.filename
        seqid_loc = dict()
        for d in self.varients:
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                varient_id = "snp_rs" + str(d["INFO"]["RS"])
                varient_type = "SNP"
            else:
                varient_type = d['INFO']['CLNVC'].lower()
                location = 'Chr' + d['CHROM']
                pos = str(d['POS'])
                v_ref, v_alt = d['REF'], d['ALT']
                varient_id = '_'.join([varient_type, location, pos, v_ref, v_alt])
            if varient_id not in known_vid:
                known_vid.add(varient_id)
                location = 'Chr' + d['CHROM']
                gnode = {'_id': varient_id, 'assembly': assembly, 'location':location, 'sourceurl': sourceurl}
                gnode['type'] = varient_type
                gnode['start'] = gnode['end'] = int(d['POS'])
                gnode['length'] = 1
                gnode['info'] = {"variant_ref": d["REF"], 'variant_alt': d['ALT']}
                for key in ('ALLELEID', 'CLNVCSO', 'GENEINFO', 'MC', 'ORIGIN', 'CLNHGVS'):
                    try:
                        gnode['info'][key] = d["INFO"][key]
                    except:
                        pass
                genome_nodes.append(gnode)
            # we will abandon this entry if no trait information found
            if 'CLNDN' not in d['INFO'] or 'CLNDISDB' not in d['INFO']: continue
            # create InfoNode for trait, one entry could have multiple traits
            this_trait_ids = []
            trait_names = d['INFO']['CLNDN'].split('|')
            trait_CLNDISDBs = d['INFO']['CLNDISDB'].split('|')
            if len(trait_names) != len(trait_CLNDISDBs):
                print(d)
                print("Number of traits in in CLNDN and CLNDISDB not consistent! Skipping.")
                continue
            for trait_name, trait_disdb in zip(trait_names, trait_CLNDISDBs):
                trait_id = 'trait_' + trait_name
                this_trait_ids.append(trait_id)
                if trait_id not in known_traits:
                    infonode = { '_id': trait_id,
                                'type': 'trait',
                                'name': trait_name.replace("_"," "), # use space here for text search in MongoDB
                                'sourceurl': sourceurl,
                                'info': dict()
                                }
                    for nameidx in trait_disdb.split(','):
                        name, idx = nameidx.split(':', 1)
                        infonode['info'][name] = idx
                    info_nodes.append(infonode)
                    known_traits.add(trait_id)
            # create EdgeNode for each trait in this entry
            for trait_id in this_trait_ids:
                # add study to edge_nodes
                edgenode = {'from_id': varient_id , 'to_id': trait_id,
                            'from_type': varient_type, 'to_type': 'trait',
                            'type': 'association',
                            'sourceurl': sourceurl,
                            'info': {
                                "CLNREVSTAT": d['INFO']["CLNREVSTAT"],
                                "QUAL": d['QUAL'],
                                "FILTER": d['FILTER']
                            }
                           }
                edge_nodes.append(edgenode)
                if self.verbose and len(edge_nodes) % 100000 == 0:
                    print("%d varients parsed" % len(edge_nodes), end='\r')
        return genome_nodes, info_nodes, edge_nodes


    def save_mongo_nodes(self, filename=None):
        if filename == None: filename = self.filename + '.mongonodes'
        genome_nodes, info_nodes, edge_nodes = self.get_mongo_nodes()
        d = {'genome_nodes': genome_nodes, 'info_nodes': info_nodes, 'edge_nodes': edge_nodes}
        json.dump(d, open(filename, 'w'), indent=2)
