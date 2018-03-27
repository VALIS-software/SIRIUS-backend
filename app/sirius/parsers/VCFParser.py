#!/usr/bin/env python

import re
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_CLINVAR

def str_to_type(s):
    s = s.strip().lower()
    if s == 'string' or s == 'str':
        return str
    elif s == 'float' or s == 'double':
        return float
    elif s == 'integer' or s == 'int':
        return int
    elif s == 'flag':
        return bool

class VCFParser(Parser):

    @property
    def variants(self):
        return self.data['variants']

    @variants.setter
    def variants(self, value):
        self.data['variants'] = value

    def parse(self):
        """ Parse the vcf data format. Ref: http://www.internationalgenome.org/wiki/Analysis/vcf4.0/ """
        # "variants": [
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
        self.variants = []
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
                self.variants.append(d)
                if self.verbose and len(self.variants) % 100000 == 0:
                    print("%d data parsed" % len(self.variants), end='\r')

    def parse_one_line_data(self, line):
        ls = line.strip().split('\t')
        assert len(ls) == len(self.labels), "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d = dict(zip(self.labels, ls))
        dinfo = dict()
        for keyandvalue in d['INFO'].split(';'):
            if '=' in keyandvalue:
                k, v = keyandvalue.split('=', 1)
                vtype = self.metadata['INFO'][k]['Type']
                v = str_to_type(vtype)(v)
            else:
                k = keyandvalue
                vtype = self.metadata['INFO'][k]['Type']
                if vtype.lower() != 'flag':
                    print('line\nWarning! No "=" found in the key %s, but its Type is not Flag' % k)
                v = True # set flag to true
            dinfo[k] = v
        d['INFO'] = dinfo
        return d


class VCFParser_ClinVar(VCFParser):
    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        # GenomeNode: Information about SNP, unique ID is defined based on rs number or varient info
        # InfoNode: Information about trait, unique ID is defined based on trait name
        # EdgeNode: Study that connects SNP and trait

        genome_nodes, info_nodes, edge_nodes = [], [], []
        # add dataSource into InfoNodes
        info_node = self.metadata.copy()
        info_node.update({"_id": DATA_SOURCE_CLINVAR, "type": "dataSource", "source": DATA_SOURCE_CLINVAR})
        info_nodes.append(info_node)
        known_vid, known_traits = set(), set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        seqid_loc = dict()
        for d in self.variants:
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in chromo_idxs: continue
            chromid = chromo_idxs[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                variant_id = "snp_rs" + str(d["INFO"]["RS"])
                variant_type = "SNP"
            else:
                variant_type = d['INFO']['CLNVC'].lower()
                pos = str(d['POS'])
                v_ref, v_alt = d['REF'], d['ALT']
                variant_key_string = '_'.join([variant_type, str(chromid), pos, v_ref, v_alt])
                variant_id = 'variant_' + self.hash(variant_key_string)
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {'_id': variant_id, 'assembly': assembly, 'chromid':chromid, 'source': DATA_SOURCE_CLINVAR}
                gnode['type'] = variant_type
                gnode['start'] = gnode['end'] = int(d['POS'])
                gnode['length'] = 1
                gnode['info'] = {"variant_ref": d["REF"], 'variant_alt': d['ALT'], 'filter': d['FILTER'], 'qual': d['QUAL']}
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
                trait_id = 'trait_' + self.hash(trait_name.lower())
                this_trait_ids.append(trait_id)
                if trait_id not in known_traits:
                    infonode = { '_id': trait_id,
                                'type': 'trait',
                                'name': trait_name.replace("_"," "), # use space here for text search in MongoDB
                                'source': DATA_SOURCE_CLINVAR,
                                'info': dict()
                                }
                    for nameidx in trait_disdb.split(','):
                        if ':' in nameidx:
                            name, idx = nameidx.split(':', 1)
                            infonode['info'][name] = idx
                    info_nodes.append(infonode)
                    known_traits.add(trait_id)
            # create EdgeNode for each trait in this entry
            for trait_id in this_trait_ids:
                # add study to edge_nodes
                edgenode = {'from_id': variant_id , 'to_id': trait_id,
                            'from_type': variant_type, 'to_type': 'trait',
                            'type': 'association',
                            'source': DATA_SOURCE_CLINVAR,
                            'info': {
                                "CLNREVSTAT": d['INFO']["CLNREVSTAT"]
                            }
                           }
                edge_nodes.append(edgenode)
                if self.verbose and len(edge_nodes) % 100000 == 0:
                    print("%d variants parsed" % len(edge_nodes), end='\r')
        if self.verbose:
            print("Parsing into mongo nodes finished.")
        return genome_nodes, info_nodes, edge_nodes


class VCFParser_dbSNP(VCFParser):
    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        genome_nodes, info_nodes, edge_nodes = [], [], []

        known_vid, known_traits = set(), set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference'].split('.',1)[0]
        else:
            assembly = 'GRCh38'
        if 'sourceurl' in self.metadata:
            sourceurl = self.metadata['source']
        else:
            sourceurl = self.filename
        seqid_loc = dict()
        for d in self.variants:
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in chromo_idxs: continue
            chromid = chromo_idxs[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                variant_id = "snp_rs" + str(d["INFO"]["RS"])
                variant_type = "SNP"
            else:
                print(d)
                print("Warning, RS number not found, skipping")
                continue
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {'_id': variant_id, 'assembly': assembly, 'chromid':chromid, 'sourceurl': sourceurl}
                gnode['type'] = variant_type
                gnode['start'] = gnode['end'] = int(d['POS'])
                gnode['length'] = 1
                gnode['info'] = {"variant_ref": d["REF"], 'variant_alt': d['ALT'], 'filter': d['FILTER'], 'qual': d['QUAL']}
                gnode['info'].update(d["INFO"])
                genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edge_nodes
