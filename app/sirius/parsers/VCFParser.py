#!/usr/bin/env python

import re, os, gzip
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP

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
        self.metadata['INFO'] = dict()
        self.variants = []
        # for splitting the metadata info line
        pattern = re.compile(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''')
        if os.path.splitext(self.filename)[1] == '.gz':
            filehandle = gzip.open(self.filename, 'rt')
        else:
            filehandle = open(self.filename)
        for line in filehandle:
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
        filehandle.close()

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

    def parse_chunk(self, size=1000000):
        """
        Parse the file line by line and stop when accumulated {size} number of variants data.
        This function is similar to self.parse(), but will be able to limit memory usage.
        Calling this function will not overwrite self.metadata, but will clean and overwrite self.variants
        Return True when the parsing reached the end of file, else return False
        """
        # open the file and keep the filehandle
        if not hasattr(self, 'filehandle') or self.filehandle.closed:
            if os.path.splitext(self.filename)[1] == '.gz':
                self.filehandle = gzip.open(self.filename, 'rt')
            else:
                self.filehandle = open(self.filename)
        # initiate metadata dict
        if 'INFO' not in self.metadata:
            self.metadata['INFO'] = dict()
        # reset variants
        self.variants = []
        # for splitting the metadata info line
        pattern = re.compile(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''')
        for line in self.filehandle:
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
                if len(self.variants) == size:
                    if self.verbose:
                        print(f"Parsing file {self.filename} finished for chunk of size {size}" )
                    break
        else:
            # close the filehandle if the parsing is finished
            if self.verbose:
                print(f"Parsing the entire file {self.filename} finished.")
            self.filehandle.close()
            return True
        return False

class VCFParser_ClinVar(VCFParser):
    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        # GenomeNode: Information about SNP, unique ID is defined based on rs number or varient info
        # InfoNode: Information about trait, unique ID is defined based on trait name
        # EdgeNode: Study that connects SNP and trait

        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node= {
            "_id": 'I'+DATA_SOURCE_CLINVAR,
            "type": "dataSource",
            'name':DATA_SOURCE_CLINVAR,
            "source": DATA_SOURCE_CLINVAR,
            'info': self.metadata.copy()
            }
        info_nodes.append(info_node)
        known_vid, known_traits, known_edge_ids = set(), set(), set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        for d in self.variants:
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in chromo_idxs: continue
            chromid = chromo_idxs[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                rs = str(d["INFO"]["RS"])
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                variant_type = d['INFO']['CLNVC'].lower()
                pos = str(d['POS'])
                v_ref, v_alt = d['REF'], d['ALT']
                variant_key_string = '_'.join([variant_type, str(chromid), pos, v_ref, v_alt])
                variant_id = 'Gvariant_' + self.hash(variant_key_string)
                name = ' '.join(s.capitalize() for s in variant_type.split('_'))
            pos = int(d['POS'])
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'assembly': assembly,
                    'chromid':chromid,
                    'start': pos,
                    'end': pos,
                    'length': 1,
                    'source': DATA_SOURCE_CLINVAR,
                    'name': name,
                    'type': variant_type
                }
                gnode['info'] = {
                    'variant_ref': d["REF"],
                    'variant_alt': d['ALT'],
                    'filter': d['FILTER'],
                    'qual': d['QUAL']
                }
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
                trait_desp = trait_name.replace("_"," ")
                trait_id = 'Itrait_' + self.hash(trait_desp.lower())
                this_trait_ids.append(trait_id)
                short_name = (''.join(s[0] for s in trait_desp.split())).upper()
                if trait_id not in known_traits:
                    infonode = {
                        '_id': trait_id,
                        'type': 'trait',
                        'name': short_name,
                        'source': DATA_SOURCE_CLINVAR,
                        'info': dict()
                    }
                    for nameidx in trait_disdb.split(','):
                        if ':' in nameidx:
                            name, idx = nameidx.split(':', 1)
                            infonode['info'][name] = idx
                    # The info.description is where we search for trait
                    # use space here for text search in MongoDB
                    infonode['info']['description'] = trait_desp
                    info_nodes.append(infonode)
                    known_traits.add(trait_id)
            # create EdgeNode for each trait in this entry
            for trait_id in this_trait_ids:
                # add study to edges
                edge = {'from_id': variant_id , 'to_id': trait_id,
                        'type': 'association',
                        'source': DATA_SOURCE_CLINVAR,
                        'name': 'ClinVar',
                        'info': {
                            'CLNREVSTAT': d['INFO']["CLNREVSTAT"],
                            'p-value': 0,
                        }
                       }
                edge['_id'] = 'E'+self.hash(str(edge))
                if edge['_id'] not in known_edge_ids:
                    known_edge_ids.add(edge['_id'])
                    edges.append(edge)
                if self.verbose and len(edges) % 100000 == 0:
                    print("%d variants parsed" % len(edges), end='\r')
        if self.verbose:
            print("Parsing into mongo nodes finished.")
        return genome_nodes, info_nodes, edges


class VCFParser_dbSNP(VCFParser):
    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node= {
            "_id": 'I'+DATA_SOURCE_DBSNP,
            "type": "dataSource",
            'name':DATA_SOURCE_DBSNP,
            "source": DATA_SOURCE_DBSNP,
            'info': self.metadata.copy()
        }
        info_nodes.append(info_node)
        known_vid = set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference'].split('.',1)[0]
        else:
            assembly = 'GRCh38'
        for d in self.variants:
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in chromo_idxs: continue
            chromid = chromo_idxs[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                rs = str(d["INFO"]["RS"])
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                print(d)
                print("Warning, RS number not found, skipping")
                continue
            pos = int(d['POS'])
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'assembly': assembly,
                    'chromid': chromid,
                    'type': 'SNP',
                    'start': pos,
                    'end': pos,
                    'length': 1,
                    'source': DATA_SOURCE_DBSNP,
                    'name': name,
                    'info': d['INFO'].copy()
                }
                gnode['info'].update({
                    "variant_ref": d["REF"],
                    'variant_alt': d['ALT'],
                    'filter': d['FILTER'],
                    'qual': d['QUAL']
                })
                genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edges
