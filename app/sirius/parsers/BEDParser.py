#!/usr/bin/env python

import json
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_ENCODE, ENCODE_COLOR_TYPES
from sirius.realdata.synonyms import Synonyms

class BEDParser(Parser):
    def parse(self):
        """ Parse the .bed data format. Ref: https://genome.ucsc.edu/FAQ/FAQformat.html#format1 """
        bed_labels = ['chrom', 'start', 'end', 'name', 'score', 'strand', 'thickStart', 'thickEnd', 'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
        chr_name_id = dict(('chr'+s, i) for s,i in chromo_idxs.items())
        intervals = []
        for line in open(self.filename):
            ls = line.strip().split('\t') # remove '\n'
            if ls[0] in chr_name_id:
                intervals.append(dict([*zip(bed_labels, ls)]))
                if self.verbose and len(intervals) % 100000 == 0:
                    print("%d data parsed" % len(intervals), end='\r')
        if self.verbose:
            print("Parsing BED data finished.")
        self.data['intervals'] = intervals

    def parse_save_data_in_chunks(self, file_prefix='dataChunk', chunk_size=100000):
        """ Parse and safe data in chunks to reduce memory usage """
        bed_labels = ['chrom', 'start', 'end', 'name', 'score', 'strand', 'thickStart', 'thickEnd', 'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
        chr_name_id = dict(('chr'+s, i) for s,i in chromo_idxs.items())
        intervals = []
        i_chunk = 0
        out_filenames = []
        for line in open(self.filename):
            ls = line.strip().split('\t') # remove '\n'
            if ls[0] in chr_name_id:
                intervals.append(dict([*zip(bed_labels, ls)]))
                if len(intervals) == chunk_size:
                    filename = file_prefix + "_%04d.json" % i_chunk
                    chunk_data = {'metadata': self.metadata, 'intervals': intervals}
                    with open(filename, 'w') as out:
                        json.dump(chunk_data, out, indent=2)
                    out_filenames.append(filename)
                    print("%s parsed and saved" % filename)
                    intervals = []
                    i_chunk += 1
        # add the lask chunk
        if len(intervals) > 0:
            filename = file_prefix + "_%04d.json" % i_chunk
            chunk_data = {'metadata': self.metadata, 'intervals': intervals}
            with open(filename, 'w') as out:
                json.dump(chunk_data, out, indent=2)
            out_filenames.append(filename)
            print("%s parsed and saved" % filename)
        return out_filenames

class BEDParser_ENCODE(BEDParser):
    def get_mongo_nodes(self):
        if hasattr(self, 'mongonodes'): return self.mongonodes
        # these four should be set by downloading script for ENCODE data
        biosample = self.metadata['biosample']
        accession = self.metadata['accession']
        description = self.metadata['description']
        targets = self.metadata['targets']
        self.metadata['assembly'] = Synonyms[self.metadata['assembly']]
        # dict for converting chr in bed file to chromid
        chr_name_id = dict(('chr'+s, i) for s,i in chromo_idxs.items())
        # start parsing
        genome_nodes, info_nodes, edges = [], [], []
        # add data as GenomeNodes
        assembly = self.metadata['assembly']
        all_types = set()
        for interval in self.data['intervals']:
            d = interval.copy()
            color = tuple(int(c) for c in d.pop('itemRgb').split(','))
            tp = ENCODE_COLOR_TYPES[color]
            all_types.add(tp) # keep track of the types for this data file
            name = d.pop('name')
            chromid = chr_name_id[d.pop('chrom')]
            start, end = int(d.pop('start')), int(d.pop('end'))
            gnode = {
                'assembly': assembly,
                'source': DATA_SOURCE_ENCODE,
                'type': tp,
                'name': name,
                'chromid': chromid,
                'start': start,
                'end':end,
                'length': end-start+1,
            }
            gnode['info'] = d.copy()
            gnode['info'].update({
                'biosample': biosample,
                'accession': accession,
                'targets': targets,
            })
            gnode['_id'] = 'G' + '_' + self.hash(str(gnode))
            genome_nodes.append(gnode)
            if self.verbose and len(genome_nodes) % 100000 == 0:
                print("%d GenomeNodes prepared" % len(genome_nodes), end='\r')
        # add ENCODE_accession into InfoNodes
        info_node = {"_id": 'I_'+accession, "type": "ENCODE_accession", "name": accession, "source": DATA_SOURCE_ENCODE}
        info_node['info'] = self.metadata.copy()
        # store all available types in the InfoNode
        info_node['info']['types'] = list(all_types)
        info_nodes.append(info_node)
        if self.verbose:
            print("Parsing BED into mongo nodes finished.")
        self.mongonodes = (genome_nodes, info_nodes, edges)
        return self.mongonodes
