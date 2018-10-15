import os
import csv
from sirius.parsers.parser import Parser
from sirius.helpers.constants import DATA_SOURCE_NATURE_CAUSAL_VARIANTS


class Parser_NatureCasualVariants(Parser):
    """
    Special parser for loading data from the paper:
    https://www.nature.com/nature/journal/v518/n7539/extref/nature13835-s1.xls
    Which was the data source for the example Giggle analysis
    The file has been converted to a .csv file and uploaded to cloud bucket.
    """

    @property
    def entries(self):
        return self.data['entries']

    @entries.setter
    def entries(self, value):
        self.data['entries'] = value

    def parse(self):
        """ parse the csv file into self.entries """
        self.filehandle.seek(0)
        self.entries = []
        reader = csv.reader(self.filehandle)
        # read the first row as column headers
        self.headers = next(iter(reader))
        for row in reader:
            self.entries.append(dict(zip(self.headers, row)))

    def get_mongo_nodes(self):
        """ convert data from nature13835-s1.csv into MongoDB GenomeNodes """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_NATURE_CAUSAL_VARIANTS, "type": "dataSource", "name": DATA_SOURCE_NATURE_CAUSAL_VARIANTS, "source": DATA_SOURCE_NATURE_CAUSAL_VARIANTS}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_traits = set()
        parsed_snp_ids = set()
        # known_biosamples = set()
        for d in self.entries:
            trait_name = d['Disease'].replace('_', ' ').lower()
            trait_id = 'Itrait' + self.hash(trait_name)
            if trait_id not in known_traits:
                # add unique trait as infonode
                known_traits.add(trait_id)
                info_node = {
                    '_id': trait_id,
                    'name': trait_name,
                    'type': 'trait',
                    'source': DATA_SOURCE_NATURE_CAUSAL_VARIANTS,
                    'info': {
                    }
                }
                info_nodes.append(info_node)
            rsid = d['SNP'].lower()
            snp_id = "Gsnp_" + rsid
            if snp_id not in parsed_snp_ids:
                # add new SNP as genome node
                parsed_snp_ids.add(snp_id)
                contig = d['chr']
                pos = int(d['pos'])
                name = rsid
                gnode = {
                    '_id': snp_id,
                    'contig':contig,
                    'start': pos,
                    'end': pos,
                    'length': 1,
                    'source': DATA_SOURCE_NATURE_CAUSAL_VARIANTS,
                    'name': name,
                    'type': 'SNP',
                    'info': {
                        'IndexSNP_riskAllele': d['IndexSNP_riskAllele'],
                    }
                }
                # add some optional info fields if available
                if d['Annotation'] != 'none':
                    gnode['info']['variant_tags'] = [d['Annotation'] + '_variant']
                if d['nearestGene'] != 'none':
                    gnode['info']['nearest_gene'] = d['nearestGene']
                if d['topEnhancer'] != 'none':
                    gnode['info']['top_enhancer'] = d['topEnhancer']
                genome_nodes.append(gnode)
            # build Edge for the entry
            edge = {
                'name': f'Causal of {trait_name}',
                'from_id': snp_id,
                'to_id': trait_id,
                'type': 'causal:SNP:trait',
                'source': DATA_SOURCE_NATURE_CAUSAL_VARIANTS,
                'info': {
                    'description': f'causal of {trait_name} from SNP {rsid}',
                    'PICS_probability': float(d['PICS_probability']),
                    # 'biosample': [],
                },
            }
            edge['_id'] = 'E' + self.hash(str(edge))
            # # collect info.biosample
            # for sample in self.headers[11:]:
            #     if d[sample] == '1':
            #         biosample = sample.replace('_', ' ')
            #         edge['info']['biosample'].append(biosample)
            #         known_biosamples.add(biosample)
            edges.append(edge)
        # # save all biosamples in dataSource info node
        # info_nodes[0]['info']['biosample'] = sorted(known_biosamples)
        return genome_nodes, info_nodes, edges