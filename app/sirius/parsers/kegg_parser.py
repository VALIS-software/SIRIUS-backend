import os, copy
from sirius.parsers.parser import Parser
from sirius.helpers.constants import DATA_SOURCE_KEGG
import xml.etree.ElementTree as ET

class KEGG_XMLParser(Parser):
    """
    Parser for the KEGG pathway XML file format.
    One such xml file will be parsed into a dictionary that contains data for one pathway
    """
    @property
    def pathwaydata(self):
        return self.data['pathwaydata']

    @pathwaydata.setter
    def pathwaydata(self, value):
        self.data['pathwaydata'] = value

    def parse(self):
        """
        Parse the KEGG xml data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. Each xml file contains information for one pathway.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = KEGG_XMLParser("path:hsa00010.xml")
        >>> parser.parse()

        The parsed patient data are stored as a dictionary called self.pathwaydata:

        >>> print(parser.pathwaydata)
        {
            'id': 'path:hsa00010',
            'name': 'Glycolysis / Gluconeogenesis',
            'image': 'http://www.kegg.jp/kegg/pathway/hsa/hsa00010.png',
            'link': 'http://www.kegg.jp/kegg-bin/show_pathway?hsa00010',
            'genes': ['ALDH2', 'BPGM', 'ACSS2', 'TPI1', 'PGM1', 'ADH1A', 'GAPDH', 'PCK1', 'PDHA1',
                'ENO1', 'GCK', 'DLD', 'DLAT', 'HK1', 'PFKL', 'MINPP1', 'ALDOA', 'GALM', 'GPI',
                'ALDH3A1', 'ADPGK', 'PGAM4', 'PKLR', 'AKR1A1', 'PGK1', 'FBP1', 'G6PC', 'LDHAL6A']
        }
        """
        self.filehandle.seek(0)
        tree = ET.parse(self.filehandle)
        root = tree.getroot()
        pathway_id = root.get('name')
        pathway_name = root.get('title')
        pathway_image = root.get('image')
        pathway_link = root.get('link')
        pathway_genes = set()
        for e in root:
            if e.tag == 'entry' and e.get('type') == 'gene':
                for child in e:
                    if child.tag == 'graphics':
                        gname_str = child.get('name')
                        if gname_str.endswith('...'):
                            gname_str = gname_str[:-3]
                        # we only add the first name here
                        pathway_genes.add(gname_str.split(', ')[0])
        self.pathwaydata = {
            'id': pathway_id,
            'name': pathway_name,
            'image': pathway_image,
            'link': pathway_link,
            'genes': list(pathway_genes),
        }

    def get_mongo_nodes(self):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.
        """
        genome_nodes, info_nodes, edges = [], [], []
        p = self.pathwaydata.copy()
        # create one infonode for this patient
        info_node = {
            '_id': 'Ipathway_' + p['id'],
            'type': 'pathway',
            'name': p['name'],
            'source': DATA_SOURCE_KEGG,
            'info': {
                'image_url': p['image'],
                'link': p['link'],
                'genes': p['genes'],
            }
        }
        # load all other fields into info node
        info_nodes.append(info_node)
        return genome_nodes, info_nodes, edges