#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GWAS
from Bio import SeqIO
import math

class FASTAParser(Parser):

    def load_to_tile_db(seq_record, tileServerId, resolution_step, min_resolution):
        scale = len(seq_record) / min_resolution
        tile_count = math.ceil(math.log(scale) / math.log(resolution_step))

        resolutions = [resolution_step**i for i in xrange(0, tile_count + 1)]
        
        # create tiles for each resolution

        return resolutions
        

    def parse(self):
        """ Parse the FASTA format using BioPython"""
        chrIdx = 0
        fname = "GRCh38_latest_genomic.fna"
        info_node = {
            "_id": "IsequenceHomoSapienGRCh38",
            "type" : "sequence",
            "name": "Homo Sapien (GRCh38)",
            "source" : "RefSeq",
            "info": {}
        }
        chromosomes = []
        for seq_record in SeqIO.parse(fname, "fasta"):
            if (len(seq_record) > 20000000):
                if chrIdx == 22:
                    chrName = "chrX"
                if chrIdx == 23:
                    chrName = "chrY"
                else:
                    chrName = "chr" + str(chrIdx + 1)
                # TODO: create tileDB file from raw sequence data
                tileServerId = fname + "_" + str(chrIdx)
                resolutions = self.load_to_tile_db(seq_record, tileServerId, 16)
                chrInfo = {
                    "length" : len(seq_record),
                    "tileServerId": tileServerId,
                    "resolutions": resolutions,
                    "name": chrName,
                    "chrIdx": chrIdx
                }
                chromosomes.append(chrInfo)
                chrIdx += 1
        info_node["info"]["chromosomes"] = chromosomes
        self.info_nodes = [info_node]



    def get_mongo_nodes(self):
        """ Parse FASTA into InfoNodes for sequence """
        #    {
        #      "_id": "IsequenceXXXXXX",
        #      "type": "sequence",
        #      "name": "Homo Sapien (GRCh38)",
        #      "source": "RefSeq"
        #      'info': {
        #        'description': "GRCh38 Alignment for Homo Sapien"
        #        'chromosomes': [
        #            {
        #               "length": 320803000,
        #               "tileServerId": "GRCh38chr1",
        #               "name" : "chr1",
        #               "index" : 0,
        #            } 
        #            ...
        #            {
        #               "length": 5803000,
        #               "tileServerId": "GRCh38chrX",
        #               "name" : "chrX",
        #               "index" : 22,
        #            } 
        #         ]
        #      }
        #    }
        if hasattr(self, 'mongonodes'): return self.mongonodes
        
        self.mongonodes = [], self.info_nodes, []
        return self.mongonodes
