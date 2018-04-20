#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GWAS
from Bio import SeqIO
import math
import gzip
import os

# the exponent for each resolution step
DOWNSAMPLE_EXPONENT = 16

class FASTAParser(Parser):

    def load_to_tile_db(self, seq_record, tileServerId, resolution_step, min_resolution):
        scale = len(seq_record) / min_resolution
        tile_count = math.ceil(math.log(scale) / math.log(resolution_step))

        resolutions = [resolution_step**i for i in range(0, tile_count + 1)]
        
        # create tiles for each resolution
        for resolution in resolutions:
            curr = tileServerId + "_" + str(resolution)

        return resolutions
        

    def parse(self):
        """ Parse the FASTA format using BioPython"""
        chrIdx = 0
        fname = self.filename
        info_node = {
            "_id": "IsequenceHomoSapienGRCh38",
            "type" : "sequence",
            "name": "Homo Sapien (GRCh38)",
            "source" : "RefSeq",
            "info": {}
        }
        chromosomes = []
        if os.path.splitext(self.filename)[1] == '.gz':
            filehandle = gzip.open(self.filename, 'rt')
        else:
            filehandle = open(self.filename)

        for seq_record in SeqIO.parse(filehandle, "fasta"):
            if (len(seq_record) > 20000000):
                if chrIdx == 22:
                    chrName = "chrX"
                if chrIdx == 23:
                    chrName = "chrY"
                else:
                    chrName = "chr" + str(chrIdx + 1)
                tileServerId = fname + "_" + str(chrIdx)
                resolutions = self.load_to_tile_db(seq_record, tileServerId, DOWNSAMPLE_EXPONENT, 10000)
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
        print(info_node)
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
