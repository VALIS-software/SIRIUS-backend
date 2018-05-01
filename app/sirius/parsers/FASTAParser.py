#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GWAS, TILE_DB_PATH, TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS
from Bio import SeqIO
import math
import gzip
import os
import numpy as np
import tiledb
import time
import collections

class FASTAParser(Parser):
    def load_to_tile_db(self, seq_record, tileServerId):
        """ Loads the sequence data into TileDB, generates downsampled tiles 
        """
        start = time.time()
        if not os.path.exists(TILE_DB_PATH):
            os.makedirs(TILE_DB_PATH)
        os.chdir(TILE_DB_PATH)
        ctx = tiledb.Ctx()
        sz = len(seq_record)
        d1 = tiledb.Dim(ctx, "locus", domain=(0, sz - 1), tile=1000000, dtype="uint64")
        domain = tiledb.Domain(ctx, d1)
        base = tiledb.Attr(ctx, "value", compressor=('lz4', -1), dtype='S1')
        tileDB_arr = tiledb.DenseArray(ctx, tileServerId,
                  domain=domain,
                  attrs=[base],
                  cell_order='row-major',
                  tile_order='row-major')


        tileDB_arr[:] = np.array(seq_record.seq, 'S1')

        stride_lengths = map(lambda x : math.ceil(sz/float(x)), TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS)
        stride_data = {}
        stride_counts = {}
        
        for stride in stride_lengths:
            stride_data[stride] = []
            stride_counts[stride] = 0
        for i, char in enumerate(seq_record.seq):
            isgc = char == 'g' or char == 'G' or char == 'c' or char == 'C'
            for stride in stride_lengths:
                if (i + 1) % stride == 0:
                    stride_data[stride].append(float(stride_counts[stride]) / stride)
                    stride_counts[stride] = 0
                
                if isgc:
                    stride_counts[stride] += 1

        for resolution in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS:
            ctx = tiledb.Ctx()
            db = tiledb.DenseArray.load(ctx, tileServerId)
            sz = len(db)
            stride_length = math.ceil(sz/float(resolution))
            newctx = tiledb.Ctx()
            d1 = tiledb.Dim(ctx, "locus", domain=(0, stride_length - 1), tile=math.ceil(stride_length/1000.0), dtype="uint64")
            domain = tiledb.Domain(ctx, d1)
            gcContent = tiledb.Attr(ctx, "gc", compressor=('lz4', -1), dtype='float32')
            downsampled_arr = tiledb.DenseArray(ctx, tileServerId + "_" + str(resolution),
                      domain=domain,
                      attrs=[gcContent],
                      cell_order='row-major',
                      tile_order='row-major')
            print("Wrote downsampled array: % d" % resolution)
            downsampled_arr[:] = np.array(stride_counts[stride_length], dtype='float32')

        return TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS

    def parse(self, chromosome_limit=-1):
        """ Parse the raw sequence using BioPython, store data to tileDB and generate InfoNodes"""
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
        print("Parsing " + self.filename)
        if os.path.splitext(self.filename)[1] == '.gz':
            filehandle = gzip.open(self.filename, 'rt')
        else:
            filehandle = open(self.filename)

        for seq_record in SeqIO.parse(filehandle, "fasta"):
            print("Parsing contig " + seq_record.id)
            if (len(seq_record) > 20000000):
                if chrIdx == 22:
                    chrName = "chrX"
                if chrIdx == 23:
                    chrName = "chrY"
                else:
                    chrName = "chr" + str(chrIdx + 1)
                tileServerId = fname + "_" + str(chrIdx)
                resolutions = self.load_to_tile_db(seq_record, tileServerId)
                chrInfo = {
                    "length" : len(seq_record),
                    "tileServerId": tileServerId,
                    "resolutions": resolutions,
                    "name": chrName,
                    "chrIdx": chrIdx
                }
                print("Parsed chromosome")
                print(chrInfo)
                chromosomes.append(chrInfo)
                chrIdx += 1
                if chrIdx >= chromosome_limit and chromosome_limit > 0:
                    break
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
