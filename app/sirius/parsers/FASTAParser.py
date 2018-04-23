#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GWAS, TILE_DB_PATH
from Bio import SeqIO
import math
import gzip
import os
import numpy as np
import tiledb
import collections

DOWNSAMPLE_RESOLUTIONS = [32, 128, 256, 1024, 16384, 65536, 131072]

class FASTAParser(Parser):
    def generate_downsampled_g_bands(tileServerId, step):
        """ Downsample the data by counting the percentage of GC content within a [step] sized interval
            See: https://en.wikipedia.org/wiki/G_banding
        """
        ctx = tiledb.Ctx()
        db = tiledb.DenseArray.load(ctx, tileServerId)
        sz = len(db)
        cnt = math.ceil(sz/float(step))
        newctx = tiledb.Ctx()
        d1 = tiledb.Dim(ctx, "locus", domain=(0, cnt - 1), tile=math.ceil(cnt/1000.0), dtype="uint64")
        domain = tiledb.Domain(ctx, d1)
        gcContent = tiledb.Attr(ctx, "gc", compressor=('lz4', -1), dtype='float32')
        tileDB_arr = tiledb.DenseArray(ctx, tileServerId + "_" + str(step),
                  domain=domain,
                  attrs=[gcContent],
                  cell_order='row-major',
                  tile_order='row-major')
        gcData = []
        for i in range(0, cnt):
            counts = collections.Counter(np.array(db[i * step : (i + 1) * step]['value']))
            # store the GC content
            percentage = float((counts[b'G'] + counts[b'C'] + counts[b'c'] + counts[b'g'])) / float(step)
            gcData.append(percentage)
        tileDB_arr[:] = np.array(gcData, dtype='float32')

    def load_to_tile_db(self, seq_record, tileServerId):
        """ Loads the sequence data into TileDB, generates downsampled tiles 
        """
        if not os.path.exists(TILE_DB_PATH):
            os.makedirs(TILE_DB_PATH)
        os.chdir(TILE_DB_PATH)
        ctx = tiledb.Ctx()
        d1 = tiledb.Dim(ctx, "locus", domain=(0, len(seq_record) - 1), tile=1000000, dtype="uint64")
        domain = tiledb.Domain(ctx, d1)
        base = tiledb.Attr(ctx, "value", compressor=('lz4', -1), dtype='S1')
        tileDB_arr = tiledb.DenseArray(ctx, tileServerId,
                  domain=domain,
                  attrs=[base],
                  cell_order='row-major',
                  tile_order='row-major')

        tileDB_arr[:] = np.array(seq_record.seq, 'S1')
        for resolution in DOWNSAMPLE_RESOLUTIONS:
            self.generate_downsampled_g_bands(tileServerId, resolution)
        return DOWNSAMPLE_RESOLUTIONS

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
                resolutions = self.load_to_tile_db(seq_record, tileServerId)
                chrInfo = {
                    "length" : len(seq_record),
                    "tileServerId": tileServerId,
                    "resolutions": resolutions,
                    "name": chrName,
                    "chrIdx": chrIdx
                }
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
