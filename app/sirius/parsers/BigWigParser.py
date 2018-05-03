#!/usr/bin/env python

from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GWAS, TILE_DB_PATH, TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS
import pyBigWig
import math
import gzip
import os
import numpy as np
import tiledb
import time
import collections

class BigWigParser(Parser):
    def load_to_tile_db(self, bigwigRecord, chromosomeName, tileServerId):
        """ Loads the sequence data into TileDB, generates downsampled tiles 
        """
        start = time.time()
        if not os.path.exists(TILE_DB_PATH):
            os.makedirs(TILE_DB_PATH)
        os.chdir(TILE_DB_PATH)
        ctx = tiledb.Ctx()
        sz = bigwigRecord.chroms(chromosomeName)
        tile_size = 1000000
        d1 = tiledb.Dim(ctx, "locus", domain=(0, sz - 1), tile=tile_size, dtype="uint64")
        domain = tiledb.Domain(ctx, d1)
        signalValue = tiledb.Attr(ctx, "value", compressor=('lz4', -1), dtype='float32')
        mainBigWigDB = tiledb.DenseArray(ctx, tileServerId,
                  domain=domain,
                  attrs=[signalValue],
                  cell_order='row-major',
                  tile_order='row-major')

        tiles = int(math.ceil(float(sz) / tile_size))
        print("Starting writing %d tiles to TileDB" % len(tiles)) 
        # write basic signal data
        for i in range(0, tiles):
            start = i * tile_size
            end = min(sz - 1, (i + 1) * tile_size)
            values =  bigwigRecord.values(chromosomeName, start, end)
            mainBigWigDB[start:end] = np.array(values, 'float32')
        print("Finished writing data to TileDB")
        print("Generating summary statistic TileDB table (avg, min, max)")
        # create downsampled arrays:
        for stride in TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS:
            stride_length = math.ceil(sz/float(stride))
            d1 = tiledb.Dim(ctx, "locus", domain=(0, stride_length), tile=math.ceil(stride_length/1000.0), dtype="uint64")
            domain = tiledb.Domain(ctx, d1)
            vmin = tiledb.Attr(ctx, "min", compressor=('lz4', -1), dtype='float32')
            vmax = tiledb.Attr(ctx, "max", compressor=('lz4', -1), dtype='float32')
            vavg = tiledb.Attr(ctx, "avg", compressor=('lz4', -1), dtype='float32')
            # create a dense array for this stride resolution
            tiledb.DenseArray(ctx, tileServerId + "_" + str(stride),
                      domain=domain,
                      attrs=[vmin, vmax, vavg],
                      cell_order='row-major',
                      tile_order='row-major')
       
        stride_lengths = map(lambda x : math.ceil(sz/float(x)), TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS)
        stride_data = {}
        stride_sums = {}
        stride_mins = {}
        stride_maxes = {}
        
        for stride in TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS:
            stride_data[stride] = []
            stride_sums[stride] = 0
            stride_mins[stride] = None
            stride_maxes[stride] = None
        print("Computing summary statistics")
        for i in range(0, sz - 2):
            # read teh value back from tileDB
            value = mainBigWigDB[i : i+1]["value"][0]
            for stride in TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS:
                if (i + 1) % stride == 0:
                    vavg = float(stride_sums[stride]) / stride
                    vmin = stride_mins[stride]
                    vmax = stride_maxes[stride]
                    stride_data[stride].append([vmin, vmax, vavg])
                    stride_sums[stride] = 0
                    stride_mins[stride] = None
                    stride_maxes[stride] = None
                stride_sums[stride] += value
                if not stride_mins[stride]:
                    stride_mins[stride] = value

                if not stride_maxes[stride]:
                    stride_maxes[stride] = value
                stride_mins[stride] = min(stride_mins[stride], value)
                stride_maxes[stride] = max(stride_maxes[stride], value)

        print("Writing summary statistics to TileDB")
        os.chdir(TILE_DB_PATH)
        for stride in TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS:
            ctx = tiledb.Ctx()
            downsampled_arr = tiledb.DenseArray.load(ctx, tileServerId + "_" + str(stride))
            downsampled_arr[:] = np.array(stride_data[stride], dtype='float32')
            print("Wrote downsampled array: % d" % stride)
        return TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS

    def parse(self, chromosome_names=None):
        """ Parse the raw sequence using BioPython, store data to tileDB and generate InfoNodes"""
        chrIdx = 0
        fname = self.filename
        info_node = {
            "_id": "IsignalENCFF918ESR",
            "type" : "signal",
            "cellType": "heart",
            "assay": "DNase-seq",
            "name": "Homo Sapien ",
            "source": "ENCODE",
            "info": {}
        }
        chromosomes = []
        print("Parsing " + self.filename)

        bw = pyBigWig.open(self.filename)
        for chrom in bw.chroms():
            if not chromosome_names or chrom in chromosome_names:
                self.load_to_tile_db(bw, chrom, self.filename + "_" + chrom)
        self.info_nodes = [info_node]


    def get_mongo_nodes(self):
        """ Parse BigWig into InfoNodes for signal """
        #    {
        #      "_id": "IsignalXXXXXX",
        #      "type": "signal",
        #      "cellType": "heart",
        #      "assay": "DNase-seq",
        #      "name": "Homo Sapien ",
        #      "source": "ENCODE"
        #      'info': {
        #        'description': "Mus musculus C57BL/6 heart embryo (10.50 days)"
        #        'chromosomes': [
        #            {
        #               "length": 320803000,
        #               "tileServerId": "chr1",
        #               "name" : "chr1",
        #               "index" : 0,
        #            } 
        #            ...
        #            {
        #               "length": 5803000,
        #               "tileServerId": "chrX",
        #               "name" : "chrX",
        #               "index" : 22,
        #            } 
        #         ]
        #      }
        #    }
        if hasattr(self, 'mongonodes'): return self.mongonodes
        
        self.mongonodes = [], self.info_nodes, []
        return self.mongonodes