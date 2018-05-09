import os, time
import numpy as np
from Bio import SeqIO

from sirius.parsers.Parser import Parser
from sirius.helpers.tiledb import tilehelper
from sirius.helpers.constants import SEQ_CONTIG

class FASTAParser(Parser):
    def load_to_tile_db(self, seq_record, tileServerId):
        """ Loads the sequence data into TileDB, generates downsampled tiles
        """
        start = time.time()
        if not os.path.exists(TILE_DB_PATH):
            os.makedirs(TILE_DB_PATH)
        os.chdir(TILE_DB_PATH)

        sz = len(seq_record)
        stride_lengths = map(lambda x : math.ceil(sz/float(x)), TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS)
        stride_data = {}
        stride_counts = {}
        # TODO: pre-allocate numpy arrays
        # Compute GC content
        for stride in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS:
            stride_data[stride] = []
            stride_counts[stride] = 0
        for i, char in enumerate(seq_record.seq):
            isgc = char == 'g' or char == 'G' or char == 'c' or char == 'C'
            for stride in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS:
                if (i + 1) % stride == 0:
                    stride_data[stride].append(float(stride_counts[stride]) / stride)
                    stride_counts[stride] = 0
                if isgc:
                    stride_counts[stride] += 1

        # Write G-Band data
        for stride in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS:
            ctx = tiledb.Ctx()
            num_buckets = len(stride_data[stride])
            d1 = tiledb.Dim(ctx, "locus", domain=(0, num_buckets - 1), tile=math.ceil(num_buckets/1000.0), dtype="uint64")
            domain = tiledb.Domain(ctx, d1)
            gcContent = tiledb.Attr(ctx, "gc", compressor=('lz4', -1), dtype='float32')
            downsampled_arr = tiledb.DenseArray(ctx, tileServerId + "_" + str(stride),
                      domain=domain,
                      attrs=[gcContent],
                      cell_order='row-major',
                      tile_order='row-major')
            downsampled_arr[:] = np.array(stride_data[stride], dtype='float32')
            print("Wrote downsampled array: % d" % stride)

        # Write source sequence data
        ctx = tiledb.Ctx()
        d1 = tiledb.Dim(ctx, "locus", domain=(0, sz - 1), tile=1000000, dtype="uint64")
        domain = tiledb.Domain(ctx, d1)
        base = tiledb.Attr(ctx, "value", compressor=('lz4', -1), dtype='S1')

        tileDB_arr = tiledb.DenseArray(ctx, tileServerId,
                  domain=domain,
                  attrs=[base],
                  cell_order='row-major',
                  tile_order='row-major')
        print("Writing final array")
        tileDB_arr[:] = np.array(seq_record.seq, 'S1')
        return TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS

    def __init__(self, filename, verbose=False):
        """ Initializer of FASTERParser class """
        super(FASTAParser, self).__init__(filename, verbose)
        self.SeqIOhandle = SeqIO.parse(self.filehandle, 'fasta')

    def parse(self):
        """ Parse the raw sequence using BioPython """
        self.sequences = []
        while self.parse_one_seq():
            pass

    def parse_one_seq(self):
        """ Parse only one sequence at a time """
        try:
            seq_record = next(self.SeqIOhandle)
        except StopIteration:
            return False
        if not hasattr(self, 'sequences'):
            self.sequences = []
        name = seq_record.name
        contig = SEQ_CONTIG.get(name, name)
        # encode the sequence into integers
        t0 = time.time()
        datastr = np.array(list(str(seq_record.seq.lower())))
        t1 = time.time()
        if self.verbose:
            print(f"Loaded sequence {name} contig {contig} size {len(datastr)}; {t1-t0:.2f} s")
        d = np.zeros_like(datastr, dtype=np.int8)
        #d[datastr=='n'] = 0
        d[datastr=='a'] = 1
        d[datastr=='t'] = 2
        d[datastr=='c'] = 3
        d[datastr=='g'] = 4
        # all other characters will be left as 0
        t2 = time.time()
        if self.verbose:
            print(f"Convert sequence into integers; {t2-t1:.2f} s")
        stored_data = self.load_to_tiledb(contig, d)
        self.sequences.append({
            'contig': contig,
            'length': len(d),
            'stored_data': stored_data
        })
        return True

    def load_to_tiledb(self, contig, data):
        stored_data = []
        t0 = time.time()
        # load the raw sequence data to tiledb
        arrayID = f'fasta_sequence_{contig}'
        tilehelper.create_dense_array(arrayID, data)
        stored_data.append({
            'resolution': 1,
            'length': len(data),
            'type': 'atcg',
            'tiledbID': arrayID
        })
        t1 = time.time()
        if self.verbose:
            print(f"Wrote {len(data)} sequence ATCG to tiledb; {t1-t0:.2f} s")
        # down sample the sequence data to gbands and store them
        TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS = [32, 128, 256, 1024, 16384, 65536, 131072]
        gbands = {}
        if self.verbose:
            print("gbands down-sampling started")
        for stride in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS:
            t_start = time.time()
            avail_previous_strides = [s for s in gbands if stride % s == 0]
            if len(avail_previous_strides) > 0:
                best_prev_stride = max(avail_previous_strides)
                width = int(stride / best_prev_stride)
                prev_gband = gbands[best_prev_stride]
                n_bins = int(len(prev_gband) / width)
                if n_bins < 100: break
                fit_size = n_bins * width
                gbands[stride] = prev_gband[:fit_size].reshape(n_bins, width).mean(axis=-1).astype(np.float32)
            else:
                # do the downsampling from raw sequence to gbands
                n_bins = int(len(data) / stride)
                if n_bins < 100: break
                fit_size = n_bins * stride
                data = data[:fit_size]
                # the ratio of 'c' and 'g' in stride
                gbands[stride] = (data > 2).reshape(n_bins, stride).mean(axis=-1).astype(np.float32)
                best_prev_stride = 1
            if self.verbose:
                t = time.time() - t_start
                print(f"stride = {stride:7d} | nBins = {n_bins:7d} | fromStride {best_prev_stride:7d} | {t:.2f} seconds")
        t2 = time.time()
        if self.verbose:
            print(f"Down-sampling finished; {t2-t1:.2f} s")
        # write the down sampled data to tiledb
        for stride, gband_data in gbands.items():
            arrayID = f'fasta_gband_{contig}_{stride}'
            tilehelper.create_dense_array(arrayID, gband_data)
            stored_data.append({
                'resolution': stride,
                'length': len(gband_data),
                'type': 'gbands',
                'tiledbID': arrayID
            })
        t3 = time.time()
        if self.verbose:
            print(f"Wrote gbands down-sampled to tiledb; {t3-t2:.2f} s")
        return stored_data

    def get_mongo_nodes(self):
        """ Parse FASTA into InfoNodes for sequence """
        genome_nodes, info_nodes, edges = [], [], []
        info_node = self.metadata.copy()
        info_node.update({
            "_id": "IsequenceHomoSapienGRCh38",
            "type" : "sequence",
            "name": "Homo Sapien (GRCh38)",
            "source" : "RefSeq",
            "info": {
                'contig_info': {}
            }
        })
        for seq_info in self.sequences:
            contig = seq_info['contig']
            info_node['info']['contig_info'][contig] = seq_info
        info_nodes.append(info_node)
        return genome_nodes, info_nodes, edges
