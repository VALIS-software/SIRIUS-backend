import os, time
import numpy as np
from Bio import SeqIO

from sirius.parsers.Parser import Parser
from sirius.helpers.tiledb import tilehelper
from sirius.helpers.constants import SEQ_CONTIG, DATA_SOURCE_FASTA

class FASTAParser(Parser):

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
        # get the contig id for this sequence
        contig = SEQ_CONTIG.get(name, name)
        # encode the sequence into integers
        t0 = time.time()
        datastr = np.array(list(str(seq_record.seq.lower())))
        t1 = time.time()
        if self.verbose:
            print(f"Loaded sequence {name} contig {contig} size {len(datastr)}; {t1-t0:.2f} s")
        d = np.zeros_like(datastr, dtype=np.int8)
        d[datastr=='a'] = 1
        d[datastr=='t'] = 2
        d[datastr=='g'] = 3
        d[datastr=='c'] = 4
        d[datastr=='n'] = 5
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
            'type': 'atgc',
            'tiledbID': arrayID
        })
        t1 = time.time()
        if self.verbose:
            print(f"Wrote {len(data)} sequence ATGC to tiledb; {t1-t0:.2f} s")
        # down sample the sequence data to distributions and store them
        TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS = [4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576]
        # create the first distribution matrix (4xN)
        stride = TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS[0]
        n_bins = int(len(data) / stride)
        fit_size = n_bins * stride
        m_distri = np.zeros([n_bins,4], dtype=np.float32)
        fit_data = data[:fit_size].reshape(n_bins, stride)
        m_distri[:,0] = np.mean(fit_data == 1, axis=-1) # 'a'
        m_distri[:,1] = np.mean(fit_data == 2, axis=-1) # 't'
        m_distri[:,2] = np.mean(fit_data == 3, axis=-1) # 'g'
        m_distri[:,3] = np.mean(fit_data == 4, axis=-1) # 'c'
        m_distri += (np.mean(fit_data == 5, axis=-1) * 0.25)[:, np.newaxis] # 'n'
        t2 = time.time()
        if self.verbose:
            print(f"stride {stride:<8d} | nBins {n_bins:<9d} | fromStride {1:<8d} | {t2-t1:.2f} s")
        distri_mats = {stride: m_distri}
        # Note, here we require all the rest of strides to be at least multiples of the first stride
        for stride in TILE_DB_FASTA_DOWNSAMPLE_RESOLUTIONS[1:]:
            t_start = time.time()
            best_prev_stride = max(s for s in distri_mats if stride % s == 0)
            width = int(stride / best_prev_stride)
            prev_m = distri_mats[best_prev_stride]
            n_bins = int(prev_m.shape[0] / width)
            fit_size = n_bins * width
            distri_mats[stride] = prev_m[:fit_size].reshape(n_bins, width, 4).mean(axis=1)
            if self.verbose:
                t = time.time() - t_start
                print(f"stride {stride:<8d} | nBins {n_bins:<9d} | fromStride {best_prev_stride:<8d} | {t:.2f} s")
            # stop the down-sampling if the sample size is already small enough
            if n_bins < 100: break
        t3 = time.time()
        if self.verbose:
            print(f"Down-sampling finished; {t3-t2:.2f} s")
        # write the down sampled data to tiledb
        for stride, mat in distri_mats.items():
            arrayID = f'fasta_distri_mat_{contig}_{stride}'
            tilehelper.create_dense_array(arrayID, mat)
            stored_data.append({
                'resolution': stride,
                'length': len(mat),
                'type': 'distribution matrix',
                'tiledbID': arrayID
            })
        t4 = time.time()
        if self.verbose:
            print(f"Wrote distribution matrices down-sampled to tiledb; {t4-t3:.2f} s")
        return stored_data

    def get_mongo_nodes(self):
        """ Parse FASTA into InfoNodes for sequence """
        genome_nodes, info_nodes, edges = [], [], []
        # add the dataSource
        info_node = {"_id": 'I'+DATA_SOURCE_FASTA, "type": "dataSource", "name": DATA_SOURCE_FASTA, "source": DATA_SOURCE_FASTA}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        # add the sequence info
        info_node = {
            "_id": "Isequence",
            "type" : "sequence",
            "name": "Homo Sapien (GRCh38)",
            "source" : DATA_SOURCE_FASTA,
            "info": {
                'contigs': self.sequences
            }
        }
        info_nodes.append(info_node)
        return genome_nodes, info_nodes, edges
