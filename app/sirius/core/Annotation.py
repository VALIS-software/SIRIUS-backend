import json
from sirius.realdata.constants import chromo_idxs, chromo_names
from sirius.mongo import GenomeNodes

class Annotation(object):

    def __init__(self, name=None, datadict=None):
        self.name = name
        self.length = 0
        if datadict != None:
            self.load_data(datadict)

    def __len__(self):
        return self.length

    def load_data(self, datadict):
        """ load data from a dictionary and do some pre-processing """
        self.start_bp = datadict['start_bp']
        self.end_bp = datadict['end_bp']
        self.length = self.end_bp - self.start_bp + 1
        self.chromo_lengths = datadict['chromo_lengths']
        self.seqids = datadict['seqids']
        self.chromo_end_bps = [self.chromo_lengths[0]] #np.cumsum(self.chromo_lengths) + self.start_bp - 1
        for l in self.chromo_lengths[1:]:
            self.chromo_end_bps.append(l+self.chromo_end_bps[-1])
        assert self.end_bp == self.chromo_end_bps[-1], 'Sum of chromo_lengths should be consistent with start_bp and end_bp'

    def json_data(self):
        return json.dumps({'annotationId': self.name, 'startBp': self.start_bp, 'endBp': self.end_bp})

    def find_bp_in_chromo(self, bp):
        """ Find the chromo that contains the bp.
        Inputs
        ------
        bp: int, input bp index

        Outputs
        _______
        chromo_id: int, index for the chromo
        bp_in_chromo: int, index for the bp relative to the start of the chromo
        *Return None if not found.
        """
        if bp >= self.start_bp and bp <= self.end_bp:
            bp_ch = bp
            for i_ch, end_bp in enumerate(self.chromo_end_bps):
                if end_bp >= bp:
                    return i_ch, bp_ch
                else:
                    bp_ch = bp - end_bp
        return None

    def db_find(self, start_bp, end_bp, types=None, min_length=0, verbose=False):
        """
        Find a GenomeNode in database. Example:
        {'_id': ObjectId('5a83f2f63dbebd0eb31b45dc'),
         'assembly': 'GRCh38',
         'end': 14409,
         'info': {'attributes': {'Dbxref': 'GeneID:100287102,HGNC:HGNC:37102',
           'ID': 'gene0',
           'Name': 'DDX11L1',
           'description': 'DEAD/H-box helicase 11 like 1',
           'gbkey': 'Gene',
           'gene': 'DDX11L1',
           'gene_biotype': 'misc_RNA',
           'pseudo': 'true'},
          'phase': '.',
          'score': '.',
          'seqid': 'NC_000001.11',
          'source': 'BestRefSeq',
          'strand': '+'},
         'length': 2536,
         'location': 'Chr1',
         'sourceurl': 'https://www.ncbi.nlm.nih.gov/projects/genome/guide/human/index.shtml',
         'start': 11874,
         'type': 'gene'}
        """
        start_i_ch, start_bp_ch = self.find_bp_in_chromo(start_bp)
        end_i_ch, end_bp_ch = self.find_bp_in_chromo(end_bp)
        if types == None: types = ['gene', 'transcript', 'exon','lnc_RNA', 'mRNA']
        query = {'assembly': self.name, 'type': {'$in': types}, 'length': {"$gt": min_length}}
        if start_i_ch == end_i_ch:
            # we use seqid to query the start and end positions
            seqid = self.seqids[start_i_ch]
            query['info.seqid'] = seqid
            query['start'] = {"$gt": start_bp_ch}
            query['end'] = {'$lt': end_bp_ch}
        else:
            start_seqid = self.seqids[start_i_ch]
            start_query = {'info.seqid':start_seqid, 'start': {'$gt': start_bp_ch}}
            end_seqid = self.seqids[end_i_ch]
            end_query = {'info.seqid':end_seqid, 'end': {"$lt": end_bp_ch}}
            query["$or"] = [start_query, end_query]
            if end_i_ch - start_i_ch > 1:
                mid_seqids = self.seqids[start_i_ch+1:end_i_ch]
                mid_query = {'info.seqid': {"$in": mid_seqids}}
                query["$or"] = [start_query, mid_query, end_query]
        if verbose: print(query)
        return GenomeNodes.find(query).sort([("seqid",1), ("start",1)])
