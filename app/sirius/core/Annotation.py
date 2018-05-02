import json
from sirius.realdata.constants import CHROMO_IDXS
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
        self.chromo_end_bps = [0]
        for l in self.chromo_lengths:
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
        """ Find a GenomeNode in database """
        start_i_ch, start_bp_ch = self.find_bp_in_chromo(start_bp)
        end_i_ch, end_bp_ch = self.find_bp_in_chromo(end_bp)
        if types == None: types = ['gene', 'transcript', 'exon','lnc_RNA', 'mRNA']
        query = {'assembly': self.name, 'type': {'$in': types}, 'length': {"$gte": min_length}}
        if start_i_ch == end_i_ch:
            # we use seqid to query the start and end positions
            query['chromid'] = start_i_ch
            query['start'] = {"$gte": start_bp_ch}
            query['end'] = {'$lte': end_bp_ch}
        else:
            start_query = {'chromid':start_i_ch, 'start': {'$gte': start_bp_ch}}
            end_seqid = self.seqids[end_i_ch]
            end_query = {'chromid':end_i_ch, 'end': {'$lte': end_bp_ch}}
            query["$or"] = [start_query, end_query]
            if end_i_ch - start_i_ch > 1:
                mid_chromids = list(range(start_query+1, end_query))
                mid_query = {'chromid': {"$in": mid_chromids}}
                query["$or"] = [start_query, mid_query, end_query]
        if verbose: print(query)
        return GenomeNodes.find(query).sort([("chromid",1), ("start",1)])

    def location_to_bp(self, chromid, bp_in_ch):
        if isinstance(chromid, str):
            chromid = CHROMO_IDXS[chromid]
        prev_end = self.chromo_end_bps[chromid-1] if chromid > 0 else 0
        return prev_end + bp_in_ch
