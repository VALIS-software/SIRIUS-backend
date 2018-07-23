import os, shutil, tempfile
import sirius.query.QueryTree
import sirius.query.GenomeQueryNode
from sirius.mongo import GenomeNodes
from sirius.mongo.utils import doc_generator
from pybedtools import BedTool, Interval

# This will let both pybedtools and our Bed class to use this temp folder
tempfile.tempdir = os.environ.get('SIRIUS_TEMP_DIR', None)

def write_tmp_bed(iterable):
    """ Write an iterable to a temporary bed file, return the filename """
    filename = tempfile.mkstemp(suffix='.bed', prefix='sirius_')[1]
    with open(filename, 'w') as outfile:
        for iv in iterable:
            outfile.write('\t'.join(map(str, iv)) + '\n')
    return filename

def get_inverval(d):
    """ Convert a gnode dictionary to an interval tuple """
    score = d['info'].get('score', '.')
    strand = d['info'].get('strand', '.')
    return (d['contig'], d['start'], d['end'], d['_id'], score, strand)

class Bed(object):
    def __init__(self, fn=None):
        self.istmp = False # flag, set to true to delete the bed file during destruction
        if fn == None:
            self.bedtool = BedTool()
        elif isinstance(fn, BedTool):
            self.bedtool = fn
        elif isinstance(fn, str):
            self.bedtool = BedTool(fn)
        elif isinstance(fn, dict) or isinstance(fn, sirius.query.QueryTree.QueryTree) or isinstance(fn, sirius.query.GenomeQueryNode.GenomeQueryNode):
            # if we have a query
            qt = sirius.query.QueryTree.QueryTree(fn) if isinstance(fn, dict) else fn
            cursor = qt.find(projection=['_id', 'contig', 'start', 'end', 'info.score', 'info.strand'])
            iv_iter = (get_inverval(d) for d in cursor)
            tmpfn = write_tmp_bed(iv_iter)
            self.bedtool = BedTool(tmpfn)
            self.istmp = True
        elif isinstance(fn, list) or isinstance(fn, tuple):
            tmpfn = write_tmp_bed(fn)
            self.bedtool = BedTool(tmpfn)
            self.istmp = True
        else:
            raise NotImplementedError(f'Initializing with type {type(fn)} is not implemented')

    @property
    def fn(self):
        return self.bedtool.fn

    def __len__(self):
        with open(self.fn) as fn:
            length = sum(1 for line in fn)
        return length

    def __str__(self):
        return self.bedtool.__str__()

    def __getitem__(self, key):
        return self.bedtool.__getitem__(key)

    def __iter__(self):
        return self.bedtool.__iter__()

    def _delete_tmp(self):
        if self.istmp == True:
            try:
                os.unlink(self.fn)
            except FileNotFoundError:
                pass

    def __del__(self):
        self._delete_tmp()
        del self.bedtool

    def load_from_ids(self, ids):
        projection=['_id', 'contig', 'start', 'end', 'info.score', 'info.strand']
        gen = doc_generator(GenomeNodes, ids, projection=projection)
        iv_iter = (get_inverval(d) for d in gen)
        tmpfn = write_tmp_bed(iv_iter)
        print(f'data written to bed file {tmpfn}')
        self.bedtool = BedTool(tmpfn)
        self.istmp = True

    def head(self):
        self.bedtool.head()

    def copy(self):
        newfn = tempfile.mkstemp(suffix='.bed', prefix='sirius_')[1]
        shutil.copyfile(self.fn, newfn)
        newBed = Bed(fn)
        newBed.istmp = True
        return newBed

    def extend(self, d):
        self.extend_asym(d, d)

    def extend_asym(self, dl, dr):
        """ Extend the range of each feature, by dl to left and dr to right """
        # create a new file with extended range
        def gen_iv_ext():
            # we use a generator to
            for iv in self.bedtool:
                chrom, start, end, name, score, strand, _id, tp = iv.fields[:8]
                yield (chrom, int(start)-dl, int(end)+dr, name, score, strand, _id, tp)
        newfn = write_tmp_bed(gen_iv_ext())
        # delete the old tmp file
        self._delete_tmp()
        # switch to the new file
        self.bedtool = BedTool(newfn)
        # we are now using a tmp file
        self.istmp = True

    def gids(self):
        """ Return a set of gnode ids from self.bedtool """
        return set(iv[3] for iv in self.bedtool)

    def intersect(self, b, *args, **kwargs):
        c = Bed()
        c.bedtool = self.bedtool.intersect(b.bedtool, *args, **kwargs)
        c.istmp = True
        return c

    def window(self, b, window=1000):
        c = Bed()
        c.bedtool = self.bedtool.window(b.bedtool, w=window, u=True)
        c.istmp = True
        return c
