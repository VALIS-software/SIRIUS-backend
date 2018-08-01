import os
import shutil
import tempfile
import collections
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
        elif isinstance(fn, collections.Iterable):
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

    def head(self):
        self.bedtool.head()

    def copy(self):
        newfn = tempfile.mkstemp(suffix='.bed', prefix='sirius_')[1]
        shutil.copyfile(self.fn, newfn)
        newBed = Bed(newfn)
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
