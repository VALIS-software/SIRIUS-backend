#!/usr/bin/env python

from sirius.core.QueryTree import QueryTree
from pybedtools import BedTool, Interval
import os, shutil, tempfile

def write_tmp_bed(iterable):
    """ Write an iterable to a temporary bed file, return the filename """
    filename = tempfile.mkstemp(suffix='.bed', prefix='sirius_')[1]
    with open(filename, 'w') as outfile:
        for iv in iterable:
            outfile.write('\t'.join(map(str, iv)) + '\n')
    return filename

def get_inverval(d):
    """ Convert a gnode dictionary to an interval tuple """
    score = '.'
    strand = '.'
    try:
        score = d['info']['score']
    except KeyError:
        pass
    try:
        strand = d['info']['strand']
    except KeyError:
        pass
    return (d['chromid'], d['start'], d['end'], d['name'], score, strand, d['_id'], d['type'])

class Bed(object):
    def __init__(self, fn=None):
        self.istmp = False # flag, set to true to delete the bed file during destruction
        if fn == None:
            self.bedtool = BedTool()
        elif isinstance(fn, BedTool):
            self.bedtool = fn
        elif isinstance(fn, str):
            self.bedtool = BedTool(fn)
        elif isinstance(fn, dict) or isinstance(fn, QueryTree):
            # if we have a query
            qt = QueryTree(fn) if isinstance(fn, dict) else fn
            cursor = qt.find(projection=['_id', 'chromid', 'start', 'end', 'name', 'type', 'info.score', 'info.strand'])
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

    def gnodes(self):
        """ Generator to yield gnodes one by one from self.bedtool """
        for iv in self.bedtool:
            chrom, start, end, name, score, strand, _id, gtype = iv.fields[:8]
            gnode = {
                '_id': _id,
                'chromid': int(chrom),
                'start': start,
                'end': end,
                'type': gtype,
                'name': name
            }
            yield gnode

    def intersect(self, b, *args, **kwargs):
        c = Bed()
        c.bedtool = self.bedtool.intersect(b.bedtool, *args, **kwargs)
        c.istmp = True
        return c

    def window(self, b, *args, **kwargs):
        c = Bed()
        c.bedtool = self.bedtool.window(b.bedtool, *args, **kwargs)
        c.istmp = True
        return c
