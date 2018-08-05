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
        for d in iterable:
            if isinstance(d, dict):
                outfile.write('\t'.join(map(str, get_interval(d))) + '\n')
            else:
                outfile.write('\t'.join(map(str, d)) + '\n')
    return filename

def get_interval(d):
    """ Convert a gnode dictionary to an interval tuple """
    score = d['info'].get('score', '.')
    strand = d['info'].get('strand', '.')
    return (d['contig'], d['start']-1, d['end'], d['_id'], score, strand)

class Bed:
    """ The Bed class is a wrapper to the pybedtools.BedTool class.
    It handles the temporary file internally, and provides interface to several BedTool methods.

    Attributes
    ----------
    fn: string
        The temporary filename sitting on disk that holds contents of this Bed() object.

    istmp: bool
        Flag for whether this Bed() object is temporary. If temporary, self.fn file will be deleted upon destruction.

    __len__: int
        Number of intervals contained in this Bed() object.

    """
    def __init__(self, fn=None):
        """ Initializer of Bed class.

        Parameters
        ----------
        fn: None, BedTool, string, tuple, list, generator
        None: Empty Bed() object
        BedTool: wraps input existing BedTool object.
        string: filename, wraps the input file with name {fn}.
        list/tuple/generator: loops over the input fn, writing as an temporary file, wraps it.
                            Init with this type will automatically set self.istmp=True
        """
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
            self.bedtool = None
            raise NotImplementedError(f'Initializing with type {type(fn)} is not implemented')

    @property
    def fn(self):
        return self.bedtool.fn

    def __len__(self):
        if not self.fn:
            return 0
        with open(self.fn) as fn:
            length = sum(1 for line in fn)
        return length

    def __str__(self):
        return self.bedtool.__str__()

    def __getitem__(self, key):
        return self.bedtool.__getitem__(key)

    def __iter__(self):
        return self.bedtool.__iter__()

    def __eq__(self, target):
        if len(self) != len(target):
            return False
        elif len(self) == len(target) == 0:
            return True
        else:
            return self.bedtool == target.bedtool

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
        """ Wrapper for BelTool.head() method, which prints the first few intervals. """
        self.bedtool.head()

    def copy(self):
        """ Create a temporary copy of the Bed() object. """
        newfn = tempfile.mkstemp(suffix='.bed', prefix='sirius_')[1]
        shutil.copyfile(self.fn, newfn)
        newBed = Bed(newfn)
        newBed.istmp = True
        return newBed

    def extend(self, d):
        """ Modify the intervals by extending each to left and right by {d} """
        self.extend_asym(d, d)

    def extend_asym(self, dl, dr):
        """ Extend the range of each interval, by dl to left and dr to right """
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

    def intersect(self, b):
        """ intersect method wrapps BedTool.intersect()

        Parameters
        ----------
        b: Bed
            Target Bed() object to be intersected with

        Returns
        -------
        c: Bed
            Temporary Bed() object that contains all intervals in {self} which intersects with {b}.
        """
        c = Bed()
        c.bedtool = self.bedtool.intersect(b.bedtool, u=True)
        c.istmp = True
        return c

    def window(self, b, window=1000):
        """ window method wrapps BedTool.window()

        Parameters
        ----------
        b: Bed
            Target Bed() object to be windowed with

        window: int, default 1000
            The window size

        Returns
        -------
        c: Bed
            Temporary Bed() object that contains all intervals in {self} which is within {window} from any interval in {b}.
        """
        c = Bed()
        c.bedtool = self.bedtool.window(b.bedtool, w=window, u=True)
        c.istmp = True
        return c
