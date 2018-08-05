#!/usr/bin/env python

import unittest
import json
from sirius.tests.timed_test_case import TimedTestCase
from sirius.analysis.bed import Bed

class AnalysisBedTest(TimedTestCase):
    gnodes = [
        {'_id': 'gid_1_0_100', 'contig': 'chr1', 'start': 1, 'end': 100, 'info':{}},
        {'_id': 'gid_2_2_200', 'contig': 'chr1', 'start': 3, 'end': 200, 'info':{}},
        {'_id': 'gid_2_1_1000', 'contig': 'chr2', 'start': 2, 'end': 1000, 'info':{}},
    ]
    intervals1 = [
        ('chr1', 0, 100, 'gid_1_0_100', '.', '.'),
        ('chr1', 2, 200, 'gid_2_2_200', '.', '.'),
        ('chr2', 1, 1000, 'gid_2_1_1000', '.', '.'),
    ]
    intervals2 = [
        ('chr1', 99, 200, 'gid_1_99_200', '.', '.'),
        ('chr1', 250, 300, 'gid_1_200_300', '.', '.'),
        ('chr2', 500, 1000, 'gid_2_500_1000', '.', '.'),
    ]
    def test_bed_init_len(self):
        """ Test Bed.__init__() and Bed.__len__()"""
        # create empty bed
        bed = Bed()
        self.assertEqual(len(bed), 0)
        # create a Bed with gnodes
        bed = Bed(self.gnodes)
        self.assertEqual(len(bed), 3)
        self.assertEqual(bed.istmp, True)
        # create a Bed with interval
        bed1 = Bed(self.intervals1)
        self.assertEqual(len(bed1), 3)
        self.assertEqual(bed1.istmp, True)
        # create with filename
        bed1 = Bed(bed.fn)
        self.assertEqual(bed1.fn, bed.fn)
        self.assertEqual(len(bed1), 3)
        # create with BedTool
        bed1 = Bed(bed.bedtool)
        self.assertEqual(bed1.fn, bed.fn)
        self.assertEqual(len(bed1), 3)

    def test_bed_equal(self):
        """ Test Bed.__eq__() """
        bed = Bed(self.gnodes)
        bed1 = Bed(self.intervals1)
        self.assertEqual(bed, bed1)

    def test_bed_gids(self):
        """ Test Bed.gids() """
        bed = Bed(self.gnodes)
        ref_gids = set(d['_id'] for d in self.gnodes)
        self.assertEqual(bed.gids(), ref_gids)

    def test_bed_intersect(self):
        """ Test Bed.intersect() """
        bed1 = Bed(self.intervals1)
        bed2 = Bed(self.intervals2)
        bed3 = bed1.intersect(bed2)
        self.assertEqual(bed1.gids(), bed3.gids())
        bed4 = bed2.intersect(bed1)
        ref_gids = {self.intervals2[0][3], self.intervals2[2][3]}
        self.assertEqual(bed4.gids(), ref_gids)

    def test_bed_window(self):
        """ Test Bed.window() """
        bed1 = Bed(self.intervals1)
        bed2 = Bed(self.intervals2)
        bed3 = bed1.window(bed2, 10)
        self.assertEqual(bed1.gids(), bed3.gids())
        bed4 = bed2.window(bed1, 50)
        ref_gids = {self.intervals2[0][3], self.intervals2[2][3]}
        self.assertEqual(bed4.gids(), ref_gids)
        bed5 = bed2.window(bed1, 51)
        self.assertEqual(bed5.gids(), bed2.gids())


if __name__ == "__main__":
    unittest.main()
