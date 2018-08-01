#!/usr/bin/env python

import unittest
from sirius.tests.timed_test_case import TimedTestCase
from sirius.helpers.loaddata import loaded_track_types_info, loaded_contig_info, loaded_data_tracks


class HelpersTest(TimedTestCase):
    def test_helpers_constants(self):
        """ Test import helpers.constants """
        from sirius.helpers import constants

    def test_track_types_info(self):
        """ Test helpers.loaddata.load_mongo_data_information() """
        for info in loaded_track_types_info:
            assert all((key in info) for key in ('track_type', 'title', 'description'))

    def test_contig_info(self):
        """ Test helpers.loaddata.loaded_track_info """
        for info in loaded_contig_info:
            assert all((key in info) for key in ('name', 'start', 'length'))

    def test_data_track_info(self):
        """ Test helpers.loaddata.load_data_track_information """
        for info in loaded_data_tracks:
            assert info['type'] in ('sequence', 'signal')

    def test_tiledb_helper(self):
        """ Test helpers.tiledb """
        from sirius.helpers.tiledb import tilehelper
        l = tilehelper.ls()

if __name__ == "__main__":
    unittest.main()
