#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.helpers.loaddata import load_mongo_data_information, load_contig_information, load_data_track_information


class HelpersTest(TimedTestCase):
    def test_helpers_constants(self):
        """ Test import helpers.constants """
        from sirius.helpers import constants

    def test_track_types_info(self):
        """ Test helpers.loaddata.load_mongo_data_information() """
        for info in load_mongo_data_information():
            assert all((key in info) for key in ('track_type', 'title', 'description'))

    def test_contig_info(self):
        """ Test helpers.loaddata.loaded_track_info """
        for info in load_contig_information():
            assert all((key in info) for key in ('name', 'length', 'chromosome'))

    def test_data_track_info(self):
        """ Test helpers.loaddata.load_data_track_information """
        for info in load_data_track_information():
            assert info['type'] in ('sequence', 'signal')

if __name__ == "__main__":
    unittest.main()
