#!/usr/bin/env python
import json
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.mockData.mock_util import getMockData, get_mock_track_data

class MockDataTest(TimedTestCase):
    def test_get_mock_data(self):
        """ Test mockData.mock_util.getMockData() """
        MOCK_DATA = getMockData()
        self.assertIn('sequence', MOCK_DATA)

    def test_get_mock_track_data(self):
        """ Test mockData.mock_util.get_mock_track_data() """
        track_data = get_mock_track_data('sequence', 1, 1000, 'basepairs', 64, 1, ['max'])
        track_data = json.loads(track_data)
        self.assertEqual(len(track_data['values']), 999)

if __name__ == "__main__":
    unittest.main()
