#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.searchindex import SearchIndex


class SearchIndexTest(TimedTestCase):
    def setUp(self):
        super(SearchIndexTest, self).setUp()
        strings = ['foo bar','foo baz','foo baz bar','foo foo','baz bar','baz baz','bar bar']
        self.search_index = SearchIndex(strings)

    def test_search(self):
        """ Test matching works """
        results = self.search_index.get_suggestions('foo', 4)
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0], 'foo foo')
        self.assertEqual(results[3], 'foo baz bar')

        results = self.search_index.get_suggestions('bar', 4)
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0], 'bar bar')
        self.assertEqual(results[3], 'foo baz bar')

        results = self.search_index.get_suggestions('baz', 4)
        self.assertEqual(len(results), 4)

    def test_tfidf_ranking(self):
        """ Test ordering of matching """
        results = self.search_index.get_suggestions('baz')
        # the one with more mentions of 'baz' should be first
        self.assertEqual(results[0], 'baz baz')
        # the one with the most other words should be last
        self.assertEqual(results[-1], 'foo baz bar')

if __name__ == "__main__":
    unittest.main()
