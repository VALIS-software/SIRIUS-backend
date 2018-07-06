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
        results = self.search_index.get_suggestions('foo')
        self.assertEqual(len(results), 4)

        results = self.search_index.get_suggestions('bar')
        self.assertEqual(len(results), 6)

        results = self.search_index.get_suggestions('baz')
        self.assertEqual(len(results), 6)

    def test_tfidf_ranking(self):
        """ Test ordering of matching """
        results = self.search_index.get_suggestions('baz')
        # the one with more mentions of 'baz' should be first
        self.assertEqual(results[0], 'baz baz')
        # the one with the most other words should be last
        self.assertEqual(results[-1], 'foo bar')

if __name__ == "__main__":
    unittest.main()
