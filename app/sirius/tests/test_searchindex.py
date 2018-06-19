#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.searchindex import SearchIndex


class SearchIndexTest(TimedTestCase):
    def build_index(self):
        data = {}
        strings = []

        strings.append(['foo', 'bar'])
        strings.append(['foo', 'baz'])
        strings.append(['foo', 'baz', 'bar'])
        strings.append(['foo', 'foo'])
        strings.append(['baz', 'bar'])
        strings.append(['baz', 'baz'])
        strings.append(['bar', 'bar'])
        

        for x, string in enumerate(strings):
            data[x] = {'id': x, 'text': " ".join(string)}
        return SearchIndex(data, 'text')

    def test_basic_search(self):
        """ Test basic matching works """
        index = self.build_index()
        results = index.get_results('foo', enable_fuzzy=False)
        self.assertEqual(len(results), 4)

        results = index.get_results('bar', enable_fuzzy=False)
        self.assertEqual(len(results), 4)

        results = index.get_results('baz', enable_fuzzy=False)
        self.assertEqual(len(results), 4)

    def test_fuzzy_search(self):
        """ Test fuzzy matching works """
        index = self.build_index()
        results = index.get_results('bazz')
        self.assertEqual(len(results), 4)

        results = index.get_results('faz')
        self.assertEqual(len(results), 4)

    def test_tfidf_ranking(self):
        """ Test ordering of matching """
        index = self.build_index()
        results = index.get_results('baz')
        # the one with more mentions of 'baz' should be first
        self.assertEqual(results[0], 'baz baz')

        # the one with the most other words should be last
        self.assertEqual(results[-1], 'foo baz bar')

if __name__ == "__main__":
    unittest.main()
