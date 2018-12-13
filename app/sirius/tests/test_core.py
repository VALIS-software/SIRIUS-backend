#!/usr/bin/env python

import unittest
import json
from sirius.tests.timed_test_case import TimedTestCase
from sirius.query.query_tree import QueryTree
from sirius.core.annotationtrack import get_annotation_query

class CoreTest(TimedTestCase):

    def test_QueryTree(self):
        """ Test QueryTree.find() """
        dfilter = {'type': 'InfoNode', 'filters': {'$text': "cancer"}}
        qt = QueryTree(dfilter)
        n_result = len(list(qt.find()))
        self.assertGreater(n_result, 10, 'Query cancer traits should return more than 10 InfoNodes')

    def test_core_views(self):
        """ Test core.annotationtrack.get_annotation_query() """
        query = {'type':'GenomeNode', "filters":{"type":'gene'}}
        result = get_annotation_query('aid', 'chr1', 1, 1000000, 100000, 96, query, verbose=False)
        d = json.loads(result)
        self.assertGreater(d['countInRange'], 8, 'Number of genes in Chr1 1-1M should be greater than 8')

    def test_import_auth(self):
        """ Test import core.auth0 module """
        from sirius.core import auth0

if __name__ == "__main__":
    unittest.main()
