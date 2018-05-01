#!/usr/bin/env python

import unittest, json
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.QueryTree import QueryTree
from sirius.core.views import get_annotation_query

class CoreTest(TimedTestCase):

    def test_QueryTree(self):
        """ Test QueryTree.find() """
        dfilter = {'type': 'InfoNode', 'filters': {'$text': "cancer"}}
        qt = QueryTree(dfilter)
        n_result = qt.find().count()
        self.assertGreater(n_result, 200, 'Query cancer traits should return more than 200 InfoNodes')

    def test_core_views(self):
        """ Test views.get_annotation_query() """
        query = {'type':'GenomeNode', "filters":{"chromid":1, "type":'gene'}}
        result = get_annotation_query('GRCh38', 0, 1000000, 100000, 96, query, verbose=False)
        d = json.loads(result)
        self.assertGreater(d['countInRange'], 50, 'Number of genes in Chr1 1-1M should be greater than 50')

if __name__ == "__main__":
    unittest.main()
