#!/usr/bin/env python
import json
import unittest

class TestSirius(unittest.TestCase):

    def test_mock_Annotation(self):
        from sirius.mockData.mock_util import get_mock_annotation_data
        annotation_ids = 'cross-track-test-1'
        result = get_mock_annotation_data(annotation_ids, 0, 11231214, 100000, 22)
        print("Mock annotation request test passed")

    def test_Annotation(self):
        from sirius.realdata.loaddata import loaded_annotations
        anno = loaded_annotations['GRCh38']
        result = anno.find_bp_in_chromo(len(anno)/2)
        self.assertEqual(result[0], 9)
        print("Real annotation class test passed")

    def test_core_views(self):
        from sirius.core.views import get_annotation_query
        # test the query api
        query = {'type': 'GenomeNode', "filters":{"chromid": 1, "type":'gene'}}
        result = get_annotation_query('GRCh38', 0, 1000000, 100000, 96, query)
        d = json.loads(result)
        assert d['countInRange'] > 50
        print("Real annotation request test passed")


    def test_query_filter(self):
        from sirius.core.QueryTree import QueryTree
        dfilter = {'type': 'InfoNode', 'name': {"$contain": "cancer", 'info.pvalue': {"<": 0.1}}}
        print(dfilter)
        qt = QueryTree(dfilter)
        print(qt.find().count())
        print("query build and search test passed")



if __name__ == "__main__":
    unittest.main()
