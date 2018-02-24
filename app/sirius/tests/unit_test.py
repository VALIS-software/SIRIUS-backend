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
        assert result[0] == 8
        print("Real annotation class test passed")

    def test_core_views(self):
        from sirius.core.views import get_real_annotation_data
        result = get_real_annotation_data('GRCh38', 11800, 14500, 100, 96)
        d = json.loads(result)
        assert d['startBp'] == 11800 and d['endBp'] == 14500
        assert d["values"][0]['entity']['location'] == 'Chr1'
        print("Real annotation request test passed")
    


if __name__ == "__main__":
    unittest.main()
