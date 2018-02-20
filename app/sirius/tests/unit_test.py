#!/usr/bin/env python
import json
from sirius.realdata.loaddata import loaded_annotations
import unittest

class TestSirius(unittest.TestCase):

    def test_Annotation(self):
        anno = loaded_annotations['GRCh38']
        result = anno.find_bp_in_chromo(len(anno)/2)
        assert result[0] == 8

    def test_core_views(self):
        from sirius.core.views import get_real_annotation_data
        result = get_real_annotation_data('GRCh38', 11800, 14500, 100, 96)
        d = json.loads(result)
        assert d['startBp'] == 11800 and d['endBp'] == 14500
        assert d["values"][0]['entity']['location'] == 'Chr1'
    


if __name__ == "__main__":
    unittest.main()
