#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.realdata.loaddata import loaded_annotations

class RealDataTest(TimedTestCase):
    def test_Annotation(self):
        """ Test realdata.loaddata.loaded_annotations """
        anno = loaded_annotations['GRCh38']
        self.assertEqual(len(anno.chromo_lengths), 24)

if __name__ == "__main__":
    unittest.main()
