#!/usr/bin/env python

import unittest
from sirius.tests.timed_test_case import TimedTestCase

class SiriusTest(TimedTestCase):
    def test_import(self):
        """ Test import sirius """
        import sirius

    def test_tileDB(self):
        """ Test import tiledb """
        import tiledb

    def test_pyBigWig(self):
        """ Test import pyBigWig """
        import pyBigWig

if __name__ == "__main__":
    unittest.main()
