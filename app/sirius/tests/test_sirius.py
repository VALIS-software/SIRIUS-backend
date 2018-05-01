#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase

class SiriusTest(TimedTestCase):
    def test_import(self):
        """Test basic import sirius"""
        import sirius

if __name__ == "__main__":
    unittest.main()
