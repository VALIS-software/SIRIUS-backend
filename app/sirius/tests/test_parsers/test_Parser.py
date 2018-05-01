import os
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.Parser import Parser

class ParserTest(TimedTestCase):
    def test_init(self):
        """Test Parser Initializer"""
        parser = Parser('1.t')
        self.assertEqual(parser.metadata['filename'], '1.t', 'Parser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.t', 'Parser should have self.ext set to extension of file.')

    def test_parse(self):
        """Test Parser.parse()"""
        parser = Parser('1.t')
        with self.assertRaises(NotImplementedError):
            parser.parse()

    def test_mongo_nodes(self):
        """Test Parser.get_mongo_nodes()"""
        parser = Parser('1.t')
        with self.assertRaises(NotImplementedError):
            parser.get_mongo_nodes()

if __name__ == "__main__":
    unittest.main()
