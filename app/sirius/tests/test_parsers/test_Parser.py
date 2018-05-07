import os
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.Parser import Parser

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class ParserTest(TimedTestCase):
    def setUp(self):
        super(ParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test.gff')

    def test_init(self):
        """Test Parser Initializer"""
        parser = Parser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'Parser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.gff', 'Parser should have self.ext set to extension of file.')

    def test_parse(self):
        """Test Parser.parse()"""
        parser = Parser(self.testfile)
        with self.assertRaises(NotImplementedError):
            parser.parse()

    def test_mongo_nodes(self):
        """Test Parser.get_mongo_nodes()"""
        parser = Parser(self.testfile)
        with self.assertRaises(NotImplementedError):
            parser.get_mongo_nodes()

if __name__ == "__main__":
    unittest.main()
