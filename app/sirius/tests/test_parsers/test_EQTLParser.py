import os, shutil
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.EQTLParser import EQTLParser

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class EQTLParserTest(TimedTestCase):
    def setUp(self):
        super(EQTLParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_eQTL.txt')

    def test_init(self):
        """ Test EQTLParser.__init__()"""
        parser = EQTLParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'EQTLParser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.txt', 'EQTLParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test EQTLParser.parse() """
        parser = EQTLParser(self.testfile)
        parser.parse()
        self.assertIn('eqtls', parser.data, 'EQTLParser should give self.data["eqtls"] after parsing')
        self.assertEqual(len(parser.eqtls), 99, 'Parsing test_eQTL.txt should give 98 studies.')
        for eqtl in parser.eqtls:
            for key in ('exSNP', 'exGENEID'):
                self.assertIn(key, eqtl, f'All eQTLs should contain key {key}')

    def test_mongo_nodes(self):
        """ Test EQTLParser.get_mongo_nodes() """
        parser = EQTLParser(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        self.assertEqual(len(genome_nodes), 0, 'Parsing test_eQTL.txt should have no GenomeNodes')
        self.assertEqual(len(info_nodes), 1, 'Parsing test_eQTL.txt should give 1 InfoNode')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', 'Parising test_eQTL.txt should give 1 InfoNode with type dataSource')
        self.assertEqual(len(edges), 99, 'Parsing test_eQTL.txt should give 98 Edges')

if __name__ == "__main__":
    unittest.main()
