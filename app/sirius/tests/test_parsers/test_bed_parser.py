import os
import shutil
import unittest
from sirius.tests.timed_test_case import TimedTestCase
from sirius.parsers import BEDParser, BEDParser_ENCODE

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class BEDParserTest(TimedTestCase):
    def setUp(self):
        super(BEDParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test.bed')

    def test_init(self):
        """ Test BEDParser.__init__()"""
        parser = BEDParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'BEDParser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.bed', 'BEDParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test BEDParser.parse() """
        parser = BEDParser(self.testfile)
        parser.parse()
        self.assertIn('intervals', parser.data, 'BEDParser should give self.data["intervals"] after parsing')
        n_expected = 16
        self.assertEqual(len(parser.intervals), n_expected, f'Parsing {parser.filename} should give {n_expected} intervals.')
        for var in parser.intervals:
            for key in ('chrom', 'start', 'end'):
                self.assertIn(key, var, f'All intervals should contain key {key}')

    def test_mongo_nodes(self):
        """ Test BEDParser.get_mongo_nodes() """
        parser = BEDParser(self.testfile)
        with self.assertRaises(NotImplementedError):
            parser.get_mongo_nodes()


class BEDParser_ENCODETest(TimedTestCase):
    def setUp(self):
        super(BEDParser_ENCODETest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test.bed')

    def test_mongo_nodes(self):
        """ Test BEDParser_ENCODETest.get_mongo_nodes() """
        parser = BEDParser_ENCODE(self.testfile)
        parser.metadata['biosample'] = '#biosample#'
        parser.metadata['accession'] = '#accession#'
        parser.metadata['description'] = '#description#'
        parser.metadata['targets'] = ['#target#']
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        n_gnode = 2
        self.assertEqual(len(genome_nodes), n_gnode, f'Parsing {parser.filename} should give {n_gnode} GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('contig',str), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
            for infokey in ('biosample', 'accession', 'targets'):
                self.assertIn(infokey, gn['info'], f"All GenomeNodes should have key info.{infokey}")
        n_inode = 1
        self.assertEqual(len(info_nodes), n_inode, f'Parsing {parser.filename} should give {n_inode} InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'ENCODE_accession', f'Parising {parser.filename} should give 1 InfoNode with type ENCODE_accession')
        self.assertIn('description', info_nodes[0]['info'], f'ENCODE_accession InfoNode should have info.description')
        n_edge = 0
        self.assertEqual(len(edges), n_edge, f'Parsing {parser.filename} should give {n_edge} Edges')


if __name__ == "__main__":
    unittest.main()
