import os
import shutil
import unittest
from sirius.tests.timed_test_case import TimedTestCase
from sirius.parsers import TSVParser, TSVParser_GWAS, TSVParser_ENCODEbigwig

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class TSVParserTest(TimedTestCase):
    def setUp(self):
        super(TSVParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_GWAS.tsv')

    def test_init(self):
        """ Test TSVParser.__init__()"""
        parser = TSVParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'TSVParser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.tsv', 'TSVParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test TSVParser.parse() """
        parser = TSVParser(self.testfile)
        parser.parse()
        self.assertIn('studies', parser.data, 'TSVParser should give self.data["studies"] after parsing')
        self.assertEqual(len(parser.studies), 98, 'Parsing test_GWAS.tsv should give 98 studies.')
        for study in parser.studies:
            for key in ('DISEASE/TRAIT', 'CHR_ID', 'CHR_POS', 'SNPS', 'P-VALUE'):
                self.assertIn(key, study, f'All studies should contain key {key}')

class TSVParser_GWASTest(TimedTestCase):
    def setUp(self):
        super(TSVParser_GWASTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_GWAS.tsv')

    def test_get_mongo_nodes(self):
        """ Test TSVParser_GWAS.get_mongo_nodes() """
        parser = TSVParser_GWAS(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        self.assertEqual(len(genome_nodes), 0, 'Parsing test_GWAS.tsv should give 0 GenomeNodes')
        self.assertEqual(len(info_nodes), 10, 'Parsing test_GWAS.tsv should give 10 InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', 'Parising test_GWAS.tsv should give 1 InfoNode with type dataSource')
        self.assertEqual(len(edges), 145, 'Parsing test_GWAS.tsv should give 145 Edges')

class TSVParser_ENCODEbigwigTest(TimedTestCase):
    def setUp(self):
        super(TSVParser_ENCODEbigwigTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_bigwig_metadata.tsv')

    def test_get_mongo_nodes(self):
        """ Test TSVParser_ENCODEbigwig.get_mongo_nodes() """
        parser = TSVParser_ENCODEbigwig(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        fn = os.path.basename(self.testfile)
        self.assertEqual(len(genome_nodes), 0, f'Parsing {fn} should give 0 GenomeNodes')
        self.assertEqual(len(info_nodes), 20, f'Parsing {fn} should give 11 InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', f'Parsing {fn} should give 1 InfoNode with type dataSource')
        self.assertEqual(len(edges), 0, f'Parsing {fn} should give no Edge')

if __name__ == "__main__":
    unittest.main()
