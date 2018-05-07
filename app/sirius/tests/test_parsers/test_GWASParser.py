import os, shutil
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.GWASParser import GWASParser

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class GWASParserTest(TimedTestCase):
    def setUp(self):
        super(GWASParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_GWAS.tsv')

    def test_init(self):
        """ Test GWASParser.__init__()"""
        parser = GWASParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'GWASParser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.tsv', 'GWASParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test GWASParser.parse() """
        parser = GWASParser(self.testfile)
        parser.parse()
        self.assertIn('studies', parser.data, 'GWASParser should give self.data["studies"] after parsing')
        self.assertEqual(len(parser.studies), 98, 'Parsing test_GWAS.tsv should give 98 studies.')
        for study in parser.studies:
            for key in ('DISEASE/TRAIT', 'CHR_ID', 'CHR_POS', 'SNPS', 'P-VALUE'):
                self.assertIn(key, study, f'All studies should contain key {key}')

    def test_mongo_nodes(self):
        """ Test GWASParser.get_mongo_nodes() """
        parser = GWASParser(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        self.assertEqual(len(genome_nodes), 87, 'Parsing test_GWAS.tsv should give 87 GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('contig',str), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
        self.assertEqual(len(info_nodes), 35, 'Parsing test_GWAS.tsv should give 35 InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', 'Parising test_GWAS.tsv should give 1 InfoNode with type dataSource')
        self.assertEqual(len(edges), 98, 'Parsing test_GWAS.tsv should give 98 Edges')

if __name__ == "__main__":
    unittest.main()
