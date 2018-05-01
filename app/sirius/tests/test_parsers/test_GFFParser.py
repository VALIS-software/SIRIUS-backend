import os, shutil
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.GFFParser import GFFParser

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class GFFParserTest(TimedTestCase):
    def setUp(self):
        super(GFFParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test.gff')

    def test_init(self):
        """ Test GFF Parser.__init__() """
        parser = GFFParser(self.testfile)
        self.assertTrue(parser.metadata['filename'] == self.testfile, 'Parser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.gff', 'Parser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test GFFParser.parse() """
        parser = GFFParser(self.testfile)
        parser.parse()
        self.assertIn('features', parser.data, 'GFFParser should give self.data["features"] after parsing')
        self.assertEqual(len(parser.features), 59, 'Parsing test.gff should give 59 features.')
        for feature in parser.features:
            for key in ('seqid', 'type', 'start', 'end', 'attributes'):
                self.assertIn(key, feature, f'All features should contain key {key}')

    def test_parse_save_data_in_chunks(self):
        """ Test GFFParser.parse_save_data_in_chunks() """
        parser = GFFParser(self.testfile)
        os.mkdir('test.tmp')
        os.chdir('test.tmp')
        parser.parse_save_data_in_chunks(file_prefix='dataChunk', chunk_size=10)
        self.assertEqual(len([f for f in os.listdir('.') if f.startswith('dataChunk')]), 6, 'Parsing test.gff into chunks of 10 should give 6 chunk files')
        os.chdir('..')
        shutil.rmtree('test.tmp')

    def test_mongo_nodes(self):
        """ Test GFFParser.get_mongo_nodes() """
        parser = GFFParser(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        self.assertEqual(len(genome_nodes), 59, 'Parsing test.gff should give 59 GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('assembly',str), ('chromid',int), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
        self.assertEqual(len(info_nodes), 1, 'Parsing test.gff should give 1 InfoNode')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', 'Parising test.gff should give 1 InfoNode with type dataSource')
        self.assertEqual(len(edges), 0, 'Parsing test.gff should give no Edge')

if __name__ == "__main__":
    unittest.main()
