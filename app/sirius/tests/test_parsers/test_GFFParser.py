import os
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.GFFParser import GFFParser, GFFParser_RefSeq, GFFParser_ENSEMBL

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class GFFParserTest(TimedTestCase):
    def setUp(self):
        super(GFFParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_refseq.gff')

    def test_init(self):
        """ Test GFF Parser.__init__() """
        parser = GFFParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'Parser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.gff', 'GFFParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test GFFParser.parse() """
        parser = GFFParser(self.testfile)
        parser.parse()
        self.assertIn('features', parser.data, 'GFFParser should give self.data["features"] after parsing')
        self.assertEqual(len(parser.features), 59, f'Parsing {self.testfile} should give 59 features.')
        for feature in parser.features:
            for key in ('seqid', 'type', 'start', 'end', 'attributes'):
                self.assertIn(key, feature, f'All features should contain key {key}')

    def test_parse_chunk(self):
        """ Test GFFParser.parse_save_data_in_chunks() """
        parser = GFFParser(self.testfile)
        size = 10
        parser.parse_chunk(size)
        self.assertEqual(len(parser.features), size, f'Parsing to a chunk of {size} should give {size} features.')

class GFFParser_RefSeqTest(TimedTestCase):
    def setUp(self):
        super(GFFParser_RefSeqTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_refseq.gff')

    def test_mongo_nodes(self):
        """ Test GFFParser_RefSeq.get_mongo_nodes() """
        parser = GFFParser_RefSeq(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        n_gnode = 58
        self.assertEqual(len(genome_nodes), n_gnode, f'Parsing {self.testfile} should give {n_gnode} GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('contig',str), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
        n_info = 2
        self.assertEqual(len(info_nodes), n_info, f'Parsing {self.testfile} should give {n_info} InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', f'Parising {self.testfile} should give 1 InfoNode with type dataSource')
        self.assertEqual(info_nodes[1]['type'], 'contig', f'Parising {self.testfile} should give 1 InfoNode with type contig')
        self.assertEqual(len(edges), 0, f'Parsing {self.testfile} should give no Edge')

if __name__ == "__main__":
    unittest.main()
