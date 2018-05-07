import os, shutil
import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.parsers.VCFParser import VCFParser, VCFParser_ClinVar, VCFParser_dbSNP

this_file_folder = os.path.dirname(os.path.realpath(__file__))

class VCFParserTest(TimedTestCase):
    def setUp(self):
        super(VCFParserTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_ClinVar.vcf')

    def test_init(self):
        """ Test VCFParser.__init__()"""
        parser = VCFParser(self.testfile)
        filename = os.path.basename(self.testfile)
        self.assertEqual(parser.metadata['filename'], filename, 'VCFParser should be initialized with self.data["metadata"] = {"filename": filename}')
        self.assertEqual(parser.ext, '.vcf', 'VCFParser should have self.ext set to extension of file.')

    def test_parse(self):
        """ Test VCFParser.parse() """
        parser = VCFParser(self.testfile)
        parser.parse()
        filename = os.path.basename(self.testfile)
        self.assertIn('variants', parser.data, 'VCFParser should give self.data["variants"] after parsing')
        n_variants = 19
        self.assertEqual(len(parser.variants), n_variants, f'Parsing {filename} should give {n_variants} studies.')
        for var in parser.variants:
            for key in ('CHROM', 'POS', 'REF', 'ALT', 'INFO'):
                self.assertIn(key, var, f'All variants should contain key {key}')

    def test_mongo_nodes(self):
        """ Test VCFParser.get_mongo_nodes() """
        parser = VCFParser(self.testfile)
        with self.assertRaises(NotImplementedError):
            parser.get_mongo_nodes()


class VCFParser_ClinVarTest(TimedTestCase):
    def setUp(self):
        super(VCFParser_ClinVarTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_ClinVar.vcf')

    def test_mongo_nodes(self):
        """ Test VCFParser_ClinVar.get_mongo_nodes() """
        parser = VCFParser_ClinVar(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        filename = os.path.basename(self.testfile)
        n_gnode = 19
        self.assertEqual(len(genome_nodes), n_gnode, f'Parsing {filename} should give {n_gnode} GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('contig',str), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
        n_inode = 6
        self.assertEqual(len(info_nodes), n_inode, f'Parsing {filename} should give {n_inode} InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', f'Parising {filename} should give 1 InfoNode with type dataSource')
        n_edge = 22
        self.assertEqual(len(edges), n_edge, f'Parsing {filename} should give {n_edge} Edges')


class VCFParser_dbSNPTest(TimedTestCase):
    def setUp(self):
        super(VCFParser_dbSNPTest, self).setUp()
        self.testfile = os.path.join(this_file_folder, 'files', 'test_dbSNP.vcf')

    def test_mongo_nodes(self):
        """ Test VCFParser_dbSNP.get_mongo_nodes() """
        parser = VCFParser_dbSNP(self.testfile)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        filename = os.path.basename(self.testfile)
        n_gnode = 151
        self.assertEqual(len(genome_nodes), n_gnode, f'Parsing {filename} should give {n_gnode} GenomeNodes')
        for gn in genome_nodes:
            self.assertEqual(gn['_id'][0], 'G', 'GenomeNodes should have _id starting with G')
            for key, typ in (('contig',str), ('start',int), ('end',int), ('length',int), ('name',str), ('type',str), ('source',str), ('info',dict)):
                self.assertIn(key, gn, f"All GenomeNodes should have key {key}")
                self.assertTrue(isinstance(gn[key], typ), f'GenomeNodes[{key}] should be type {typ}')
        n_inode = 1
        self.assertEqual(len(info_nodes), n_inode, f'Parsing {filename} should give {n_inode} InfoNodes')
        self.assertEqual(info_nodes[0]['type'], 'dataSource', f'Parising {filename} should give 1 InfoNode with type dataSource')
        n_edge = 0
        self.assertEqual(len(edges), n_edge, f'Parsing {filename} should give {n_edge} Edges')

if __name__ == "__main__":
    unittest.main()
