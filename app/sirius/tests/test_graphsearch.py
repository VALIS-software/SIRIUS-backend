import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.graphsearch import Parser

class GraphSearchTest(TimedTestCase):
    def test_all(self):  
        tokens = {
            'A': "a",
            'B': "b",
            'C': "c"
        }

        grammar = {
            'ROOT': ['ALL', 'A', 'B', 'C']
        }

        p = Parser(grammar, tokens)
        results = p.parse('abc')[0]
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]['type'], 'A')
        self.assertEqual(results[0]['depth'], 0)
        self.assertEqual(results[1]['type'], 'B')
        self.assertEqual(results[1]['depth'], 1)
        self.assertEqual(results[2]['type'], 'C')
        self.assertEqual(results[2]['depth'], 2)


    def test_any(self):  
        tokens = {
            'A': "a",
            'B': "b",
            'C': "c"
        }

        grammar = {
            'X': ['ALL', 'A', 'B', 'C'],
            'Y': ['ALL', 'C', 'B', 'A'],
            'ROOT': ['ANY', 'X', 'Y']
        }

        p = Parser(grammar, tokens)
        results = p.parse('abc')
        self.assertEqual(len(results), 2)
        resultsX = results[0]
        resultsY = results[1]
        self.assertEqual(len(resultsX), 3)
        self.assertEqual(len(resultsY), 1)
        self.assertEqual(resultsX[0]['type'], 'A')
        self.assertEqual(resultsX[0]['depth'], 0)
        self.assertEqual(resultsX[1]['type'], 'B')
        self.assertEqual(resultsX[1]['depth'], 1)
        self.assertEqual(resultsX[2]['type'], 'C')
        self.assertEqual(resultsX[2]['depth'], 2)
        self.assertEqual(resultsY[0]['remainder'], 'abc')
        self.assertEqual(resultsY[0]['value'], 'ERROR')

        results = p.parse('abb')
        assert(len(results) == 2)
        assert(len(results[0]) == 3)
        assert(results[0][2]['value'] == 'ERROR')
        assert(len(results[1]) == 1)



    def test_variant_parser(self):  
        tokens = {
            'STR': '"\w+"',
            'VARIANT_OF': 'Variants of',
            'VARIANT_INFLUENCING': 'Variants influencing'

        }

        grammar = {
            'TRAIT': 'STR',
            'GENE': 'STR',
            'GENE_QUERY': ['ALL', 'VARIANT_OF', 'GENE'],
            'TRAIT_QUERY': ['ALL', 'VARIANT_INFLUENCING', 'TRAIT'],
            'ROOT': ['ANY', 'GENE_QUERY', 'TRAIT_QUERY', 'GENE', 'TRAIT']
        }

        p =  Parser(grammar, tokens)
        results = p.parse('Variants of "MAOA"')
        self.assertEqual(len(results[0]), 2)
        self.assertEqual(results[0][0]['type'], 'VARIANT_OF')
        self.assertEqual(results[0][0]['depth'], 0)
        self.assertEqual(results[0][1]['type'], 'GENE')
        self.assertEqual(results[0][1]['value'], '"MAOA"')
        self.assertEqual(results[0][1]['depth'], 1)

        results = p.parse('Variants influencing "Cancer"')
        self.assertEqual(len(results[0]), 2)
        self.assertEqual(results[0][0]['type'], 'VARIANT_INFLUENCING')
        self.assertEqual(results[0][0]['depth'], 0)
        self.assertEqual(results[0][1]['type'], 'TRAIT')
        self.assertEqual(results[0][1]['value'], '"Cancer"')
        self.assertEqual(results[0][1]['depth'], 1)


if __name__ == "__main__":
    unittest.main()
