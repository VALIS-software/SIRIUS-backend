#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.graphsearch import Parser, build_parser

class GraphSearchText(TimedTestCase):
    def parse_text(self, text):
        genes = ['MAOA', 'MAOB', 'PCSK9', 'NF2']
        traits = ['Cancer', 'Alzheimers', 'Depression']
        suggestions = {
            'GENE': genes,
            'TRAIT': traits,
        }
        p = build_parser(suggestions)
        result = p.get_suggestions(text)
        print(result)
        return result


    def test_empty_query(self):
        """ Test empty search text parses to suggestions for gene, variant, trait """
        tokens, suggestions, query = self.parse_text('')

    def test_parse_variant_query_incomplete(self):
        """ Test variant search returns correct suggestions """
        tokens, suggestions, query = self.parse_text('variants')
        self.assertEqual(len(tokens), 1)
        self.assertEqual(len(suggestions), 2)
        self.assertEqual(query, None)

    def test_parse_variant_query_missing_gene(self):
        """ Test autocomplete of gene names """
        tokens, suggestions, query = self.parse_text('variants of ')
        self.assertEqual(len(tokens), 2)
        self.assertEqual(len(suggestions), 4)
        self.assertEqual(query, None)

    def test_parse_variant_query_influencing(self):
        """ Test autocomplete of gene trait """
        tokens, suggestions, query = self.parse_text('variants influencing')
        self.assertEqual(len(tokens), 2)
        self.assertEqual(len(suggestions), 3)
        self.assertEqual(query, None)
        
    def test_parse_variant_query_complete(self):
        """ Test valid search text parses to Query """
        tokens, suggestions, query = self.parse_text('variants of "MAOA"')
        self.assertEqual(len(tokens), 4)
        self.assertEqual(len(suggestions), 0)
        self.assertNotEqual(query, None)

    def test_parse_variant_query_prefix(self):
        """ Test token prefix search works """
        tokens, suggestions, query = self.parse_text('variants of MAO')
        self.assertEqual(len(tokens), 2)
        self.assertEqual(len(suggestions), 2)
        self.assertEqual(query, None)

    def test_parse_variant_query_extra_spaces(self):
        """ Test spaces around tokens are ignored """
        tokens1, suggestions1, query1 = self.parse_text('variants    of    "MAOA"    ')
        tokens2, suggestions2, query2 = self.parse_text('    variants    of  "MAOA"    ')
        tokens3, suggestions3, query3 = self.parse_text('    variants of  "MAOA"    ')
        self.assertEqual(len(tokens1), len(tokens2))
        self.assertEqual(len(tokens2), len(tokens3))

if __name__ == "__main__":
    unittest.main()
