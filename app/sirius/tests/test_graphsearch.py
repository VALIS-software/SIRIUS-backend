#!/usr/bin/env python

import unittest
from sirius.tests.TimedTestCase import TimedTestCase
from sirius.core.graphsearch import build_parser
from sirius.core.utilities import HashableDict

class GraphSearchText(TimedTestCase):
    def parse_text(self, text):
        genes = ['MAOA', 'MAOB', 'PCSK9', 'NF2']
        traits = ['Cancer', 'Alzheimers', 'Depression']
        suggestions = {
            'GENE': genes,
            'TRAIT': traits,
        }
        p = build_parser(HashableDict(suggestions))
        result = p.get_suggestions(text)
        return result


    def test_empty_query(self):
        """ Test empty search text parses to suggestions for gene, variant, trait """
        tokens, suggestions, query, is_quoted = self.parse_text('')

    def test_parse_variant_query_incomplete(self):
        """ Test variant search returns correct suggestions """
        tokens, suggestions, query, is_quoted = self.parse_text('variants')
        self.assertEqual(len(tokens), 1)
        self.assertEqual(len(suggestions), 1)
        self.assertEqual(query, None)

    def test_parse_variant_query_influencing(self):
        """ Test autocomplete of gene trait """
        tokens, suggestions, query, is_quoted = self.parse_text('variants influencing')
        self.assertEqual(len(tokens), 2)
        self.assertEqual(len(suggestions), 3)
        self.assertEqual(query, None)

    def test_parse_gene_query_complete(self):
        """ Test valid search text parses to Query """
        tokens, suggestions, query, is_quoted = self.parse_text('gene \"MAOA\"')
        self.assertEqual(len(tokens), 3)
        self.assertEqual(len(suggestions), 1)
        self.assertEqual(tokens[0].rule, 'GENE_T')
        self.assertEqual(suggestions[-1], "MAOA")
        self.assertNotEqual(query, None)

    def test_parse_gene_query_prefix_quoted(self):
        """ Test valid search text parses to Query """
        tokens, suggestions, query, is_quoted = self.parse_text('gene "MAO"')
        self.assertEqual(len(tokens), 3)
        self.assertEqual(tokens[0].rule, 'GENE_T')
        self.assertEqual(len(suggestions), 2)
        self.assertEqual(suggestions[-1], "MAOB")
        self.assertNotEqual(query, None)
    
    def test_parse_cell_query(self):
        """ Test enhancer query parses properly """
        tokens, suggestions, query, is_quoted = self.parse_text('enhancers in "heart cell"')
        self.assertEqual(query['filters']['type'], 'Enhancer-like')
        self.assertEqual(query['filters']['info.biosample'], 'heart cell')
        self.assertNotEqual(query, None)

if __name__ == "__main__":
    unittest.main()
