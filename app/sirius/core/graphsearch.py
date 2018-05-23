import json
import re
from collections import namedtuple
from functools import lru_cache
import fuzzyset
# from sirius.query.QueryTree import QueryTree
# from sirius.core.utilities import get_data_with_id, HashableDict

def Token(ttype, remainder, value, depth):
    return {
        "type": ttype,
        "remainder": remainder,
        "value": value,
        "depth": depth
    }

class Parser:
    def __init__(self, grammar, tokens, suggestions):
        self.grammar = grammar
        self.tokens = tokens
        self.patterns = {}
        for token in self.tokens.keys():
            p = self.tokens[token]
            self.patterns[token] = re.compile(p)
            
    def get_suggestions(self, input_text):
        return self.parse(input_text, self.grammar['ROOT'])

    def eat(self, so_far, rule):
        m = self.patterns[rule].match(so_far)
        if m is not None:
            # if there is  match append the match to each path
            val, offset = m.group(), m.end()
            return val, so_far[offset:]
        return None, so_far

    def parse(self, so_far, rule, depth=0):
        """
            This function recursively walks the grammar to generate all possible parse paths.
            The paths are returned to be ranked and returned as autocomplete suggestions
        """
        so_far = so_far.strip().lower()

        if (isinstance(rule, str) and rule in self.tokens):
            return [(rule, depth)]
        if (isinstance(rule, str) and rule in self.grammar):
            return self.parse(so_far, self.grammar[rule], depth)
        elif (rule[0] == 'ANY'):
            # just union all possible parse paths together
            possibilities = []
            for sub_rule in rule[1:]:
                possibilities += self.parse(so_far, sub_rule, depth)
            return possibilities
        elif (rule[0] == 'ALL'):
            if (rule[1] in self.tokens):
                # check if we can eat part of the input
                parsed, rest = self.eat(so_far, rule[1])
                if rest == so_far:
                    # we were not able to eat a token! return suggestions for the current token rule
                    return self.parse(so_far, rule[1], depth)
                else:
                    # we were able to eat a token! return suggestions for the remainder
                    if (len(rule[2:]) == 0):
                        return []
                    if (len(rule[2:]) == 1):
                        return self.parse(rest, rule[2], depth + 1)
                    else:
                        return self.parse(rest, ['ALL'] + rule[2:], depth + 1)
        return []


@lru_cache(maxsize=1)
def get_parser():
    genes = json.loads(open("/Users/saliksyed/Desktop/genes2.json").read())["genes"]
    traits = ['Cancer', 'Alzheimers', 'Dementia']
    # genes = []
    # traits = []
    # # load the gene names
    # query = {"type": "GenomeNode", "filters": {"type": "gene"}, "toEdges": []}
    # qt = QueryTree(query)
    # genes = qt.find()

    # # load the trait names
    # query = {"type": "InfoNode", "filters": {"type": "trait"}, "toEdges": []}
    # qt = QueryTree(query)
    # traits = qt.find().distinct('info.description')

    tokens = {
        'TRAIT': '"\w+"',
        'GENE': '"\w+"',
        'INFLUENCING': 'influencing',
        'OF': 'of',
        'VARIANTS': 'variants',
    }

    grammar = {
        'VARIANT_INFLUENCING_ASSOCIATION': ['ALL', 'OF', 'GENE'],
        'VARIANT_OF_ASSOCIATION': ['ALL', 'INFLUENCING', 'TRAIT'],
        'VARIANT_ASSOCIATION': ['ANY', 'VARIANT_INFLUENCING_ASSOCIATION', 'VARIANT_OF_ASSOCIATION'],
        'VARIANT_QUERY': ['ALL', 'VARIANTS', 'VARIANT_ASSOCIATION'],
        'ROOT': ['ANY', 'VARIANT_QUERY']
    }

    suggestions = {
        'GENE': genes,
        'TRAIT': traits,
    }

    return Parser(grammar, tokens, suggestions)

def get_recommendations(search_text):
    # parse the search text:
    p = get_parser()
    return p.get_suggestions(search_text)

while True:
    val = input("Enter a search:  ")
    print(get_recommendations(val))

