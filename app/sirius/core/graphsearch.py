from python_parser import Parser, a, anyof, someof, maybe, skip, to_dot
from sirius.query.QueryTree import QueryTree
from sirius.core.utilities import get_data_with_id, HashableDict
from functools import lru_cache

import re
from collections import namedtuple

    
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
    'ROOT': ['ANY', 'GENE_QUERY', 'TRAIT_QUERY']
}

def Token(ttype, remainder, value, depth):
    return {
        "type": ttype,
        "remainder": remainder,
        "value": value,
        "depth": depth
    }

class Parser:
    def __init__(self, grammar, tokens):
        self.grammar = grammar
        self.tokens = tokens
        self.patterns = {}
        for token in self.tokens.keys():
            p = self.tokens[token]
            self.patterns[token] = re.compile(p)

    def parse(self, so_far, rule='ROOT', paths=[[]], depth=0):
        possibilities = []
        if len(so_far) == 0:
            for path in paths:
                possibilities.append(path[:] + [Token('EOF', '', '', depth + 1)])
        elif isinstance(rule, str):
            if rule in self.grammar:
                return self.parse(so_far, self.grammar[rule], paths, depth)
            m = self.patterns[rule].match(so_far.strip())
            if m is not None:
                val, offset = m.group(), m.end()
                rest = so_far[offset + 1:]
                for path in paths:
                    possibilities.append(path[:] + [Token(rule , rest, val, depth + 1)])
            else:
                for path in paths:
                    possibilities.append(path[:] + [Token(rule, so_far, 'ERROR', depth + 1)])
        else:
            if isinstance(rule, str) and rule in self.grammar:
                rule = self.grammar[rule]
            if (rule[0] == 'ANY'):
                for sub_rule in rule[1:]:
                    possibilities += self.parse(so_far, sub_rule, paths)
            elif (rule[0] == 'ALL'):
                root_possibilities = self.parse(so_far, rule[1], paths)

                for possibility in root_possibilities:
                    if (possibility[-1]["value"] != 'ERROR'):
                        rest = possibility[-1]["remainder"]
                        if len(rule[2:]) > 1:
                            next_possibilities = self.parse(rest, ['ALL'] + rule[2:], [[]], depth + 1)
                        else:
                            next_possibilities = self.parse(rest, rule[2], [[]], depth + 1)

                        for p in next_possibilities:
                            possibilities.append(possibility[:] + p)    
                    else:
                        possibilities.append(possibility[:])

                    
                
        return possibilities



p = Parser(grammar, tokens)
print(p.parse('Variants of "MAOA"'))

