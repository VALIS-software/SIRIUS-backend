import re
from collections import namedtuple
  
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

    def parse(self, so_far, rule='ROOT', paths=[[]], depth=0, root_rule=None):
        """
            This function recursively walks the grammar to generate all possible parse paths.
            The paths are returned to be ranked and returned as autocomplete suggestions
        """
        possibilities = []

        if len(so_far) == 0:
            # return an EOF for empty string
            for path in paths:
                possibilities.append(path[:] + [Token('EOF', '', 'EOF', depth)])
        elif isinstance(rule, str):
            # if we have reached a rule that maps to a token expand it:
            if rule in self.grammar:
                return self.parse(so_far, self.grammar[rule], paths, depth, rule)
            # otherwise check if the pattern matches 
            m = self.patterns[rule].match(so_far.strip())
            if m is not None:
                # if there is  match append the match to each path
                val, offset = m.group(), m.end()
                rest = so_far[offset:]
                for path in paths:
                    possibilities.append(path[:] + [Token(root_rule or rule , rest, val, depth)])
            else:
                # otherwise return a parse error at the end of each path
                for path in paths:
                    possibilities.append(path[:] + [Token(root_rule or rule, so_far, 'ERROR', depth)])
        else:
            # if we have reached a rule that maps to another rule in the grammar, expand it:
            if isinstance(rule, str) and rule in self.grammar:
                return self.parse(so_far, self.grammar[rule], paths, depth, rule)
            if (rule[0] == 'ANY'):
                # just union all possible parse paths together
                for sub_rule in rule[1:]:
                    possibilities += self.parse(so_far, sub_rule, paths)
            elif (rule[0] == 'ALL'):
                # get the root possibilities for the first AND clause
                root_possibilities = self.parse(so_far, rule[1], paths, depth)
                for possibility in root_possibilities:
                    if (possibility[-1]["value"] != 'ERROR'):
                        # for each parse that doesn't result in an error, keep expanding the remainder
                        rest = possibility[-1]["remainder"]
                        if len(rule[2:]) > 1:
                            next_possibilities = self.parse(rest, ['ALL'] + rule[2:], [[]], depth + 1)
                        else:
                            next_possibilities = self.parse(rest, rule[2], [[]], depth + 1)
                        for p in next_possibilities:
                            possibilities.append(possibility[:] + p)    
                    else:
                        # just add the current parse path 
                        possibilities.append(possibility[:])
        return sorted(possibilities, key=lambda x : -x[-1]["depth"])


def test_all():  
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
    assert(len(results) == 3)
    assert(results[0]['type'] == 'A')
    assert(results[0]['depth'] == 0)
    assert(results[1]['type'] == 'B')
    assert(results[1]['depth'] == 1)
    assert(results[2]['type'] == 'C')
    assert(results[2]['depth'] == 2)


def test_any():  
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
    assert(len(results) == 2)
    resultsX = results[0]
    resultsY = results[1]
    assert(len(resultsX) == 3)
    assert(len(resultsY) == 1)
    assert(resultsX[0]['type'] == 'A')
    assert(resultsX[0]['depth'] == 0)
    assert(resultsX[1]['type'] == 'B')
    assert(resultsX[1]['depth'] == 1)
    assert(resultsX[2]['type'] == 'C')
    assert(resultsX[2]['depth'] == 2)
    assert(resultsY[0]['remainder'] == 'abc')
    assert(resultsY[0]['value'] == 'ERROR')

    results = p.parse('abb')
    assert(len(results) == 2)
    assert(len(results[0]) == 3)
    assert(results[0][2]['value'] == 'ERROR')
    assert(len(results[1]) == 1)



def test_variant_parser():  
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
    assert(len(results[0]) == 2)
    assert(results[0][0]['type'] == 'VARIANT_OF')
    assert(results[0][0]['depth'] == 0)
    assert(results[0][1]['type'] == 'GENE')
    assert(results[0][1]['value'] == '"MAOA"')
    assert(results[0][1]['depth'] == 1)

    results = p.parse('Variants influencing "Cancer"')
    assert(len(results[0]) == 2)
    assert(results[0][0]['type'] == 'VARIANT_INFLUENCING')
    assert(results[0][0]['depth'] == 0)
    assert(results[0][1]['type'] == 'TRAIT')
    assert(results[0][1]['value'] == '"Cancer"')
    assert(results[0][1]['depth'] == 1)

test_variant_parser()
