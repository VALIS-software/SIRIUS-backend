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

test_variant_parser()
