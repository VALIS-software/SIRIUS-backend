import json
import re
from functools import lru_cache
from sirius.query.QueryTree import QueryTree
from sirius.helpers.loaddata import loaded_gene_names, loaded_trait_names

def Token(ttype, remainder, value, depth):
    return {
        "type": ttype,
        "remainder": remainder,
        "value": value,
        "depth": depth
    }

class QueryParser:
    def __init__(self, grammar, tokens, suggestions):
        self.grammar = grammar
        self.tokens = tokens
        self.patterns = {}
        for token in self.tokens.keys():
            p = self.tokens[token]
            self.patterns[token] = re.compile(p)
        self.suggestions = suggestions

    def is_terminal(self, token):
        return token[0] in self.tokens

    def build_variant_query(self, parse_path):
        token = parse_path[0]
        print(parse_path[1])
        q = None
        if token[0] == 'OF':
            gene_name = parse_path[1][1][1:-1]
            # TODO: return a boolean track intersecting SNPs with Gene
            return {
                "query" : "TODO"
            }
        elif token[0] == 'INFLUENCING':
            trait_name = parse_path[1][1][1:-1]
            return {
              "type": "GenomeNode",
              "filters": {

              },
              "toEdges": [
                {
                  "type": "EdgeNode",
                  "filters": {
                    "info.p-value": {
                      "<": 0.05
                    }
                  },
                  "toNode": {
                    "type": "InfoNode",
                    "filters": {
                      "type": "trait",
                      "$text": trait_name
                    },
                    "toEdges": [

                    ]
                  }
                }
              ],
              "limit": 1000000
            }

    def build_trait_query(self, parse_path):
        trait_name = parse_path[0][1][1:-1]
        return {"type":"InfoNode","filters":{"type":"trait","$text": trait_name},"toEdges":[],"limit":150}

    def build_gene_query(self, parse_path):
        gene_name = parse_path[0][1][1:-1]
        return {"type":"GenomeNode","filters":{"type":"gene","name": gene_name},"toEdges":[],"limit":150}

    def build_query(self, parse_path):
        print(parse_path)
        token = parse_path[0]
        if token[0] == 'VARIANTS':
            return self.build_variant_query(parse_path[1:])
        elif token[0] == 'GENE_T':
            return self.build_gene_query(parse_path[1:])
        elif token[0] == 'TRAIT_T':
            return self.build_trait_query(parse_path[1:])

    def get_suggestions(self, input_text, max_suggestions=15):
        results = self.parse(input_text, self.grammar['ROOT'])
        max_parse = max(results, key=lambda x : len(x[-1]))
        max_depth = len(max_parse[2])
        final_suggestions = []
        quoted_suggestion = False
        for token, token_text, path in [x for x in results]:
            if len(path) != max_depth:
                continue
            if token == 'EOF':
                # ignore the EOF and keep giving suggestions for the previous token
                token = path[-2][0]
                # set the token text to the text with '"' characters removed
                token_text = path[-2][1][1:-1]
            if token in self.suggestions:
                token_text = token_text.strip().lower()
                # try doing a prefix match with the remainder
                for suggestion in self.suggestions[token]:
                    suggestion_l = suggestion.lower()
                    if token_text in suggestion_l and suggestion_l.index(token_text) == 0:
                        final_suggestions.append(suggestion)
                        if len(final_suggestions) >= max_suggestions:
                            break
                quoted_suggestion = True
            else:
                # just return the token string
                quoted_suggestion = False
                final_suggestions.append(self.tokens[token])
                if len(final_suggestions) >= max_suggestions:
                    break
        query = None
        if max_parse[0] == 'EOF':
            query = self.build_query(max_parse[2])
        paths_to_return = [result[2] for result in results if len(result[2]) == max_depth]
        return paths_to_return[0], final_suggestions, query, quoted_suggestion

    def eat(self, so_far, rule):
        so_far = so_far.strip()
        m = self.patterns[rule].match(so_far.lower())
        if m is not None:
            # if there is  match append the match to each path
            val, offset = m.group(), m.end()
            return so_far[:offset], so_far[offset:]
        return None, so_far

    def parse(self, so_far, rule, path=[]):
        """
            This function recursively walks the grammar to generate all possible parse paths.
            The paths are returned to be ranked and returned as autocomplete suggestions
        """
        if (rule == 'EOF' and len(so_far) == 0):
            new_path = path[:]
            new_path.append(('EOF', ''))
            return [(rule, so_far, new_path)]
        if (isinstance(rule, str) and rule in self.tokens):
            return [(rule, so_far, path[:])]
        if (isinstance(rule, str) and rule in self.grammar):
            return self.parse(so_far, self.grammar[rule], path[:])
        elif (rule[0] == 'ANY'):
            # just union all possible parse paths together
            possibilities = []
            for sub_rule in rule[1:]:
                possibilities += self.parse(so_far, sub_rule, path[:])
            return possibilities
        elif (rule[0] == 'ALL'):
            if (rule[1] in self.tokens):
                # check if we can eat part of the input
                parsed, rest = self.eat(so_far, rule[1])
                new_path = path[:]
                new_path.append((rule[1], parsed))
                if rest == so_far or parsed == None:
                    # we were not able to eat a token! return suggestions for the current token rule
                    return self.parse(so_far, rule[1], path[:])
                else:
                    # we were able to eat a token! return suggestions for the remainder
                    if (len(rule[2:]) == 0):
                        return []
                    if (len(rule[2:]) == 1):
                        return self.parse(rest, rule[2], new_path)
                    else:
                        return self.parse(rest, ['ALL'] + rule[2:], new_path)
        return []


def get_default_parser_settings():
    tokens = {
        'TRAIT': '"(.+?)"',
        'GENE': '"(.+?)"',
        'INFLUENCING': 'influencing',
        'OF': 'of',
        'VARIANTS': 'variants',
        'GENE_T': 'gene',
        'TRAIT_T': 'trait',
    }

    grammar = {
        'VARIANT_OF_ASSOCIATION': ['ALL', 'OF', 'GENE', 'EOF'],
        'VARIANT_INFLUENCING_ASSOCIATION': ['ALL', 'INFLUENCING', 'TRAIT', 'EOF'],
        'VARIANT_ASSOCIATION': ['ANY', 'VARIANT_INFLUENCING_ASSOCIATION', 'VARIANT_OF_ASSOCIATION'],
        'VARIANT_QUERY': ['ALL', 'VARIANTS', 'VARIANT_ASSOCIATION'],
        'GENE_QUERY' : ['ALL', 'GENE_T', 'GENE', 'EOF'],
        'TRAIT_QUERY' : ['ALL', 'TRAIT_T', 'TRAIT', 'EOF'],
        'ROOT': ['ANY', 'VARIANT_QUERY', 'GENE_QUERY', 'TRAIT_QUERY']
    }

    return tokens, grammar

def load_suggestions():
    return {
        'GENE': loaded_gene_names,
        'TRAIT': loaded_trait_names,
    }

@lru_cache(maxsize=1)
def build_parser(suggestions=None):
    tokens, grammar = get_default_parser_settings()
    if suggestions == None:
        suggestions =  load_suggestions()
    return QueryParser(grammar, tokens, suggestions)

def get_suggestions(search_text):
    p = build_parser()
    tokens, suggestions, query, quoted_suggestion = p.get_suggestions(search_text)
    return {
        "tokens": tokens,
        "suggestions": suggestions,
        "query": query,
        "quoted_suggestion": quoted_suggestion,
    }

if __name__ == "__main__":
    print("Testing grammar")
    while(True):
        genes = ['MAOA', 'MAOB', 'PCSK9', 'NF2']
        traits = ['Cancer', 'Alzheimers', 'Depression']
        suggestions = {
            'GENE': genes,
            'TRAIT': traits,
        }
        p = build_parser(suggestions)
        text = input("Enter a search: ")
        result = p.get_suggestions(text)
        print(result)
