from python_parser import Parser, a, anyof, someof, maybe, skip, to_dot
from sirius.query.QueryTree import QueryTree
from sirius.core.utilities import get_data_with_id, HashableDict

@lru_cache(maxsize=1)
def build_grammar():
    genes = []
    traits = []
    # load the gene names
    query = {"type": "GenomeNode", "filters": {"type": "gene"}, "toEdges": []}
    qt = QueryTree(query)
    genes = qt.find()

    # load the trait names
    query = {"type": "InfoNode", "filters": {"type": "trait"}, "toEdges": []}
    qt = QueryTree(query)
    traits = qt.find().distinct('info.description')
    
    tokens = [
        ('Variants of', 'SNP_OF'),
        ('Variants influencing', 'SNP_INFLUENCING'),
    ]

    for gene in genes:
        tokens.append((gene, 'GENE'))
    for trait in traits:
        tokens.append((trait, 'TRAIT'))
    
    grammar = {
        'SNPSet': anyof(a('SNP_INFLUENCING', 'TRAIT'), a('SNP_OF', 'GENE')),
        'EXPR': anyof('SNPSet'), 
        'QUERY': anyof('TRAIT', 'GENE', 'EXPR')
    }
    string_to_parse = 'Variants influencing'
    parser = Parser(tokens, grammar)

def get_recommendations(string_to_parse):
    parser = build_grammar()
    ast = parser.parse('QUERY', string_to_parse)
    return (to_dot(ast))
