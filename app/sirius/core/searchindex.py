import string
import nltk
import collections
import operator
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
import fuzzyset
import numpy as np

from sirius.helpers.loaddata import loaded_gene_names, loaded_trait_names, loaded_cell_types, loaded_patient_tumor_sites


nltk.download('stopwords')
nltk.download('punkt')
stop_word_set = set(stopwords.words('english') + list(string.punctuation))

def get_all(self, value):
    ''' Extend the fuzzyset.FuzzySet.__getitem__ method, to get all matching strings '''
    lvalue = value.lower()
    result = self.exact_set.get(lvalue)
    if result:
        return [(1, result)]
    matches = collections.defaultdict(float)
    for gram_size in range(self.gram_size_upper, self.gram_size_lower - 1, -1):
        grams = fuzzyset._gram_counter(lvalue, gram_size)
        items = self.items[gram_size]
        norm = sum(x**2 for x in grams.values())**0.5
        for gram, occ in grams.items():
            for idx, other_occ in self.match_dict.get(gram, ()):
                matches[idx] += occ * other_occ
        if not matches:
            continue
        results = [(match_score / (norm * items[idx][0]), items[idx][1])
                   for idx, match_score in matches.items()]
        results.sort(reverse=True, key=operator.itemgetter(0))
        return [(score, self.exact_set[lval]) for score, lval in results]
    else:
        print(f'{value} not found in fuzzyset')
        return []

fuzzyset.FuzzySet.get_all = get_all

class SearchIndex:
    def __init__(self, documents):
        self.documents = np.array(documents)
        if documents:
            self.tfidf = TfidfVectorizer(tokenizer=self.tokenize_document, stop_words=stop_word_set)
            self.tfs = self.tfidf.fit_transform(self.documents)
            self.fuzzyset = fuzzyset.FuzzySet(self.tfidf.get_feature_names(), use_levenshtein=False)

    def tokenize_document(self, doc):
        tokens = [x.lower() for x in nltk.wordpunct_tokenize(doc) if x not in stop_word_set]
        return tokens

    def get_suggestions(self, query, max_hits=100):
        """
        Input
        -----
        query: string, like 'alz dis'

        Output
        ------
        suggestions: list of strings
        The suggestions are the best matching strings in self.documents
        """
        tokens = self.tokenize_document(query)
        if len(tokens) == 0:
            return self.documents[:max_hits].tolist()
        # aggregate the fuzzy-matched token values
        fuzzy_matched_token_values = collections.defaultdict(float)
        for t in tokens:
            for v, s in self.fuzzyset.get_all(t):
                fuzzy_matched_token_values[s] += v
        if len(fuzzy_matched_token_values) == 0:
            return []
        matched_tokens, fuzzy_matching_score = zip(*fuzzy_matched_token_values.items())
        # compute the score of each matched token
        token_feature_scores = self.tfidf.transform(matched_tokens)
        # weighted sum the scores
        fuzzy_matching_weight = np.array(fuzzy_matching_score)[:, np.newaxis]
        weighted_summed_scores = np.array(token_feature_scores.multiply(fuzzy_matching_weight).sum(axis=0))[0]
        # compute the dot product
        document_matching_scores = self.tfs.dot(weighted_summed_scores)
        # sort the nonzero results
        nonzero_idxs = document_matching_scores.nonzero()[0]
        nonzero_values = document_matching_scores[nonzero_idxs]
        nonzero_sorted = np.argsort(-nonzero_values)[:max_hits]
        best_matching_doc_idxs = nonzero_idxs[nonzero_sorted]
        # get the best suggestions
        suggestions = self.documents[best_matching_doc_idxs].tolist()
        return suggestions

loaded_SearchIndex = {
    'GENE': SearchIndex(loaded_gene_names),
    'TRAIT': SearchIndex(loaded_trait_names),
    'CELL_TYPE': SearchIndex(loaded_cell_types),
    'TUMOR_SITE': SearchIndex(loaded_patient_tumor_sites)
}

def get_suggestions(term, search_text, max_results=15):
    searchIdx = loaded_SearchIndex.get(term, None)
    if searchIdx is None: return []
    return searchIdx.get_suggestions(search_text, max_results)
