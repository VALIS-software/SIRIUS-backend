import string
import nltk
import collections
from functools import lru_cache
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
import fuzzyset
import numpy as np

from sirius.helpers.loaddata import loaded_gene_names, loaded_trait_names, loaded_cell_types, loaded_patient_tumor_sites


nltk.download('stopwords')
nltk.download('punkt')
stop_word_set = set(stopwords.words('english') + list(string.punctuation))

class SearchIndex:
    def __init__(self, documents, enable_fuzzy=True):
        self.documents = np.array(documents)
        all_tokens = set(nltk.word_tokenize(' '.join(documents))) - stop_word_set
        self.fuzzyset = fuzzyset.FuzzySet(all_tokens, use_levenshtein=False)
        self.tfidf = TfidfVectorizer(tokenizer=self.tokenize_document, stop_words=stop_word_set)
        self.tfs = self.tfidf.fit_transform(self.documents)

    def tokenize_document(self, doc):
        tokens = [x.lower() for x in nltk.word_tokenize(doc) if x not in stop_word_set]
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
            for v, s in self.fuzzyset.get(t):
                fuzzy_matched_token_values[s] += v
        matched_tokens, fuzzy_matching_score = zip(*fuzzy_matched_token_values.items())
        # compute the score of each matched token
        token_feature_scores = self.tfidf.transform(matched_tokens)
        # weighted sum the scores
        fuzzy_matching_weight = np.array(fuzzy_matching_score)[:, np.newaxis]
        weighted_summed_scores = np.array(token_feature_scores.multiply(fuzzy_matching_weight).sum(axis=0))[0]
        # compute the dot product
        document_matching_scores = self.tfs.dot(weighted_summed_scores)
        # adjust max_hits to number of the positive values
        max_hits = min(max_hits, len(document_matching_scores.nonzero()[0]))
        # get the heighest matching document indices
        best_matching_doc_idxs = (-document_matching_scores).argsort()[:max_hits]
        suggestions = self.documents[best_matching_doc_idxs].tolist()
        return suggestions

loaded_SearchIndex = {
    'GENE': SearchIndex(loaded_gene_names),
    'TRAIT': SearchIndex(loaded_trait_names),
    'CELL_TYPE': SearchIndex(loaded_cell_types),
    'TUMOR_SITES': SearchIndex(loaded_patient_tumor_sites)
}

def get_suggestions(term, search_text, max_results=15):
    searchIdx = loaded_SearchIndex.get(term, None)
    if searchIdx is None: return []
    return searchIdx.get_suggestions(search_text, max_results)
