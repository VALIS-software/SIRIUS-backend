import nltk
import collections
from functools import lru_cache
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
import fuzzyset

from sirius.helpers.loaddata import loaded_gene_names, loaded_trait_names, loaded_cell_types


nltk.download('stopwords')
nltk.download('punkt')
stop = set(stopwords.words('english'))

class SearchIndex:
	def __init__(self, data, dataKey):
		self.inverted_index = {}
		self.ngram_index = {}
		self.data = data
		self.dataKey = dataKey
		self.fuzzyset = fuzzyset.FuzzySet(use_levenshtein=False)
		self.documents = []
		for doc_id in self.data.keys():
			self.add_document(doc_id, self.data[doc_id])
			self.documents.append(self.data[doc_id][dataKey])
		
		self.tfidf = TfidfVectorizer(tokenizer=self.tokenize_document, stop_words='english')
		self.tfs = self.tfidf.fit_transform(self.documents)
	
	def add_document(self, doc_id, doc):
		tokens = self.tokenize_document(doc[self.dataKey])
		for token in tokens:
			if not token in self.inverted_index:
				self.inverted_index[token] = []
			self.inverted_index[token].append(doc_id)
			self.fuzzyset.add(token)

	def tokenize_document(self, doc):
		tokens = [x.lower() for x in nltk.word_tokenize(doc) if x not in stop]
		return tokens

	def get_token_results(self, tokens, seen=None):
		results = []
		for token in tokens:
			if token in self.inverted_index:
				results += self.inverted_index[token]
		return collections.Counter(results)

	def get_fuzzy_results(self, tokens):
		final_tokens = []
		for token in tokens:
			result = self.fuzzyset.get(token)
			if not result:
				continue
			final_tokens += [x[1] for x in result]
		return self.get_token_results(final_tokens)

	def score_result(self, result, query):
		tokens = self.tokenize_document(query)
		query_tokens = []
		for token in tokens:
			query_tokens += [x[1] for x in self.fuzzyset.get(token)]
		
		result_tokens = self.tokenize_document(result)

		tfidf1 = self.tfidf.transform([" ".join(result_tokens)])
		tfidf2 = self.tfidf.transform([" ".join(query_tokens)])

		feature_names = self.tfidf.get_feature_names()
		
		tf1scores = tfidf1.nonzero()[1]
		tf2scores = tfidf2.nonzero()[1]
		total = 0

		for col in tf1scores:
			if col in tf2scores:
				total += tfidf1[0, col] * tfidf2[0, col]

		return (total, result)

	def get_results(self, query, max_hits=100, enable_fuzzy=True):
		tokens = self.tokenize_document(query)
		if (len(tokens) == 0):
			return self.documents[:max_hits]
		results = set()
		token_results = self.get_token_results(tokens)
		
		if enable_fuzzy:
			fuzzy_results = self.get_fuzzy_results(tokens) 
		else:
			fuzzy_results = collections.Counter([])

		a = [self.data[x[0]][self.dataKey] for x in (token_results).most_common()]
		b = [self.data[x[0]][self.dataKey] for x in (fuzzy_results).most_common()]
		result = sorted([self.score_result(x, query) for x in set(a).union(set(b))], key=lambda x : x[0], reverse=True)
		if max_hits != None:
			return [x[1] for x in result[:max_hits]]
		else:
			return [x[1] for x in result]

def buildIndex(arr):
	data = {}
	for idx, suggestion in enumerate(arr):
		data[idx] = { 'text': suggestion, 'id' : idx }
	return SearchIndex(data, 'text')

@lru_cache(maxsize=1)
def load_suggestions():
	return {
		'GENE': buildIndex(loaded_gene_names),
		'TRAIT': buildIndex(loaded_trait_names),
		'CELL_TYPE': buildIndex(loaded_cell_types),
	}

@lru_cache(maxsize=1000)
def get_suggestions(term, search_text, max_results):
	suggestions = load_suggestions()
	if not term in suggestions:
		results = []
	else:
		results = suggestions[term].get_results(search_text, max_results, enable_fuzzy=False)
	return results

