
import nltk
import collections
# from sklearn.feature_extraction.text import TfidfVectorizer
nltk.download('punkt')

class SearchIndex:
	def __init__(self, data, dataKey):
		self.inverted_index = {}
		self.ngram_index = {}
		self.data = data
		self.dataKey = dataKey
		for key in self.data.keys():
			self.add_document(key, self.data[key])
	
	def add_document(self, doc_id, doc):
		tokens = self.tokenize_document(doc[self.dataKey])
		for token in tokens:
			if not token in self.inverted_index:
				self.inverted_index[token] = []
			self.inverted_index[token].append(doc_id)

		ngrams = self.generate_ngrams_from_tokens(tokens, 3)
		for ngram in ngrams:
			if not ngram in self.ngram_index:
				self.ngram_index[ngram] = []
			self.ngram_index[ngram].append(doc_id)
	def remove_stopwords_from_tokens(self, tokens):
		return tokens

	def generate_ngrams_from_tokens(self, tokens, n):
		return nltk.ngrams(list(" ".join(tokens)), n)

	def tokenize_document(self, doc):
		tokens = [x.lower() for x in nltk.word_tokenize(doc)]
		return self.remove_stopwords_from_tokens(tokens)

	def rank_results(self, results):
		# TODO: implement TF-IDF based scoring
		return results

	def get_ngram_results(self, ngrams, seen=None):
		results = []
		for ngram in ngrams:
			if ngram in self.ngram_index:
				results += self.ngram_index[ngram]
		return collections.Counter(results)

	def get_token_results(self, tokens, seen=None):
		results = []
		for token in tokens:
			if token in self.inverted_index:
				results += self.inverted_index[token]
		return collections.Counter(results)

	def get_results(self, query, max_hits=100):
		tokens = self.tokenize_document(query)
		ngrams = self.generate_ngrams_from_tokens(tokens, 3)
		results = set()
		token_results = self.get_token_results(tokens)
		ngram_results = self.get_ngram_results(ngrams, seen=token_results)
		print(ngram_results)
		# all_results = token_results.union(ngram_results)
		# return [self.data[x] for x in all_results]
		return []

lines = open("/Users/saliksyed/Desktop/CTD_diseases.csv", "r").readlines()
diseases = {}
for idx, line in enumerate(lines[30:]):
	name = line.split(",")[0]
	diseases[idx] = {
		"name": name,
		"id": idx	
	}

index = SearchIndex(diseases, 'name')

while True:
	query = raw_input("Enter search query:")
	print(index.get_results(query))