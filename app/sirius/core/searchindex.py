
import nltk
import collections
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
import fuzzyset

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
		documents = []
		for doc_id in self.data.keys():
			self.add_document(doc_id, self.data[doc_id])
			documents.append(self.data[doc_id][dataKey])
		
		tfidf = TfidfVectorizer(tokenizer=self.tokenize_document, stop_words='english')
		self.tfs = tfidf.fit_transform(documents)
	
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
			final_tokens += [x[1] for x in self.fuzzyset.get(token)]
		return self.get_token_results(final_tokens)

	def score_result(self, result, query):
		return (1.0, result)

	def get_results(self, query, max_hits=100):
		tokens = self.tokenize_document(query)
		results = set()
		token_results = self.get_token_results(tokens)
		fuzzy_results = self.get_fuzzy_results(tokens)
		# get tf-idf vector for query
		a = [self.data[x[0]][self.dataKey] for x in (token_results).most_common()]
		b = [self.data[x[0]][self.dataKey] for x in (fuzzy_results).most_common()]
		return sorted([self.score_result(x, query) for x in set(a).union(set(b))], key=lambda x : x[0])


lines = open("/CTD_diseases.csv", "r").readlines()
diseases = {}
for idx, line in enumerate(lines[30:]):
	name = line.split(",")[0]
	diseases[idx] = {
		"name": name,
		"id": idx	
	}

index = SearchIndex(diseases, 'name')

while True:
	query = input("Enter search query:")
	print(index.get_results(query))