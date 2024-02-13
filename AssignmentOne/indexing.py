#-------------------------------------------------------------------------
# AUTHOR: Ryan Larson
# FILENAME: indexing.py
# SPECIFICATION: Reading "documents" from CSV file named "collection.csv" and creating a tf-idf matrix
# FOR: CS 4250- Assignment #1
# TIME SPENT: START: 9:03 PM
#             END  : 10:42
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays
def simple_stem(word):
    if word.endswith("s") :
        return word[:-1]
    else :
        return word
def create_stem_mapping(words):
    stemming = {}
    for word in words:
        stemming[word] = simple_stem(word)
    return stemming

#Importing some Python libraries
import csv
from collections import Counter
import math

documents = []

#Reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
  reader = csv.reader(csvfile)
  for i, row in enumerate(reader):
         if i > 0:  # skipping the header
            documents.append (row[0])

#Conducting stopword removal. Hint: use a set to define your stopwords.
#--> add your Python code here

stopWords = {"and","she","her","their","i","they"}
filtered_documents = []

for document in documents:
    words = document.split()
    filtered_words = [word for word in words if word.lower() not in stopWords]
    filtered_document = ' '.join(filtered_words)
    filtered_documents.append(filtered_document)
#Conducting stemming. Hint: use a dictionary to map word variations to their stem.
#--> add your Python code here
stemming = create_stem_mapping(set(' '.join(filtered_documents).split()))
stemmed_documents = []

for document in filtered_documents:
    stemmed_words = [stemming[word] for word in document.split()]
    stemmed_document = ' '.join(stemmed_words)
    stemmed_documents.append(stemmed_document)
#Identifying the index terms.
terms = set(' '.join(stemmed_documents).split())

#Building the document-term matrix by using the tf-idf weights.
docTermMatrix = []

for document in stemmed_documents:
    term_freq = Counter(document.split())
    #print(term_freq)
    tfidf_weights = [
        term_freq[term] * math.log10(len(stemmed_documents) / sum(1 for doc in stemmed_documents if term in doc.split()))
        for term in terms]
    docTermMatrix.append(tfidf_weights)


#Printing the document-term matrix.
max_term_length = max(len(term) for term in terms)
term_header = "\t".join(term.ljust(max_term_length) for term in terms)
print(f"{'Term':<{max_term_length}}\t{term_header}")

for i, row in enumerate(docTermMatrix):
    document_name = f"Document {i + 1}:"
    print(f"{document_name:<{max_term_length}}\t{'\t'.join(f'{value:.4f}' for value in row)}")