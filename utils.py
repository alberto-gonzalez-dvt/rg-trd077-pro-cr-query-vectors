from rank_bm25 import BM25Okapi
#from nltk.tokenize import word_tokenize
from langchain.schema import Document

def order_search_result(search_result, gemini_keywords, num_results): #
  """Use rankbm25 library to order and reduce the results from SEARCH query in BigQuery

  Args:
      search_result (_type_): List of results from SEARCH query.
      gemini_keywords (_type_): list of key words.
      num_results (_type_): number of results with the higher score that we want to keep.

  Returns:
      list: list of Document objects with the best score.
  """  
  documents = []
  for i in search_result:
    documents.append(i['content'])

  # Tokenize the documents
  #tokenized_corpus = [word_tokenize(doc.lower()) for doc in documents]
  #tokenized_corpus=[doc.lower().split() for doc in documents] 
  tokenized_corpus=list(map(lambda doc: doc.lower().split(), documents))
  # Initialize BM25
  bm25 = BM25Okapi(tokenized_corpus)
  # Query
  query = " ".join(gemini_keywords)
  #query=text_to_find
  #tokenized_query = word_tokenize(query.lower())
  tokenized_query = query.lower().split()
  # Retrieve BM25 results
  bm25_scores = bm25.get_scores(tokenized_query)
  #bm25_results = bm25.get_top_n(tokenized_query, documents, n=3)
  # Get the top documents based on bm25 scores
  top_idx = bm25_scores.argsort()[::-1]
  # Get 100 best results based on bm25 scores
  drive_docs=[]
  for i in top_idx[:num_results]:
    drive_docs.append(Document(page_content=search_result[i]["content"], 
                               metadata={"file_name":search_result[i]["file_name"], 
                                         "score": float(bm25_scores[i]),
                                         "file_extension": search_result[i]["file_extension"],
                                         "file_url": search_result[i]["file_url"],
                                         'library_id': search_result[i]["library_id"],
                                         'site_id': search_result[i]["site_id"],
                                         'user_id': search_result[i]["user_id"],
                                         'library_name': search_result[i]["library_name"],
                                         'library_upload_date': search_result[i]["library_upload_date"],
                                         'file_size': search_result[i]["file_size"],
                                         'library_url': search_result[i]["library_url"],
                                         'file_creation_date': search_result[i]["file_creation_date"],
                                         'file_modification_date': search_result[i]["file_modification_date"],
                                         'metadata': search_result[i]["metadata"]
                                        }))

  return drive_docs 