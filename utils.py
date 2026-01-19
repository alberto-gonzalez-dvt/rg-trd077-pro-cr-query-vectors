from rank_bm25 import BM25Okapi
from langchain_core.documents import Document
from gemini import generate_KeyWords
from bigquery_functions import bigquery_search_request, bigquery_vector_request
from langchain_google_community.vertex_rank import VertexAIRank
import time

# Instantiate the VertexAIReranker with the SDK manager
#available models --> https://cloud.google.com/generative-ai-app-builder/docs/ranking#rank_or_rerank_a_set_of_records_according_to_a_query
reranker = VertexAIRank(project_id="rg-trd077-pro",
                          location_id="europe-west1",
                          model="semantic-ranker-default-004", #"semantic-ranker-512-003", #"semantic-ranker-default-004"
                          ranking_config="default_ranking_config",
                          title_field="source",
                          top_n=30,
                        )

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
  tokenized_corpus=list(map(lambda doc: doc.lower().split(), documents))
  # Initialize BM25
  bm25 = BM25Okapi(tokenized_corpus)
  # Query
  query = " ".join(gemini_keywords)
  tokenized_query = query.lower().split()
  # Retrieve BM25 results
  bm25_scores = bm25.get_scores(tokenized_query)
  # Get the top documents based on bm25 scores
  top_idx = bm25_scores.argsort()[::-1]
  # Get `num_results` best results based on bm25 scores
  drive_docs=[]
  for i in top_idx[:num_results]:
    drive_docs.append(Document(page_content=search_result[i]["content"], 
                               metadata={"file_name":search_result[i]["file_name"], 
                                         "score": float(bm25_scores[i]),
                                         "file_extension": search_result[i]["file_extension"],
                                         "file_url": search_result[i]["file_url"],
                                         'drive_id': search_result[i]["drive_id"],
                                         #'site_id': search_result[i]["site_id"],
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


def do_search_type_text(drives_to_find, text_to_find, search_column, user_id, key_words_list=None, files_to_filter=None, cache=True):
  search_context=[]
  key_word_search=[]
  # First, generate Key Words if not passed
  if key_words_list==None:
    gemini_response = generate_KeyWords(text_to_find)
    key_words=gemini_response['key_words']
  else:
    key_words=key_words_list
  
  #print(f"Palabras clave: {key_words}")
  # After key words, make a SEARCH query to find chunks containing these key words
  key_word_search=bigquery_search_request(drives_to_find, search_column, key_words, files_to_filter, cache)
  
  #Add user_id to all items
  for drive_result in key_word_search:
      drive_result['user_id'] = user_id
      
  #print(f"NÚMERO DE RESULTADOS DEL SEARCH: {len(key_word_search)}")
  # Eliminate posible duplicates based on content field
  uniques = {item["content"]: item for item in key_word_search}
  # Convert back to list
  key_word_search = list(uniques.values())
  if len(key_word_search)!=0:
    if len(key_word_search)>50:
      # Use rankbm25 to associate scores and order SEARCH results. Return the best 100 results
      search_results_ordered = order_search_result(search_result=key_word_search, gemini_keywords=key_words, 
                                                     num_results=50)
    else:
      search_results_ordered = [Document(page_content=i["content"],
                                             metadata={"file_name":i["file_name"],
                                                      "file_extension": i["file_extension"],
                                                      "file_url": i["file_url"],
                                                      'drive_id': i["drive_id"],
                                                      #'site_id': i["site_id"],
                                                      'user_id': i["user_id"],
                                                      'library_name': i["library_name"],
                                                      'library_upload_date': i["library_upload_date"],
                                                      'file_size': i["file_size"],
                                                      'library_url': i["library_url"],
                                                      'file_creation_date': i["file_creation_date"],
                                                      'file_modification_date': i["file_modification_date"],
                                                      'metadata': i["metadata"]}) 
                                    for i in key_word_search]

    # Re-rank best 50 results
    reranked_docs = reranker._rerank_documents(query=text_to_find, documents=search_results_ordered)

    # make context
    for i in reranked_docs:
      #if len(key_word_search)>50:
      search_context.append({
                             'content': i.page_content,  
                             'score': i.metadata['relevance_score'],
                             'file_extension': search_results_ordered[int(i.metadata['id'])].metadata['file_extension'],
                             'file_url': search_results_ordered[int(i.metadata['id'])].metadata['file_url'],
                             "file_name": search_results_ordered[int(i.metadata['id'])].metadata['file_name'], 
                             'drive_id': search_results_ordered[int(i.metadata['id'])].metadata['drive_id'],
                             #'site_id': search_results_ordered[int(i.metadata['id'])].metadata['site_id'],
                             'user_id': search_results_ordered[int(i.metadata['id'])].metadata['user_id'],
                             'library_name': search_results_ordered[int(i.metadata['id'])].metadata['library_name'],
                             'library_upload_date': search_results_ordered[int(i.metadata['id'])].metadata['library_upload_date'],
                             'file_size': search_results_ordered[int(i.metadata['id'])].metadata['file_size'],
                             'library_url': search_results_ordered[int(i.metadata['id'])].metadata['library_url'],
                             'file_creation_date': search_results_ordered[int(i.metadata['id'])].metadata['file_creation_date'],
                             'file_modification_date': search_results_ordered[int(i.metadata['id'])].metadata['file_modification_date'],
                             'metadata': search_results_ordered[int(i.metadata['id'])].metadata['metadata'] 
                        })
  
  return search_context


def do_search_type_vector(drives_to_find, text_to_find, user_id, files_to_filter=None, cache=True):
  #FIND FOR EACH DRIVE
  #seach_answer = []
  seach_answer = bigquery_vector_request(drives_to_find, text_to_find, files_to_filter, cache)
  #Add user_id to all items
  for drive_result in seach_answer:
      drive_result['user_id'] = user_id
  
    
  # Order results 
  seach_answer_ordered = sorted(seach_answer, key=lambda x: x["score"], reverse=False) #reverse=False->small distances are better
  #print(f"NÚMERO DE RESULTADOS DEL VECTOR_SEARCH: {len(seach_answer_ordered)}")
  # Eliminate posible duplicates based on content field. Keep first ocurrence, i.e., the one with highest score
  uniques = {item["content"]: item for item in reversed(seach_answer_ordered)}
  # Convert back to list
  seach_answer_ordered = list(reversed(uniques.values()))
    
  

  drive_docs=[]
  # Convert dict to Document
  for i in seach_answer_ordered[:100]: # Keep only the best 100 results from all drives, to keep it only 1 request to Re-Rank
    drive_docs.append(Document(page_content=i["content"], metadata={"source":i["file_name"]}))

  # rerank de contextos
  reranked_docs = reranker._rerank_documents(query=text_to_find, documents=drive_docs)

  vector_search_context=[]
  for i in reranked_docs:
    vector_search_context.append({
                             'content': i.page_content, 
                             'score': i.metadata['relevance_score'], 
                             'file_extension': seach_answer_ordered[int(i.metadata['id'])]['file_extension'],
                             'file_url': seach_answer_ordered[int(i.metadata['id'])]['file_url'],
                             "file_name": seach_answer_ordered[int(i.metadata['id'])]['file_name'], 
                             'drive_id': seach_answer_ordered[int(i.metadata['id'])]['drive_id'],
                             #'site_id': seach_answer_ordered[int(i.metadata['id'])]['site_id'],
                             'user_id': seach_answer_ordered[int(i.metadata['id'])]['user_id'],
                             'library_name': seach_answer_ordered[int(i.metadata['id'])]['library_name'],
                             'library_upload_date': seach_answer_ordered[int(i.metadata['id'])]['library_upload_date'],
                             'file_size': seach_answer_ordered[int(i.metadata['id'])]['file_size'],
                             'library_url': seach_answer_ordered[int(i.metadata['id'])]['library_url'],
                             'file_creation_date': seach_answer_ordered[int(i.metadata['id'])]['file_creation_date'],
                             'file_modification_date': seach_answer_ordered[int(i.metadata['id'])]['file_modification_date'],
                             'metadata': seach_answer_ordered[int(i.metadata['id'])]['metadata']
                            })
    
  return vector_search_context