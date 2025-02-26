from bigquery_functions import bigquery_vector_request, get_site_id_of_drive_id, get_site_id_of_drive_url, get_drives_ids_of_site_id, get_drives_ids_of_site_url
from gemini import generate_answer
from langchain_google_community.vertex_rank import VertexAIRank
from langchain.schema import Document

from flask import Flask, request, jsonify

app = Flask(__name__)

#Import to log all the trace
import traceback

@app.post("/")
def analyze_sharepoint():
  
  body_json = request.get_json(silent=True)

  #Check mandatory parameters
  mandatory_parameters_list = ['action', 'request_id', 'user_id', 'timestamp', 'query']
  for parameter_name in mandatory_parameters_list:
    if parameter_name not in body_json:
       return jsonify({
         "error": "Bad Request",
         "message": f"Missing required parameter: '{parameter_name}'"
        }), 400
  if (body_json['action'] != 'search'):
    return jsonify({
      "error": "Bad Request",
      "message": "Action forbidden"
        }), 400
  
  #Text to find
  text_to_find = body_json['query']
  # replace double quotes to avoid problems in query definition
  text_to_find = text_to_find.replace('"','')

  
  #Main execution
  try:

    
    #List of places to find in
    drives_to_find = []
    
    #TODO: Unificar para que todos los metodos devuelvan tanto site_id como drive_id para que sea más directo el append y en todos los casos igual

    # PROCESS A DRIVE_ID
    if 'selected_libraries_ids' in body_json:
      for drive_id in body_json['selected_libraries_ids']:
        # Get site of library
        try:
          site_id = get_site_id_of_drive_id(drive_id)
        except:
          print("ERROR IN DRIVE_ID")
          return "ERROR IN DRIVE_ID",500
        drives_to_find.append({ 'site_id':site_id, 'drive_id': drive_id })

    # PROCESS A DRIVE_URL
    if 'selected_libraries_urls' in body_json:
      for drive_url in body_json['selected_libraries_urls']:
        # Get site_id and drive_id
        try:
          site_id, drive_id = get_site_id_of_drive_url(drive_url)
        except:
          print("ERROR IN DRIVE_URL")
          return "ERROR IN DRIVE_URL",500
        drives_to_find.append({ 'site_id':site_id, 'drive_id': drive_id })

    ###

    # PROCESS LIST OF SITES_IDS
    if 'selected_sites_ids' in body_json:
      for site_id in body_json['selected_sites_ids']:
        # Get list of drives
        try:
          drives_list = get_drives_ids_of_site_id(site_id)
        except:
          print("ERROR IN SITE_ID")
          return "ERROR IN SITE_ID",500

        #Add each drive
        for drive_id in drives_list:
          drives_to_find.append({ 'site_id':site_id, 'drive_id': drive_id })

    # PROCESS LIST OF SITES
    if 'selected_sites_urls' in body_json:
      for site_url in body_json['selected_sites_urls']:
        # Get list of drives
        try:
          drives_list = get_drives_ids_of_site_url(site_url)
        except:
          print("ERROR IN SITE_URL")
          return "ERROR IN SITE_URL",500

        #Add each drive
        for drive_item in drives_list:
          drives_to_find.append({ 'site_id':drive_item['site_id'], 'drive_id': drive_item['drive_id'] })
    
    # FINAL OUTPUT
    final_output = {}
    
    #FIND FOR EACH DRIVE
    seach_answer = []
    for drive_obj in drives_to_find:
      drive_results = bigquery_vector_request(drive_obj['site_id'], drive_obj['drive_id'],text_to_find)

      #Add user_id to all items
      for drive_result in drive_results:
        drive_result['user_id'] = body_json['user_id']

      #seach_answer.append(drive_results)
      seach_answer.extend(drive_results)
    
    # Instantiate the VertexAIReranker with the SDK manager
    reranker = VertexAIRank(project_id="rg-trd077-pro",
                            location_id="europe-west1",
                            model="semantic-ranker-512-003",   # available models --> https://cloud.google.com/generative-ai-app-builder/docs/ranking#rank_or_rerank_a_set_of_records_according_to_a_query
                            ranking_config="default_ranking_config",
                            title_field="source",
                            top_n=5,
                          )
    drive_docs=[]
    # Convert dict to Document
    for i in seach_answer:
      drive_docs.append(Document(page_content=i["content"], metadata={"source":i["file_name"]}))

    # rerank de contextos
    reranked_docs = reranker._rerank_documents(query=text_to_find, documents=drive_docs)
    context_output=[]
    for i in reranked_docs:
      context_output.append({
                             'content': i.page_content, 
                             'score': seach_answer[int(i.metadata['id'])]['score'], 
                             'file_extension': seach_answer[int(i.metadata['id'])]['file_extension'],
                             'file_url': seach_answer[int(i.metadata['id'])]['file_url'],
                             "file_name": seach_answer[int(i.metadata['id'])]['file_name'], 
                             'library_id': seach_answer[int(i.metadata['id'])]['library_id'],
                             'site_id': seach_answer[int(i.metadata['id'])]['site_id'],
                             'user_id': seach_answer[int(i.metadata['id'])]['user_id'],
                             'library_name': seach_answer[int(i.metadata['id'])]['library_name'],
                             'library_upload_date': seach_answer[int(i.metadata['id'])]['library_upload_date'],
                             'file_size': seach_answer[int(i.metadata['id'])]['file_size'],
                             'library_url': seach_answer[int(i.metadata['id'])]['library_url'],
                             'file_creation_date': seach_answer[int(i.metadata['id'])]['file_creation_date'],
                             'file_modification_date': seach_answer[int(i.metadata['id'])]['file_modification_date'],
                             'metadata': seach_answer[int(i.metadata['id'])]['metadata']
                             })

    

    #If user wants to generate semantic answer must select generate_semantic_answer
    semantic_answer = ''
    if 'generate_semantic_answer' in body_json and body_json['generate_semantic_answer'] == True:
      #Context are the texts of the elements found
      contexts = []


      for drive_result in reranked_docs:
        contexts.append(drive_result.page_content)

      final_output['semantic_answer'] = generate_answer(body_json['query'], contexts[:5])#Le pasamos solo los 5 con mejor score

    final_output['search_answer'] = context_output

    return jsonify(final_output),200

  except:
    error_trace = traceback.format_exc()
    print(error_trace)
    #Errors are controlled in function so return the text
    return jsonify({
            "error": "Database Error",
            "message": "An error occurred while accessing the database."
    }), 500

if __name__ == "__main__":
    app.run(host="localhost", port=8080, debug=False)