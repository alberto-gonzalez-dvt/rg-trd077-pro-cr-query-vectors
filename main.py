from bigquery_functions import get_site_id_of_drive_id, get_site_id_of_drive_url, get_drives_ids_of_site_id, get_drives_ids_of_site_url
from gemini import generate_answer, generate_keywords_and_weights
from utils import do_search_type_text, do_search_type_vector 

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
    search_column='text'

    # Check for type of search in parameters
    if 'search_type'in body_json:
      search_type=body_json['search_type']

    else:
      search_type="default"


    if search_type=='text':
      print("TEXT MODE")
      search_context = do_search_type_text(drives_to_find=drives_to_find, 
                                           text_to_find=text_to_find, 
                                           search_column=search_column, 
                                           user_id=body_json['user_id'])
      final_context_ordered_uniques=search_context
      if final_context_ordered_uniques==[]:
        return jsonify({
            "error": "SEARCH Error",
            "message": "No matches found while doing SEARCH. Try using another search_type."
        }), 500
        
    
    elif search_type=='vector':
      print("VECTOR MODE")
      vector_search_context= do_search_type_vector(drives_to_find, text_to_find, body_json['user_id'])
      final_context_ordered_uniques=vector_search_context
    
    elif search_type=='hybrid':
      print("HYBRID MODE")
      # assing weights
      search_weight=0.5
      vector_search_weight=0.5
      # Do SEARCH
      search_context = do_search_type_text(drives_to_find=drives_to_find, 
                                           text_to_find=text_to_find, 
                                           search_column=search_column, 
                                           user_id=body_json['user_id'])
      search_context_weighted = [{**item, "score": item["score"] * search_weight} for item in search_context]
      # Do VECTOR_SEARCH
      vector_search_context=do_search_type_vector(drives_to_find, text_to_find, body_json['user_id'])
      vector_search_context_weighted = [{**item, "score": item["score"] * vector_search_weight} for item in vector_search_context]
      # Sum contexts from SEARCH and VECTOR_SEARCH. Order them according to scores(already weighted)
      final_context=search_context_weighted+vector_search_context_weighted 
      #order contexts using relevance score from Ranking model
      final_context_ordered = sorted(final_context, key=lambda x: x['score'], reverse=True) #reverse=True --> descending order
      # Eliminate posible duplicates based on content field. Keep first ocurrence, i.e., the one with highest score
      uniques = {item["content"]: item for item in reversed(final_context_ordered)}
      # Convert back to list
      final_context_ordered_uniques = list(reversed(uniques.values()))
    
    else: # Default mode, we need to infer keywords and weights with Gemini
      print("DEFAULT MODE")
      gemini_response=generate_keywords_and_weights(text_to_find)
      search_weight=gemini_response['keyword_weight']
      vector_search_weight=gemini_response['semantic_weight']
      key_words=gemini_response['key_words']
      print(f"Peso semántico: {vector_search_weight}")
      print(f"Peso léxico: {search_weight}")
      print(f"Keywords: {key_words}")

      if vector_search_weight > 0.6:  # Strong semantic sense
        vector_search_context=do_search_type_vector(drives_to_find, text_to_find, body_json['user_id'])
        final_context_ordered_uniques=vector_search_context
      else:  # Low semantic sense, so we do hybrid search
        # Do SEARCH
        search_context = do_search_type_text(drives_to_find=drives_to_find, 
                                           text_to_find=text_to_find, 
                                           search_column=search_column, 
                                           user_id=body_json['user_id'], 
                                           key_words_list=key_words)
        search_context_weighted = [{**item, "score": item["score"] * search_weight} for item in search_context]
        # Do VECTOR_SEARCH
        vector_search_context=do_search_type_vector(drives_to_find, text_to_find, body_json['user_id'])
        vector_search_context_weighted = [{**item, "score": item["score"] * vector_search_weight} for item in vector_search_context]
        # Sum contexts from SEARCH and VECTOR_SEARCH. Order them according to scores(already weighted)
        final_context=search_context_weighted+vector_search_context_weighted 
        #order contexts using relevance score from Ranking model
        final_context_ordered = sorted(final_context, key=lambda x: x['score'], reverse=True) #reverse=True --> descending order
        # Eliminate posible duplicates based on content field. Keep first ocurrence, i.e., the one with highest score
        uniques = {item["content"]: item for item in reversed(final_context_ordered)}
        # Convert back to list
        final_context_ordered_uniques = list(reversed(uniques.values()))
        
    #If user wants to generate semantic answer must select generate_semantic_answer
    if 'generate_semantic_answer' in body_json and body_json['generate_semantic_answer'] == True:
      #Context are the texts of the elements found
      contexts = []


      for drive_result in final_context_ordered_uniques:
        contexts.append({"file_name": drive_result['file_name'],"content":drive_result['content']})

      final_output['semantic_answer'] = generate_answer(body_json['query'], 
                                                        contexts[:10])

    final_output['search_answer'] = final_context_ordered_uniques[:10] 

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