from bigquery_functions import bigquery_vector_request, get_site_id_of_drive_id, get_drives_ids_of_site_id

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


  #Main execution
  try:

    #List of places to find in
    drives_to_find = []

    # PROCESS LIST OF DRIVE
    if 'selected_libraries' in body_json:
      for drive_id in body_json['selected_libraries']:
        # Get site of library
        site_id = get_site_id_of_drive_id(drive_id)

        drives_to_find.append({ 'site_id':site_id, 'drive_id': drive_id })

    # PROCESS LIST OF SITES
    if 'selected_sites' in body_json:
      for site_id in body_json['selected_sites']:
        # Get list of drives
        drives_list = get_drives_ids_of_site_id(site_id)

        #Add each drive
        for drive_id in drives_list:
          drives_to_find.append({ 'site_id':site_id, 'drive_id': drive_id })


    #FIND FOR EACH DRIVE
    vector_results = []
    for drive_obj in drives_to_find:
      drive_results = bigquery_vector_request(drive_obj['site_id'], drive_obj['drive_id'],text_to_find)
      vector_results.append(drive_results)

    return jsonify(vector_results),200

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