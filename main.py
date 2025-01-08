from bigquery_functions import query_vectors

from flask import Flask, request, jsonify

app = Flask(__name__)

#Import to log all the trace
import traceback

@app.post("/")
def analyze_sharepoint():
  
  body_json = request.get_json(silent=True)

  #Check mandatory parameters
  mandatory_parameters_list = ['action', 'request_id', 'user_id', 'timestamp', 'query', 'selected_libraries']
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

  #TODO: Anadir busqueda si no se especifica el site_id

  #Main execution
  try:

    site_id = 'bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d'
    drive_id = body_json['selected_libraries'][0]
    text_to_find = body_json['query']
    
    vector_results = query_vectors(site_id, drive_id,text_to_find)

    return jsonify(vector_results),200

  except:
    error_trace = traceback.format_exc()
    print(error_trace)
    #Errors are controlled in function so return the text
    return jsonify({
            "error": "Database Error",
            "message": "An error occurred while accessing the database."
    }), 500
     
    
  return 'OK', 200

if __name__ == "__main__":
    app.run(host="localhost", port=8080, debug=False)