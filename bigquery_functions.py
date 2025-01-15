import os
from google.cloud import bigquery

#Initialize bigquery
bigqueryClient = bigquery.Client(location="europe-west1")

#Import  logging
import logging
import google.cloud.logging

#Initilize client_logging
client_logging = google.cloud.logging.Client()
client_logging.setup_logging()

project_id = os.environ.get('project_id')

def format_biquery_table(site_id,drive_id):
    
  site_id_bq = site_id.replace('trsa.sharepoint.com,','').replace(',','__').replace('-','_')
  drive_id_bq = drive_id.replace('!','__')

  return site_id_bq, drive_id_bq

def get_site_id_of_drive_id(drive_id):

  query = f"""
  SELECT site_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE drive_id = '{drive_id}'
  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    #Return site_id of first row
    for row in query_job:
      return row['site_id']

  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")

def get_drives_ids_of_site_id(site_id):
  
  query = f"""
  SELECT drive_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE site_id = '{site_id}'
  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    drives_list = []
    for row in query_job:
      drives_list.append(row['drive_id'])
    return drives_list
  
  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")


def bigquery_vector_request(site_id, drive_id, text_to_find):
    
  site_id_bq, drive_id_bq = format_biquery_table(site_id,drive_id)

      
  query = f"""
  SELECT query.query, base.text, base.sp_file_extension, distance
  FROM VECTOR_SEARCH(
    TABLE `{project_id}.{site_id_bq}.{drive_id_bq}`, 'embeddings',
    (
      SELECT ml_generate_embedding_result, content AS query
      FROM ML.GENERATE_EMBEDDING(
        MODEL `bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d.embedding_model`,
          (SELECT '{text_to_find}' AS content))
    ),
    top_k => 5)
  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    nears_list = []
    for row in query_job:
      
      output_object = {}
      
      output_object['key'] = ''
      output_object['text'] = row['text']
      output_object['score'] = row['distance']
      output_object['file_extension'] = row['sp_file_extension']
      output_object['library_id'] = drive_id
      output_object['site_id'] = site_id
      

      nears_list.append(output_object)
    return nears_list
  except Exception as e:
    logging.error(f"Error doing request {e}", exc_info=True)
    print((f"Error generic on query {e}"))
    raise Exception(f"Error generic on query {e}")