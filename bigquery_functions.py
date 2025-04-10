import os
from datetime import datetime
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

    #TODO: Tirar error si hay 0 rows o mas de 1
    df = query_job.to_dataframe()
    if len(df)==0 or len(df)>1:
      print("ERROR AL INDICAR EL NOMBRE DEL DRIVE URL")
      logging.error(f"Error in drive url name:", exc_info=True)
      raise Exception(f"Error in drive url name:") 
    else:
      #Return site_id of first row
      for row in query_job:
        return row['site_id']

  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")
  
def get_site_id_of_drive_url(drive_url):

  query = f"""
  SELECT site_id, drive_id
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE drive_web_url = '{drive_url}'
  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    #TODO: Tirar error si hay 0 rows o mas de 1
    df = query_job.to_dataframe()
    if len(df)==0 or len(df)>1:
      print("ERROR AL INDICAR EL NOMBRE DEL DRIVE URL")
      logging.error(f"Error in drive url name:", exc_info=True)
      raise Exception(f"Error in drive url name")
    else:
      #Return site_id of first row
      for row in query_job:
        return row['site_id'], row['drive_id']


  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_url {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_url {e}")
  
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

def get_drives_ids_of_site_url(site_url):

  query = f"""
  SELECT site_id,drive_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE site_web_url = '{site_url}'
  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    drives_list = []
    for row in query_job:
      drives_list.append({'drive_id':row['drive_id'],'site_id':row['site_id']})
    return drives_list
  
  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")




def bigquery_vector_request(site_id, drive_id, text_to_find):
    
  site_id_bq, drive_id_bq = format_biquery_table(site_id,drive_id)

  query = f"""
  SELECT 
    query.query, 
    base.text, 
    base.sp_file_extension, 
    base.webUrl, 
    base.file_name, 
    base.drive_name,
    base.trace_timestamp,
    base.sp_file_size,
    base.file_id,
    base.drive_path,
    base.sp_file_created_date_time,
    base.sp_file_last_modified_date_time,
    distance
  FROM VECTOR_SEARCH(
    (
      SELECT * FROM `{project_id}.{site_id_bq}.{drive_id_bq}`
     WHERE EXISTS (
        SELECT 1 FROM UNNEST(embeddings) AS e WHERE e <> 0
      )
      ), 'embeddings', 
    (
      SELECT ml_generate_embedding_result, content AS query
      FROM ML.GENERATE_EMBEDDING(
        MODEL `bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d.embedding_model`,
          (SELECT " {text_to_find}" AS content))
    ),
    top_k => 50, distance_type => 'COSINE') 
  """
  #, options => '{{"use_brute_force":true}}'


  try:
    query_job = bigqueryClient.query(query)  # Execute the query

    nears_list = []
    for row in query_job:
      
      output_object = {}
      
      output_object['content'] = row['text']
      output_object['score'] = row['distance']
      output_object['file_extension'] = row['sp_file_extension']
      output_object['file_url'] = row['webUrl']
      output_object['file_name'] = row['file_name']
      output_object['library_id'] = drive_id
      output_object['site_id'] = site_id
      output_object['library_name'] = row['drive_name']
      output_object['library_upload_date']= row['trace_timestamp']
      output_object['file_size'] = row['sp_file_size']
      output_object[''] = row['file_id']
      output_object['library_url'] = row['drive_path']
      output_object['file_creation_date'] = row['sp_file_created_date_time']
      output_object['file_modification_date'] = row['sp_file_last_modified_date_time']
  
      source = {
        'source': row['webUrl']
      }
      output_object['metadata'] = source



      #library_category
      #file_num_pages
      #chunk_language
      #chunk_key_phrases
      #file_author
      #file_title
      #chunk_entities


      

      nears_list.append(output_object)
    return nears_list
  except Exception as e:
    logging.error(f"Error doing request {e}", exc_info=True)
    print((f"Error generic on query {e}"))
    raise Exception(f"Error generic on query {e}")
  
def bigquery_search_request(site_id, drive_id, search_column,key_word):
  """Use SEARCH function from BigQuery, given a table and some key words.

  Args:
      site_id (string): ID of site from SharePoint
      drive_id (string): ID of drive from SharePoint
      search_column (string): column to search
      key_word (list): key words to search in search_column

  Raises:
      Exception: raise an exception if query fails

  Returns:
      list: list of chunks that contains the key words
  """
  
  
  site_id_bq, drive_id_bq = format_biquery_table(site_id,drive_id)
  
  key_words = ["`"+i+"`" if "-" in i else i for i in key_word]
  key_words = ["`"+i+"`" if "'" in i else i for i in key_words]
  key_words = " ".join(key_words)

  query=f"""
     SELECT 
      file_name, 
      text, 
      sp_file_extension, 
      webUrl, 
      drive_name,
      trace_timestamp, 
      sp_file_size, 
      file_id, 
      drive_path,
      sp_file_created_date_time,
      sp_file_last_modified_date_time
     FROM `{project_id}.{site_id_bq}.{drive_id_bq}`
     WHERE SEARCH({search_column}, r"{key_words}", analyzer=>'LOG_ANALYZER')

  """

  try:
    query_job = bigqueryClient.query(query)  # Execute the query
    query_result = query_job.result()
    num_rows = query_result.total_rows

    if num_rows < 250:
      nears_list=[]
      for row in query_job:
        output_object = {}

        output_object['content'] = row['text']
        #output_object['score'] = row['distance']
        output_object['file_extension'] = row['sp_file_extension']
        output_object['file_url'] = row['webUrl']
        output_object['file_name'] = row['file_name']
        output_object['library_id'] = drive_id
        output_object['site_id'] = site_id
        output_object['library_name'] = row['drive_name']
        output_object['library_upload_date']= row['trace_timestamp']
        output_object['file_size'] = row['sp_file_size']
        output_object[''] = row['file_id']
        output_object['library_url'] = row['drive_path']
        output_object['file_creation_date'] = row['sp_file_created_date_time']
        output_object['file_modification_date'] = row['sp_file_last_modified_date_time']
        
        source = {'source': row['webUrl']}
        output_object['metadata'] = source

        nears_list.append(output_object)
    else:
      # Fetch results as an Arrow table
      table = query_job.to_arrow()  
      # Convert to Pandas DataFrame 
      df = table.to_pandas()
      #df = query_job.to_dataframe()
      df = df.map(lambda x: x.isoformat() if isinstance(x, datetime) else x) # Convert TimeStamps to string
      #convert to dict
      nears_list = df.to_dict(orient="records")
      # Define a mapping of original column names to new names
      column_mapping = {
        "text": "content",
        "sp_file_extension": "file_extension",
        "webUrl":"file_url", 
        "drive_name":"library_name", 
        "trace_timestamp":"library_upload_date", 
        "sp_file_size":"file_size", 
        "drive_path":"library_url", 
        "sp_file_created_date_time":"file_creation_date",
        "sp_file_last_modified_date_time":"file_modification_date", 
        # Add more mappings as needed
      }

      # Convert DataFrame to a list of dictionaries with renamed keys
      nears_list = [
        {column_mapping.get(k, k): v for k, v in row.items()}  # Rename keys
        | {"library_id": drive_id, "site_id": site_id, "metadata":{'source': row['webUrl']}}
        for row in df.to_dict(orient="records")
      ]

    return nears_list
  except Exception as e:
    print((f"Error generic on query {e}"))
    raise Exception(f"Error generic on query {e}")