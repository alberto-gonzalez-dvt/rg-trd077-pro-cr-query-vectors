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

def get_site_id_of_drive_id(drive_id, cache=True):
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)
  query = f"""
  SELECT site_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE drive_id = '{drive_id}'
  """

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query

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
  
def get_site_id_of_drive_url(drive_url, cache=True):
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)
  query = f"""
  SELECT site_id, drive_id
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE drive_web_url = '{drive_url}'
  """

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query

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
  
def get_drives_ids_of_site_id(site_id,cache):
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)
  
  query = f"""
  SELECT drive_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE site_id = '{site_id}'
  """

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query

    drives_list = []
    for row in query_job:
      drives_list.append(row['drive_id'])
    return drives_list
  
  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")

def get_drives_ids_of_site_url(site_url, cache=True):
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)

  query = f"""
  SELECT site_id,drive_id 
  FROM `rg-trd077-pro.configuration_details.sites_and_drives` 
  WHERE site_web_url = '{site_url}'
  """

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query

    drives_list = []
    for row in query_job:
      drives_list.append({'drive_id':row['drive_id'],'site_id':row['site_id']})
    return drives_list
  
  except Exception as e:
    logging.error(f"Error doing get_site_id_of_drive_id {e}", exc_info=True)
    raise Exception(f"Error doing get_site_id_of_drive_id {e}")

def get_mapping(cache=True):
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)

  query = f"""
  SELECT drive_web_url, site_id, drive_id
  FROM `rg-trd077-pro.configuration_details.sites_and_drives`
  """

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query
    query_result = query_job.result()
    #num_rows = query_result.total_rows

    df = query_job.to_dataframe()
    #convert to dict
    results = df.to_dict(orient="records")
    #make mapping
    mapping={i['drive_web_url']:{'site_id':i['site_id'], 'library_id':i['drive_id']} for i in results}
    return mapping
  
  except Exception as e:
    logging.error(f"Error doing get_mapping {e}", exc_info=True)
    raise Exception(f"Error doing get_mapping {e}")



def make_search_query(drives_to_find, search_column, key_words, files_to_filter=None):
  if len(drives_to_find)==1:
    query_base=f"""
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
      FROM `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}`
      WHERE SEARCH({search_column}, r"{key_words}", analyzer=>'LOG_ANALYZER')
      """
    if files_to_filter:
      query=query_base.replace("WHERE", f"WHERE webUrl IN ({files_to_filter}) AND")
    else:
      query=query_base

  else:
    query_parts=[]
    for i in drives_to_find:
      query_base=f"""
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
        FROM `rg-trd077-pro.{i['site_id']}.{i['drive_id']}`
        WHERE SEARCH({search_column}, r"{key_words}", analyzer=>'LOG_ANALYZER')
      """
      if files_to_filter:
        single_query=query_base.replace("WHERE", f"WHERE webUrl IN ({files_to_filter}) AND")
      else:
        single_query=query_base
  
      query_parts.append(single_query)

    query="\nUNION ALL\n".join(query_parts)

  return query


# EDITAMOS LA FUNCIÓN QUE HACE LA QUERY PARA QUE EN FUNCIÓN DEL TAMAÑO DE LA TABLA USE FUERZA BRUTA O ÍNDICE
def make_vector_search_query(drives_to_find,text_to_find, files_to_filter=None):
  if len(drives_to_find)==1:
    num_rows = bigqueryClient.get_table(f"rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}").num_rows
    query_base=f"""
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
        TABLE `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}`
        , 'embeddings',
        (
          SELECT ml_generate_embedding_result, content AS query
          FROM ML.GENERATE_EMBEDDING(
            MODEL `bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d.embedding_model`,
              (SELECT "{text_to_find}" AS content))
        ),
        top_k => 50, distance_type => 'EUCLIDEAN', options => '{{"fraction_lists_to_search":0.1}}')
      """
    
    if num_rows >= 150000:
      if files_to_filter:
        query=query_base.replace(f"TABLE `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}`", f"(SELECT * FROM `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}` WHERE webUrl IN ({files_to_filter}))")
      else:
        query=query_base
    else:
      if files_to_filter:
        query=query_base.replace('{"fraction_lists_to_search":0.1}','{"use_brute_force":true}')
        query=query.replace(f"TABLE `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}`", f"(SELECT * FROM `rg-trd077-pro.{drives_to_find[0]['site_id']}.{drives_to_find[0]['drive_id']}` WHERE webUrl IN ({files_to_filter}))")
      else:
        query=query_base.replace('{"fraction_lists_to_search":0.1}','{"use_brute_force":true}')

      
  else:
    query_parts=[]
    for i in drives_to_find:
      num_rows = bigqueryClient.get_table(f"rg-trd077-pro.{i['site_id']}.{i['drive_id']}").num_rows
      query_base = f"""
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
            TABLE `rg-trd077-pro.{i['site_id']}.{i['drive_id']}`
            , 'embeddings',
            (
              SELECT ml_generate_embedding_result, content AS query
              FROM ML.GENERATE_EMBEDDING(
                MODEL `bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d.embedding_model`,
                  (SELECT "{text_to_find}" AS content))
            ),
            top_k => 50, distance_type => 'EUCLIDEAN', options => '{{"fraction_lists_to_search":0.1}}')
          """
      if num_rows >= 150000:
        if files_to_filter:
          # increase fraction_list_to_search to look in more groups
          single_query=query_base.replace('{"fraction_lists_to_search":0.1}','{"use_brute_force":true}') #'{"fraction_lists_to_search":0.2}'
          single_query=single_query.replace(f"TABLE `rg-trd077-pro.{i['site_id']}.{i['drive_id']}`", f"(SELECT * FROM `rg-trd077-pro.{i['site_id']}.{i['drive_id']}` WHERE webUrl IN ({files_to_filter}))")
        else:
          single_query=query_base
        query_parts.append(single_query)
      else:
        if files_to_filter:
          single_query=query_base.replace('{"fraction_lists_to_search":0.1}','{"use_brute_force":true}')
          single_query=single_query.replace(f"TABLE `rg-trd077-pro.{i['site_id']}.{i['drive_id']}`", f"(SELECT * FROM `rg-trd077-pro.{i['site_id']}.{i['drive_id']}` WHERE webUrl IN ({files_to_filter}))")
        else:
          single_query=query_base.replace('{"fraction_lists_to_search":0.1}','{"use_brute_force":true}')
        query_parts.append(single_query)

    #Unify queries  
    query="\nUNION ALL\n".join(query_parts)

  return query

def bigquery_search_request(drives_to_find, search_column, key_words, files_to_filter=None, cache=True):
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

  #site_id_bq, drive_id_bq = format_biquery_table(site_id,drive_id)
  # BigQuery Format
  drives_to_find_formatted=[]
  for i in drives_to_find:
    site_id_bq, drive_id_bq = format_biquery_table(i['site_id'], i['drive_id'])
    drives_to_find_formatted.append({'site_id':site_id_bq, 'drive_id':drive_id_bq})

  # Wrap keywords with rare characters in backticks
  delimiters=[
    "[", "]", "<", ">", "(", ")", "{", "}", "|", "!", ";", ",", "'", '"', "*", "&",
    "?", "+", "/", ":", "=", "@", ".", "-", "$", "%", "\\", "_", "\n", "\r", "\s", "\t",
    "%21", "%26", "%2526", "%3B", "%3b", "%7C", "%7c", "%20", "%2B", "%2b", "%3D", "%3d",
    "%2520", "%5D", "%5d", "%5B", "%5b", "%3A", "%3a", "%0A", "%0a", "%2C", "%2c", "%28", "%29"
  ]
  key_words = [
    f"`{word}`" if any(delim in word for delim in delimiters) else word
    for word in key_words
  ] 
  #key_words = ["`"+i+"`" if "-" in i else i for i in key_word]
  #key_words = ["`"+i+"`" if "'" in i else i for i in key_words]
  key_words = " ".join(key_words)

  query=make_search_query(drives_to_find_formatted,search_column, key_words, files_to_filter)
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query
    query_result = query_job.result()
    num_rows = query_result.total_rows

    if num_rows < 250:
      nears_list=[]
      for row in query_job:
        output_object = {}

        output_object['content'] = row['text']
        output_object['file_extension'] = row['sp_file_extension']
        output_object['file_url'] = row['webUrl']
        output_object['file_name'] = row['file_name']
        #output_object['library_id'] = drive_id
        #output_object['site_id'] = site_id
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
        | {"metadata":{'source': row['webUrl']}} #"library_id": "TABLE", "site_id": "TABLE", 
        for row in df.to_dict(orient="records")
      ]

    return nears_list
  except Exception as e:
    print((f"Error generic on query {e}"))
    raise Exception(f"Error generic on query {e}")

def bigquery_vector_request(drives_to_find, text_to_find, files_to_filter=None, cache=True):

  # BigQuery Format
  drives_to_find_formatted=[]
  for i in drives_to_find:
    site_id_bq, drive_id_bq = format_biquery_table(i['site_id'], i['drive_id'])
    drives_to_find_formatted.append({'site_id':site_id_bq, 'drive_id':drive_id_bq})


  query = make_vector_search_query(drives_to_find_formatted,text_to_find, files_to_filter)
  job_config = bigquery.QueryJobConfig(use_query_cache=cache)

  try:
    query_job = bigqueryClient.query(query, job_config=job_config)  # Execute the query

    nears_list = []

    for row in query_job:

      output_object = {}

      output_object['content'] = row['text']
      output_object['score'] = row['distance']
      output_object['file_extension'] = row['sp_file_extension']
      output_object['file_url'] = row['webUrl']
      output_object['file_name'] = row['file_name']
      #output_object['library_id'] = drive_id
      #output_object['site_id'] = site_id
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

      nears_list.append(output_object)
    return nears_list
  except Exception as e:
    print((f"Error generic on query {e}"))
    raise Exception(f"Error generic on query {e}")