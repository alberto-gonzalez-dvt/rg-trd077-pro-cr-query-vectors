# rg-trd077-pro-cr-query-vectors
rg-trd077-pro-cr-query-vectors

## Description
- Los errores capturados en bigquery_functions - query_vectors capturarlos y subirlos controlados
- Poner el modelo en configuration_details en lugar de rg-trd077-pro.bcadf53e_9768_4234_9e07_f706d718f12b__dd4ef53e_f365_4da7_aebb_14a52138466d.embedding_model
- Comprobar tambien los tipos de datos de la entrada
- Metodo que te devuelva todos los chunks de un archivo

## Execute in local
<TO BE DONE>

## Deploy to Cloud Run
```
gcloud run deploy rg-trd077-pro-cr-query-vectors \
--source . \
--project=rg-trd077-pro \
--region=europe-west1 \
--platform managed \
--no-allow-unauthenticated \
--max-instances=20 \
--min-instances=0 \
--cpu=0.5 \
--memory=512Mi \
--timeout=60 \
--concurrency=1 \
--service-account=id-rg-trd077-pro-cloud-functio@rg-trd077-pro.iam.gserviceaccount.com \
--set-env-vars "project_id=rg-trd077-pro"
```
