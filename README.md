# data_engineering_project
The data pipeline consists of three main parts:

Extracting data from Kaggle rankings page.

Transforming the data and loading it into Google Cloud Storage(GCS).

Loading the data into BigQuery table.

The DAG includes the following tasks:

load_kaggle2_task: task retrieve the HTML content of the Kaggle rankings page.
get_json_task: task extract the necessary data from the HTML content and convert it to a JSON format.
upload_to_gcs_task: task upload the JSON data to a specified GCS bucket.
etl_gcs_to_bq_task: task load the JSON data from GCS to a specified BigQuery table.
etl_stg_to_odm_task: task transform and load the data from the staging table to the(ODM) table. The query filters data that is already in the ODM table and inserts only new data.
