from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)

import requests
from datetime import datetime
				
import re
import json
import os
import functools
import time		   

from google.cloud import bigquery
from google.cloud import storage

url ='https://www.kaggle.com/rankings'


def get_html(url):

    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(response.status_code)

def get_json(ti):
    html = ti.xcom_pull(task_ids=
        "load_kaggle2_"
    )
    usernames = re.findall('{"currentRanking":[^}]*}', html)
    username = []
    for i in usernames:
        username.append(json.loads(i))

    return username

    
def upload_to_gcs(ti, bucket, object_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    username = ti.xcom_pull(task_ids=
        "get_json"
    )

    with blob.open("w") as outfile:
        for i in range(len(username)):
            json.dump(username[i], outfile)
            outfile.write('\n')


def etl_gcs_to_bq():
    # Construct a BigQuery client object.
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("currentRanking", "STRING"),
            bigquery.SchemaField("displayName", "STRING"),
            bigquery.SchemaField("thumbnailUrl", "STRING"),
            bigquery.SchemaField("userId", "STRING"),
            bigquery.SchemaField("userUrl", "STRING"),
            bigquery.SchemaField("tier", "STRING"),
            bigquery.SchemaField("points", "STRING"),
            bigquery.SchemaField("joinTime", "STRING"),
            bigquery.SchemaField("totalGoldMedals", "STRING"),
            bigquery.SchemaField("totalSilverMedals", "STRING"),
            bigquery.SchemaField("totalBronzeMedals", "STRING"),
            bigquery.SchemaField("sysinsertdttm", "DATETIME"), 
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = "gs://skycryback/test/json_data.json"
    table_id = "massive-capsule-295317.258.raw_data"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  
        job_config=job_config,
    )  

    load_job.result()  

    dml_statement = (
    f'''UPDATE `{table_id}`
    SET sysinsertdttm = CURRENT_DATETIME() 
    where sysinsertdttm is null''')
    query_job = client.query(dml_statement)  
    query_job.result() 
    
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def etl_stg_to_odm():
    # Construct a BigQuery client object.
    client = bigquery.Client()
    
    schema=[
            bigquery.SchemaField("currentRanking", "STRING"),
            bigquery.SchemaField("displayName", "STRING"),
            bigquery.SchemaField("thumbnailUrl", "STRING"),
            bigquery.SchemaField("userId", "STRING"),
            bigquery.SchemaField("userUrl", "STRING"),
            bigquery.SchemaField("tier", "STRING"),
            bigquery.SchemaField("points", "STRING"),
            bigquery.SchemaField("joinTime", "STRING"),
            bigquery.SchemaField("totalGoldMedals", "STRING"),
            bigquery.SchemaField("totalSilverMedals", "STRING"),
            bigquery.SchemaField("totalBronzeMedals", "STRING"),
            bigquery.SchemaField("sysupdatedttm", "DATETIME"), 
        ]
    
    table_id = "massive-capsule-295317.258.golden_medals"
    table = bigquery.Table(table_id, schema=schema)
    
    # Check if table exists
    table = client.get_table(table_id)
    if table:
        print(f'Table {table_id} already exists.')

    else:
        table = client.create_table(table)  
        
    query = (
    f'''INSERT INTO  `{table_id}`
    WITH raw AS (
		 SELECT 
            currentRanking
            ,displayName
            ,thumbnailUrl
            ,userId
            ,userUrl
            ,tier
            ,points
            ,joinTime
            ,totalGoldMedals
            ,totalSilverMedals
            ,totalBronzeMedals
            ,cast(sysinsertdttm as date)
		FROM `massive-capsule-295317.258.raw_data`
		where cast(sysinsertdttm as date) = current_date
		)
    SELECT raw.*
    FROM raw
    LEFT JOIN `massive-capsule-295317.258.golden_medals` tgt
    ON raw.userId = tgt.userId
    WHERE tgt.userId IS NULL;

    UPDATE  `{table_id}` t
    set
            currentRanking = src.currentRanking
            ,displayName = src.displayName
            ,thumbnailUrl = src.thumbnailUrl
            ,userId = src.userId
            ,userUrl = src.userUrl
            ,tier = src.tier
            ,points = src.points
            ,joinTime = src.joinTime
            ,totalGoldMedals = src.totalGoldMedals
            ,totalSilverMedals = src.totalSilverMedals
            ,totalBronzeMedals = src.totalBronzeMedals
            ,sysupdatedttm = case when md5(concat(coalesce(src.currentRanking, 'n')
            ,coalesce(src.displayName, 'n')
            ,coalesce(src.thumbnailUrl, 'n')
            ,coalesce(src.userId, 'n')
            ,coalesce(src.userUrl, 'n')
            ,coalesce(src.tier, 'n')
            ,coalesce(src.points, 'n')
            ,coalesce(src.joinTime, 'n')
            ,coalesce(src.totalGoldMedals, 'n')
            ,coalesce(src.totalSilverMedals, 'n')
            ,coalesce(src.totalBronzeMedals, 'n'))) =
            md5(concat(coalesce(tgt.currentRanking, 'n')
            ,coalesce(tgt.displayName, 'n')
            ,coalesce(tgt.thumbnailUrl, 'n')
            ,coalesce(tgt.userId, 'n')
            ,coalesce(tgt.userUrl, 'n')
            ,coalesce(tgt.tier, 'n')
            ,coalesce(tgt.points, 'n')
            ,coalesce(tgt.joinTime, 'n')
            ,coalesce(tgt.totalGoldMedals, 'n')
            ,coalesce(tgt.totalSilverMedals, 'n')
            ,coalesce(tgt.totalBronzeMedals, 'n')))
            then 
            tgt.sysupdatedttm else CURRENT_DATETIME() end 
	FROM `massive-capsule-295317.258.raw_data`src
    inner JOIN `{table_id}` tgt
    ON src.userId = tgt.userId
    and cast(sysinsertdttm as date) = current_date
    where src.userId = t.userId
    ; 
    
    DELETE `{table_id}`
    where userId in ( 
    select tgt.userId from `{table_id}` tgt
    left join `massive-capsule-295317.258.raw_data` src
    ON src.userId = tgt.userId
    and cast(sysinsertdttm as date) = current_date 
    WHERE src.userId IS NULL
    );
   ''')
    query_job = client.query(query) 
    query_job.result() 

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 12),
    'catchup': False,
}

with DAG('load_dag',
    default_args=default_args,
    description='Extract, transform, and load Kaggle rankings data into BigQuery',
    schedule_interval='0 0 * * *',  # Run once per day at midnight UTC
    ) as dag:

        load_kaggle2_ = PythonOperator(
            task_id="load_kaggle2_",
            python_callable=functools.partial(get_html, url ='https://www.kaggle.com/rankings') 
            
        )

        endish = BashOperator(
            task_id = 'endish',
            bash_command = "echo 'end of stream'"
            
        )

        get_json = PythonOperator(
            task_id="get_json",
            python_callable=get_json 
            
        )

        upload_to_gcs = PythonOperator(
            task_id="upload_to_gcs",
            python_callable=functools.partial(upload_to_gcs, bucket = 'skycryback', 
            object_name = 'test/json_data_' + time.strftime("%Y%m%d")  + '.json')
               
        )

        etl_stg_to_odm = PythonOperator(
            task_id="etl_stg_to_odm",
            python_callable=etl_stg_to_odm  
             
        )

        etl_gcs_to_bq = PythonOperator(
             task_id="etl_gcs_to_bq",
             python_callable = etl_gcs_to_bq
             
        )

        load_kaggle2_ >> get_json >> upload_to_gcs >> etl_gcs_to_bq >> etl_stg_to_odm >> endish

