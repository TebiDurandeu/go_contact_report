import csv
import requests
from google.cloud import bigquery
import pandas as pd
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(1)
date = "2023-01-20"

default_args = {
    'owner': 'esteban.durandeu',
}

data_path_load = '/home/airflow/gcs/data/report_' + date + '.csv'
data_path_read = data_path_load[-26:]

def build_report_template_info():

    url = "https://carnovo.gocontact.es/fs/modules/report-builder/php/reportBuilderRequests.php"

    payload='action=downloadReport&domain=26ac59b4-3608-4b3f-9906-2824e7eb993f&' \
            'username=data&password=f9baf4d70612f037035b52a1e4e5b254e8af565d89afcffb47920c40c11c43c7aada5d01599ab8d6aa09d379d59ecf3fcc7b699b26ad442d4d4074b52bff5c05&' \
            'api_download=true&' \
            'ownerType=campaign&' \
            'startDate={0}%2000%3A00%3A00&' \
            'endDate={0}%2023%3A59%3A59&' \
            'dataType=0&' \
            'templateId=1270&' \
            'includeALLOwners=true'.format(date.strip('\"'))
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'PHPSESSID=4b0c2ekevet5j5jcaiurqibimo'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

    #def dowload_reporting():

    url = "https://carnovo.gocontact.es/fs/" \
          "modules/report-builder/php/reportBuilderRequests.php?action=getCsvReportFile&" \
          "domain=26ac59b4-3608-4b3f-9906-2824e7eb993f&" \
          "username=data&" \
          "password=f9baf4d70612f037035b52a1e4e5b254e8af565d89afcffb47920c40c11c43c7aada5d01599ab8d6aa09d379d59ecf3fcc7b699b26ad442d4d4074b52bff5c05&" \
          "api_download=true&file={0}&=".format(response.text.strip('\"'))
    print(url)
    payload = {}
    headers = {
        'Cookie': 'PHPSESSID=4b0c2ekevet5j5jcaiurqibimo; go_fileDownload=true'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    decoded_content = response.content.decode('utf-8')
    with open(data_path_load, "w", encoding='utf-8') as f:
        f.write(decoded_content)
    print(f"Output to {data_path_load}")

with DAG(
    'go_contact_workflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 2 * * *',
    tags=['go_contact','workshop']
) as dag:
    dag.doc_md = "Sample Workflow to extract go_contact data and load to data big query (datawarehouse)"

    t1 = PythonOperator(
        task_id='get_data_from_api',
        python_callable=build_report_template_info,
        op_kwargs={'data_path': data_path_load},
    )

    t2 = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='europe-west1-test-carnovo-s-14c55f20-bucket',
        source_objects=[data_path_read],
        field_delimiter =';',
        destination_project_dataset_table='carnovo-data.carnovo.go_contact',
        skip_leading_rows=1,
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'client_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_center_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'phone',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'database_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'database_name',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'campaign_name',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_outcome_name',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_outcome_group',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'contact_outcome_name',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_type',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_start_date',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_end_date',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'call_lenght',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'total_calls',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'agent_user_name',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'ddi',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'hang_up_cause',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'owner_type',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'ring_time',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'talk_time',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'wait_time',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'wrap_up_time',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'recording_adress',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'transfer_destination',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'load_date',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'comments',
                'type': 'STRING'
            },
        ],
        write_disposition='WRITE_APPEND',
    )


    t1 >> t2
