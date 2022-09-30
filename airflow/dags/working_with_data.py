import json
import pathlib
import datetime
import os

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

dag = DAG(
    dag_id="working_with_data",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval='@daily',
    tags=['database', 'query', 'display']
)


def _get_date():
    date_now = datetime.datetime.now()
    date_in_format = date_now.strftime(
        "%Y") + date_now.strftime("%m") + date_now.strftime("%d")
    return date_in_format

# task_http_sensor_check = HttpSensor(
#     task_id='http_sensor_check',
#     http_conn_id='http_default',
#     endpoint='',
#     request_params={},
#     response_check=lambda response: "httpbin" in response.text,
#     poke_interval=5,
#     dag=dag,
# )

# is_api_available = HttpSensor(
#     task_id='is_api_available',
#     http_conn_id='user_api',
#     endpoint='api/'
# )


fetch_user = SimpleHttpOperator(
    task_id='fetch_booking',
    http_conn_id='eaccom_chaesonvintage_api',
    endpoint='/booking',
    headers={"Content-Type": "application/json",
             "Authorization": "Bearer "+Variable.get('api_token')},
    method='GET'
    request_params={
        "start": "", "end": ""
    }
)
# heck_dump >> dumping >> echo_result
