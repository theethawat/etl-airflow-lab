from curses import echo
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


def format_date(unformatted_datetime):
    date_in_format = unformatted_datetime.strftime(
        "%Y") + '-' + unformatted_datetime.strftime("%m") + '-' + unformatted_datetime.strftime("%d")
    return date_in_format


def last_5_day_date():
    result_date = datetime.datetime.now() - datetime.timedelta(days=5)
    return result_date


def next_5_day_date():
    result_date = datetime.datetime.now() + datetime.timedelta(days=5)
    return result_date


def logging_some_data(ti):
    # XCom ส่วนกลาง / ข้อมูลส่วนกลาง
    plain_booking_input = ti.xcom_pull(
        task_ids=["fetch_booking"])[0]
    plain_booking = json.loads(plain_booking_input)
    print(str(plain_booking))


with DAG(
    dag_id="working_with_data",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval='@daily',
    tags=['database', 'query', 'display'],
    catchup=False,  # เราจะ Sync ให้ตรงกับ time ปัจจุบันมั้ย
) as dag:

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

    fetch_booking = SimpleHttpOperator(
        task_id='fetch_booking',
        http_conn_id='eaccom_chaesonvintage_api',
        endpoint='/booking',
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer "+Variable.get('api_token')},
        method='GET',
        data={
            "start": format_date(last_5_day_date()), "end": format_date(next_5_day_date())
        },
    )

    echo_booking = PythonOperator(
        task_id='echo_booking',
        python_callable=logging_some_data
    )

    fetch_booking >> echo_booking
