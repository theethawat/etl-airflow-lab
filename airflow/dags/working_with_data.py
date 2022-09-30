from curses import echo
import json
import pathlib
import datetime
import logging

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
    logging.debug(str(plain_booking))


def process_booking(ti):
    plain_booking_input = ti.xcom_pull(
        task_ids=["fetch_booking"])[0]
    plain_booking = json.loads(plain_booking_input)
    if 'rows' not in plain_booking:
        raise ValueError('Empty Booking')
    booking_list = plain_booking['rows']
    booking_status_list = []
    for booking in booking_list:
        booking_status_list.append(booking['status'])
    logging.info('Booking Status List')
    logging.info(booking_status_list)


with DAG(
    dag_id="working_with_data",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval='@daily',
    tags=['database', 'query', 'display'],
    catchup=False,  # เราจะ Sync ให้ตรงกับ time ปัจจุบันมั้ย
) as dag:

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

    booking_explainer = PythonOperator(
        task_id="booking_explainer",
        python_callable=process_booking
    )

    fetch_booking >> [echo_booking, booking_explainer]
