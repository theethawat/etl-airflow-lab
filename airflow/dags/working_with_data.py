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

import pandas as pd
import matplotlib.pyplot as plt


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
    logging.info(str(plain_booking))


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
    Variable.set("booking_status_list", booking_status_list)


def save_image(imagefile, filename):
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # try:
    target_file = f"/tmp/images/{filename}"
    with open(target_file, "wb") as f:
        f.write(imagefile)
    print(f"Saved {filename} to {target_file}")
    # except:
    #     print('Error on Savefile')
    #     raise ValueError('Save File Error')


def process_graph_generator():
    fetched_status_list = Variable.get('booking_status_list')
    if not len(fetched_status_list):
        raise ValueError('Empty Booking Status')

    booking_status_text = ['จอง', 'ยืนยันการจอง', 'เช็คอินเข้าพัก',
                           'เช็คเอาท์ออกจากห้องพัก', 'ยกเลิก', 'รอ', 'เสร็จสิ้น']
    booking_status_eng_text = ['Booking', 'Confirmed', 'Checkedin',
                               'Checkout', 'Cancle', 'Wait', 'Success']
    status_amount = [0, 0, 0, 0, 0, 0, 0]

    for booking_staus in fetched_status_list:
        count = 0
        for status_text in booking_status_text:
            if booking_staus == status_text:
                status_amount[count] += 1
            count += 1

    # booking_amont_series = pd.Series(
    #     data=status_amount, index=booking_status_text)
    # all_booking_df = pd.DataFrame(booking_amont_series)
    # logging.info('All Booking Dataframe')
    # logging.info(all_booking_df)
    fig, ax = plt.subplots()

    ax.bar(booking_status_eng_text, status_amount,
           width=1, edgecolor="white", linewidth=0.7)

    image_name = 'booking'+format_date(last_5_day_date()
                                       )+'-to-'+format_date(next_5_day_date()) + '.png'
    
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    target_file = f"/tmp/images/{image_name}"
    image = plt.savefig(fname=target_file)
    #save_image(image, image_name)


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

    booking_generate_graph = PythonOperator(
        task_id="booking_generate_graph",
        python_callable=process_graph_generator
    )

    fetch_booking >> [echo_booking, booking_explainer]
    booking_explainer >> booking_generate_graph
