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

dag = DAG(
    dag_id="server_mongodb_backup",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval='@daily',
    tags=['backup', 'mongodb', 'db']
)

MONGO_URI = Variable.get("MONGO_URI")

# Get Date in string format


def _get_date():
    date_now = datetime.datetime.now()
    date_in_format = date_now.strftime(
        "%Y") + date_now.strftime("%m") + date_now.strftime("%d")
    return date_in_format


def running_backup():
    backup_command = 'mongodump --uri ' + MONGO_URI + \
        ' --out /opt/airflow/backup-result/'+_get_date()
    os.system(backup_command)
    print("Process Success")


# First Create backup to folder by dumping database
dumping = BashOperator(
    task_id="dump_database",
    bash_command='mongodump --uri ' + MONGO_URI +
    ' --out /opt/airflow/backup-result/'+_get_date(),
    dag=dag,
)

check_dump = BashOperator(
    task_id="check_mongodump_available",
    bash_command=""" 
        if ! mongodump --help ; then \
            mkdir -p /opt/airflow/; \
            curl https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian10-x86_64-100.6.0.deb --output /opt/airflow/mongotool.deb ;\
            apt install -y /opt/airflow/mongotool.deb ; \
        fi 
         """,
    dag=dag,
)


echo_result = BashOperator(
    task_id="echo_result",
    bash_command="ls /opt/airflow/backup-result",
    dag=dag,
)


# def _get_pictures():
#     # Ensure directory exists
#     pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

#     # Download all pictures in launches.json
#     with open("/tmp/launches.json") as f:
#         launches = json.load(f)
#         image_urls = [launch["image"] for launch in launches["results"]]
#         for image_url in image_urls:
#             try:
#                 response = requests.get(image_url)
#                 image_filename = image_url.split("/")[-1]
#                 target_file = f"/tmp/images/{image_filename}"
#                 with open(target_file, "wb") as f:
#                     f.write(response.content)
#                 print(f"Downloaded {image_url} to {target_file}")
#             except requests_exceptions.MissingSchema:
#                 print(f"{image_url} appears to be an invalid URL.")
#             except requests_exceptions.ConnectionError:
#                 print(f"Could not connect to {image_url}.")


# get_pictures = PythonOperator(
#     task_id="get_pictures", python_callable=_get_pictures, dag=dag
# )


check_dump >> dumping >> echo_result
