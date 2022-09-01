from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

import logging

default_args = {
    'owner': 'airflow',
}

# เหมือนเป็นการ extend/implement เพื่อใช้งาน
with DAG(
    'my_1st_dag',
    schedule_interval='*/5 2 * * *',
    default_args=default_args,
    start_date=timezone.datetime(2022, 9, 1),
    tags=['ETL', 'Hello World'],
    catchup=False,  # เราจะ Sync ให้ตรงกับ time ปัจจุบันมั้ย
) as dag:
    # DAG เป็น class, dag เป็น Object

    t1 = DummyOperator(
        task_id='my_1st_task',
        dag=dag,
    )

    t2 = DummyOperator(
        task_id='my_2nd_task',
        dag=dag,
    )

    t3 = DummyOperator(
        task_id='my_3rd_task',
        dag=dag,
    )

    t4 = BashOperator(
        task_id='t4-echo',
        bash_command='echo hello',
        dag=dag
    )

    def hello():
        return 'Hello, An ant'

    def print_log_message():
        logging.debug('This is Debugging')
        logging.info('This is Info')
        logging.error('This is Error')
        logging.warn('This is waringing')
        return 'Whatever is returned also gets printed in log'

    say_hello = PythonOperator(
        task_id='say_hello_ant',
        python_callable=hello,
        dag=dag
    )

    run_this = PythonOperator(
        task_id='print_log_message',
        python_callable=print_log_message,
        dag=dag
    )

    say_hello >> t1 >> [t2, t3] >> t4 >> run_this
