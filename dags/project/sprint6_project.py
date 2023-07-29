import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import os
import boto3
import vertica_python

conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023070319',       
             'password': 'zmb2aHGYQIPkxIS',
             'database': 'dwh',
             'autocommit': True
}


# ------------------- Получение group_log.csv -----------------------

def fetch_s3_file(bucket: str, key: str):
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    ) 

bash_command_tmpl = """
head {{ params.files }}
"""


# ------------------- Заливка group_log в STG  -----------------------

def fill_group_logs_f(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:

        querry = """
        COPY STV2023070319__STAGING.group_log(
            group_id,
            user_id,
            user_id_from,
            event,
            "datetime"
            )
        FROM LOCAL '/data/group_log.csv'
        DELIMITER ','
        REJECTED DATA AS TABLE group_log_rej
        ;
        """

        cur = conn.cursor()
        cur.execute(querry) 

        # res = cur.fetchall()
        return 300



@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_project():

    bucket_files = ['group_log.csv']
    
    get_group_logs = PythonOperator(
        task_id=f'fetch_group_log.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )

    fill_group_logs = PythonOperator(
        task_id='fill_group_logs',
        python_callable=fill_group_logs_f,
    )


    get_group_logs >> print_10_lines_of_each >> fill_group_logs

dag_project = sprint6_project() 