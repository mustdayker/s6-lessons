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


# ------------------- Скачивание исходников -----------------------

def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла
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

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
# bash_command_tmpl = """
# echo "$(head -n 10 {{ params.files }})"
# """

bash_command_tmpl = """
echo {{ params.files }}
"""



# ------------------- СОЗДАНИЕ ТАБЛИЦ -----------------------

def ddl_tables_f(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:

        querry = """
        DROP TABLE IF EXISTS STV2023070319__STAGING.groups;

        CREATE TABLE STV2023070319__STAGING.groups(
            id              int PRIMARY KEY,
            admin_id        int,
            group_name      varchar(100),
            registration_dt timestamp,
            is_private      int
        )
        ORDER BY id, admin_id
        PARTITION BY registration_dt::date
        GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
        ;


        DROP TABLE IF EXISTS STV2023070319__STAGING.users;

        CREATE TABLE STV2023070319__STAGING.users(
            id              int PRIMARY KEY,
            chat_name       varchar(200),
            registration_dt timestamp,
            country         varchar(200),
            age             int
        )
        ORDER BY id
        ;

        DROP TABLE IF EXISTS STV2023070319__STAGING.dialogs;

        CREATE TABLE STV2023070319__STAGING.dialogs(
            message_id   int PRIMARY KEY,
            message_ts   timestamp,
            message_from int,
            message_to   int,
            message      varchar(1000),
            message_group int
        )
        ORDER BY message_id
        PARTITION BY message_ts::date
        GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
        ;
        """

        cur = conn.cursor()
        cur.execute(querry) 

        # res = cur.fetchall()
        return 300

# ------------------- Заливка groups -----------------------

def fill_groups_f(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:

        querry = """
        COPY STV2023070319__STAGING.groups(
            id,
            admin_id,
            group_name,
            registration_dt,
            is_private
            )
        FROM LOCAL '/data/groups.csv'
        DELIMITER ','
        REJECTED DATA AS TABLE groups_rej
        ;
        """

        cur = conn.cursor()
        cur.execute(querry) 

        # res = cur.fetchall()
        return 300

# ------------------- Заливка users -----------------------

def fill_users_f(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:

        querry = """
        COPY STV2023070319__STAGING.users(
            id,
            chat_name,
            registration_dt,
            country,
            age
            )
        FROM LOCAL '/data/users.csv'
        DELIMITER ','
        REJECTED DATA AS TABLE users_rej
        ;
        """

        cur = conn.cursor()
        cur.execute(querry) 

        # res = cur.fetchall()
        return 300

# ------------------- Заливка dialogs -----------------------

def fill_dialogs_f(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:

        querry = """
        COPY STV2023070319__STAGING.dialogs(
            message_id,
            message_ts,
            message_from,
            message_to,
            message,
            message_group
            )
        FROM LOCAL '/data/dialogs.csv'
        DELIMITER ','
        REJECTED DATA AS TABLE dialogs_rej
        ;
        """

        cur = conn.cursor()
        cur.execute(querry) 

        # res = cur.fetchall()
        return 300    



@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_fill_stg():

    bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv']
    
    task1 = PythonOperator(
        task_id=f'fetch_users.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )
    
    task2 = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    task3 = PythonOperator(
        task_id=f'fetch_dialogs.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )

    ddl_tables = PythonOperator(
        task_id='ddl_tables',
        python_callable=ddl_tables_f,
    )

    fill_groups = PythonOperator(
        task_id='fill_groups',
        python_callable=fill_groups_f,
    )

    fill_users = PythonOperator(
        task_id='fill_users',
        python_callable=fill_users_f,
    )

    fill_dialogs = PythonOperator(
        task_id='fill_dialogs',
        python_callable=fill_dialogs_f,
    )


    [task1, task2, task3] >> print_10_lines_of_each >> ddl_tables >> [fill_groups, fill_users, fill_dialogs]

dag_3_3_2 = sprint6_dag_fill_stg() 