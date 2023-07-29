import logging

import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

log = logging.getLogger(__name__)


def say_hello():
    print("Hello Worlds!!")

def print_testing_task():
    print('_' * 50)
    print('ПРОВЕРКА')
    print('_' * 50)

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
)
def print_test():
    hello = PythonOperator(
        task_id='helloooooo',
        python_callable=say_hello
    )
    
    prnt_task = PythonOperator(
        task_id='prrrrrrnt',
        python_callable=print_testing_task
    )
  
    [hello, prnt_task]

hello_dag = print_test()
