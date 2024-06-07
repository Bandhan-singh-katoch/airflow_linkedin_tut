from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {'owner': 'admin'}

# dag = DAG(dag_id='hello_world',
#           description='first hello world dag',
#           default_args=default_args,
#           start_date=days_ago(1),
#           schedule_interval=None)

# task = BashOperator(task_id='hello_world_task',
#                     bash_command='echo "Hello World"',
#                     dag=dag)

with DAG(
        dag_id = 'hello_world',
        description='first hello world dag',
        default_args=default_args,
        start_date = days_ago(1),
        schedule_interval = '@daily'
    ) as dag:
        task = BashOperator(
                           task_id='hello_world_task',
                           bash_command='echo "Hello World" using with'
            )
        
    

task
