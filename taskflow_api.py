import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

default_args = {'owner': 'admin'}


    
@dag(
         dag_id = 'taskflow_dag',
         description='Running pipeline with taskflow',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = '@once',
         tags = ['taskflow', 'decorator']
    )
def dag_with_taskflowapi():
    @task
    def task_a():
        print("task a executed!")
    
    @task
    def task_b():
        time.sleep(5)
        print("task b executed!")
    
    @task
    def task_c():
        time.sleep(5)
        print("task c executed!")
    
    @task  
    def task_d():
        time.sleep(5)
        print("task d executed!")
    
    @task    
    def task_e():
        print("task e executed!")  
        
    task_a() >> [task_b(), task_c(), task_d()] >> task_e()
    
dag_with_taskflowapi()