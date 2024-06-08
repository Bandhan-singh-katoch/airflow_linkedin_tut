from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from random import choice 

default_args = {'owner': 'admin'}

def has_driving_licence():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_licence'):
        return 'eligible_to_drive'
    return 'not_eligible_to_drive'

def eligible_to_drive():
    print("You can drive because u have a licence")
    
def not_eligible_to_drive():
    print("You can't drive because u don't have a licence")


with DAG(
         dag_id = 'brach_pipeline',
         description='Running BranchPythonOperator pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'branch']
    ) as dag:
        has_driving_licence = PythonOperator(
                                             task_id='has_driving_licence',
                                             python_callable=has_driving_licence
            )
        branch = BranchPythonOperator(
                                             task_id='branch',
                                             python_callable=branch
            )
        eligible_to_drive = PythonOperator(
                                             task_id='eligible_to_drive',
                                             python_callable=eligible_to_drive
            )
        not_eligible_to_drive = PythonOperator(
                                             task_id='not_eligible_to_drive',
                                             python_callable=not_eligible_to_drive
            )
            
has_driving_licence >> branch >> [eligible_to_drive, not_eligible_to_drive]