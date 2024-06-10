import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from random import choice 

default_args = {'owner': 'admin'}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='choose_branch'):
        return 'task_c'
    return 'task_e'

def taskC():
    print("Task C executed")
    
    
with DAG(
         dag_id = 'crone_catchup_backfill',
         description='Running crone, catchup and backfill',
         default_args = default_args,
         start_date = days_ago(5),
         schedule_interval = '0 0 * * *',
         tags = ['crone', 'catchup' , 'backfill'],
         catchup = True
    ) as dag:
       taskA = BashOperator(
                            task_id='taskA',
                            bash_command='echo "task A has executed"'
           )
           
       chooseBranch = PythonOperator(
                                    task_id='choose_branch',
                                    python_callable=choose_branch
        )
            
       taskBranch = BranchPythonOperator(
                               task_id='task_branch',
                               python_callable=branch
           )
       
       taskC = PythonOperator(
                              task_id='task_c',
                              python_callable=taskC
           )
           
       taskD = BashOperator(
                             task_id='task_d',
                             bash_command='echo "task D executed"'
           )
           
       taskE = EmptyOperator(
                             task_id='task_e'
           )
            
taskA >> chooseBranch >> taskBranch >> [taskC, taskE]
taskC >> taskD