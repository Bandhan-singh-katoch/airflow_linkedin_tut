import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import json
import pandas as pd

default_args = {'owner': 'admin'}


@dag(
         dag_id = 'taskflow_pass_data_dag',
         description='passing data with taskflow',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = '@once',
         tags = ['taskflow', 'decorator']
    )
def branching_using_taskflow():
    @task(task_id='read_csv_task')
    def read_csv():
        df = pd.read_csv('/home/ubuntu/environment/airflow/dataset/car_data.csv')
        print(df)
        return df.to_json()
        
    @task.branch
    def determine_branch():
        variable = Variable.get('transform', default_var=None)
        if variable == 'filter_two_seaters':
            return 'filter_two_seaters'
        elif variable == 'fwds_seats':
            return 'fwds_seats_task'
        
        
    @task(task_id='filter_two_seaters')
    def filter_two_seaters_task(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_csv_task')
        df = pd.read_json(json_data)
        two_seater_df = df[df['Seats']==2]
        
        ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
        ti.xcom_push(key='file_name', value='two_seater_file')
        
    @task(task_id='fwds_seats_task') 
    def filter_fwds_task(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_csv_task')
        df = pd.read_json(json_data)
        
        fwds_df = df[df['PowerTrain']=='FWD']
        
        ti.xcom_push(key='transform_result', value=fwds_df.to_json())
        ti.xcom_push(key='file_name', value='fwds_file')
    
    @task(trigger_rule='none_failed')     
    def write_csv(**kwargs):
        ti = kwargs['ti']
        transform_result = ti.xcom_pull(key='transform_result')
        df = pd.read_json(transform_result)
        file_name = ti.xcom_pull(key='file_name')
        
        print(transform_result)
        print(file_name)
        df.to_csv("/home/ubuntu/environment/airflow/output/{0}.csv".format(file_name), index=False)

    
    read_csv() >> determine_branch() >> [ filter_two_seaters_task() , filter_fwds_task()] >> write_csv()

branching_using_taskflow()