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
def interoperating_with_taskflow():
    
    def read_csv(ti):
        df = pd.read_csv('/home/ubuntu/environment/airflow/dataset/car_data.csv')
        print(df)
        ti.xcom_push(key='carData', value=df.to_json())
        
    @task
    def filteredTesla(jsonData):
        df = pd.read_json(jsonData)
        df = df[df['Brand']=='Tesla ']
        return df.to_json()
       
    def write_csv(filteredTeslaData):
        df = pd.read_json(filteredTeslaData)
        df.to_csv("/home/ubuntu/environment/airflow/output/teslas.csv", index=False)
    
    read_csv_task = PythonOperator(
                                   task_id='read_csv_task',
                                   python_callable=read_csv
        )    
    filteredData = filteredTesla(read_csv_task.output['carData'])
    
    write_csv_task = PythonOperator(
                                   task_id='write_csv_task',
                                   python_callable=write_csv,
                                   op_kwargs={'filteredTeslaData': filteredData}
        )  
        

interoperating_with_taskflow()