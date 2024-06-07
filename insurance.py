from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as  pd

default_args = {'owner': 'admin'}


def read_csv_file():
    df = pd.read_csv('/home/ubuntu/environment/airflow/dags/dataset/insurance.csv')
    print(df)
    return df.to_json()
    
def remove_null_values(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(data)
    
    df =df.dropna()
    print(df)
    return df.to_json()

def group_by_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    print(smoker_df)
    
    smoker_df.to_csv('/home/ubuntu/environment/airflow/dags/output/smoker.csv', index=False)

def group_by_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    
    region_df = df.groupby('region').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    print(region_df)
    region_df.to_csv('/home/ubuntu/environment/airflow/dags/output/region.csv', index=False)
    
with DAG(
         dag_id = 'python_pipeline',
         description='Running a python pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'insurance data']
    ) as dag:
        read_csv_file = PythonOperator(
                                 task_id='read_csv_file',
                                 python_callable=read_csv_file
            )
            
        remove_null_values = PythonOperator(
                                 task_id='remove_null_values',
                                 python_callable=remove_null_values
            )
        
        group_by_smoker = PythonOperator(
                                 task_id='group_by_smoker',
                                 python_callable=group_by_smoker
            )   
        group_by_region = PythonOperator(
                                 task_id='group_by_region',
                                 python_callable=group_by_region
            )
            
read_csv_file >> remove_null_values >> [group_by_smoker, group_by_region]