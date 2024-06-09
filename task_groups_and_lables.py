import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {'owner': 'admin'}
DATASET_PATH = '/home/ubuntu/environment/airflow/dataset/insurance.csv'
OUTPUT_PATH = '/home/ubuntu/environment/airflow/output/{0}.csv'

def read_csv_file(ti):
    df = pd.read_csv(DATASET_PATH)
    print(df)
    ti.xcom_push(key='my_csv', value=df.to_json())
    
def remove_null_values(ti):
    data = ti.xcom_pull(key='my_csv')
    df = pd.read_json(data)
    
    df =df.dropna()
    print(df)
    ti.xcom_push(key='clean_csv', value=df.to_json())

def determine_branch():
    transform_action = Variable.get('transform_action', default_var=None)
    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'group_by_region_smoker' :
        return "grouping.{0}".format(transform_action)
        
    
def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)
    
    df = df[df['region']=='southwest']
    df.to_csv(OUTPUT_PATH.format('southwest'))

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)
    
    df = df[df['region']=='southeast']
    df.to_csv(OUTPUT_PATH.format('southeast'))
    
def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)
    
    df = df[df['region']=='northwest']
    df.to_csv(OUTPUT_PATH.format('northwest'))    

def group_by_region_smoker(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)
    
    region_df = df.groupby('region').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    region_df.to_csv(OUTPUT_PATH.format('region_df'))
    
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    smoker_df.to_csv(OUTPUT_PATH.format('smoker_df'))
    

with DAG(
         dag_id = 'taskgroup_label_pipeline',
         description='Running with taskgroup and label',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'taskgroup', 'label']
    ) as dag:
        with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:
            read_csv_file = PythonOperator(
                                     task_id='read_csv_file',
                                     python_callable=read_csv_file
                )
                
            remove_null_values = PythonOperator(
                                     task_id='remove_null_values',
                                     python_callable=remove_null_values
                )
            read_csv_file >> remove_null_values
        
        determine_branch = BranchPythonOperator(
                                 task_id='determine_branch',
                                 python_callable=determine_branch
            )
        
        with TaskGroup('filtering') as filtering:
            filter_by_southwest = PythonOperator(
                                     task_id='filter_by_southwest',
                                     python_callable=filter_by_southwest
                )
                
            filter_by_northwest = PythonOperator(
                                     task_id='filter_by_northwest',
                                     python_callable=filter_by_northwest
                )
                
            filter_by_southeast = PythonOperator(
                                     task_id='filter_by_southeast',
                                     python_callable=filter_by_southeast
                )
        
        with TaskGroup('grouping') as grouping:
            group_by_region_smoker = PythonOperator(
                                     task_id='group_by_region_smoker',
                                     python_callable=group_by_region_smoker
                )
            
reading_and_preprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]