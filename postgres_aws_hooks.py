from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from io import StringIO
import pandas as pd

default_args = {'owner': 'admin'}

def fetch_s3_file(bucket_name, file_key):
     s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
     file_content = s3_hook.read_key(bucket_name=bucket_name, key=file_key)
     
     if isinstance(file_content, bytes):
         file_content = file_content.decode('utf-8')
         
     df = pd.read_csv(StringIO(file_content))
     return df.to_json()
     
def remove_null_values(json_data):
    df = pd.read_json(json_data)
    df = df.dropna()
    
    return df.to_json()

def create_customer_credit_card_details_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    sql = """CREATE TABLE IF NOT EXISTS customer_credit_card_details (
            id INT,
            name VARCHAR(255),
            email VARCHAR(255),
            credit_card_number VARCHAR(50),
            credit_card_type VARCHAR(50)
        );"""
    
    pg_hook.run(sql=sql)

def insert_customer_credit_card_details_data(json_data):
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    df = pd.read_json(json_data)
    
    for _, row in df.iterrows(): 
        query = f"""
                INSERT INTO customer_credit_card_details 
                (id, name, email, credit_card_number, credit_card_type)
                VALUES ({row['id']}, 
                        '{row['name']}', 
                        '{row['email']}', 
                        '{row['credit card number']}', 
                        '{row['credit card type']}');
        """
        pg_hook.run(sql=query)
        
with DAG(
         dag_id = 'aws_hook_pipeline',
         description='Running aws hook pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['hook', 'aws']
    ) as dag:
        
        fetch_s3_file_task =  PythonOperator(
                                        task_id='fetch_s3_file_task',
                                        python_callable=fetch_s3_file,
                                        op_kwargs={'bucket_name':'credit-card-info', 'file_key':'credit_card_details.csv'}
            )
        remove_null_values_task = PythonOperator(
                                        task_id='remove_null_values_task',
                                        python_callable=remove_null_values,
                                        op_kwargs={'json_data': '{{ti.xcom_pull(task_ids="fetch_s3_file_task")}}'}
            )
        create_customer_credit_card_details_table_task = PythonOperator(
                                           task_id='create_customer_credit_card_details_table_task',
                                           python_callable=create_customer_credit_card_details_table
            )
        insert_customer_credit_card_details_data_task = PythonOperator(
                                           task_id='insert_customer_credit_card_details_data_task',
                                           python_callable=insert_customer_credit_card_details_data,
                                           op_kwargs={'json_data': '{{ti.xcom_pull(task_ids="remove_null_values_task")}}'}
            ) 
            
fetch_s3_file_task >> remove_null_values_task >> create_customer_credit_card_details_table_task >> insert_customer_credit_card_details_data_task 

# run below command
# airflow dags test pipe_line_name 2024-06-16
