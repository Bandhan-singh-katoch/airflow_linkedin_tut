from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
import glob
import pandas as pd

default_args = {'owner': 'bandhan'}

FILE_PATH = '/home/ubuntu/environment/airflow/files/laptops_*.csv'
FILE_COLS = ['Id','Company','Product',	'TypeName','Price_euros']
OUTPUT_PATH = '/home/ubuntu/environment/airflow/output/{}.csv'

def insertLaptopData():
    conn = psycopg2.connect(
                    host="localhost",
                    database="laptop_db",
                    user="bandhan",
                    password="Band1234"
        )
        
    cur = conn.cursor()
    
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file,usecols=FILE_COLS)
        records = df.to_dict('records')
        
        for row in records:
            sql = f"""INSERT INTO laptops 
                        (id, company, product, type_name, price_euros) 
                        VALUES (
                            {row['Id']}, 
                            '{row['Company']}', 
                            '{row['Product']}', 
                            '{row['TypeName']}', 
                            {row['Price_euros']})
                    """
            cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    
def filterGamingLaptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)
        filteredDf = df[df['TypeName'] == 'Gaming']
        
        filteredDf.to_csv(OUTPUT_PATH.format('gaming_laptops'), mode='a',header=False, index=False)

def filterNotebookLaptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)
        filteredDf = df[df['TypeName'] == 'Notebook']
        
        filteredDf.to_csv(OUTPUT_PATH.format('notebook_laptops'), mode='a',header=False, index=False)
        
def filterUltrabookLaptops():
    for file in glob.glob(FILE_PATH):
        df = pd.read_csv(file, usecols=FILE_COLS)
        filteredDf = df[df['TypeName'] == 'Ultrabook']
        
        filteredDf.to_csv(OUTPUT_PATH.format('gaming_laptops'), mode='a',header=False, index=False)
        
with DAG(
         dag_id='file_sensor_pipeline',
         description='Running file sensor with postgres',
         default_args=default_args,
         start_date=days_ago(1),
         schedule_interval='@once',
         tags=['postgres', 'sensor'],
         template_searchpath='/home/ubuntu/environment/airflow/sql'
    ) as dag:
        drop_laptop_table = PostgresOperator(
                                task_id='drop_laptop_table_task',
                                postgres_conn_id='laptop_db_conn',
                                sql="""Drop table if exists laptops;"""
            )
            
        create_laptop_table = PostgresOperator(
                                task_id='create_laptop_table_task',
                                postgres_conn_id='laptop_db_conn',
                                sql='create_table_laptops.sql'
            )
            
        drop_premium_laptop_table = PostgresOperator(
                                task_id='drop_premium_laptop_task',
                                postgres_conn_id='laptop_db_conn',
                                sql="""Drop table if exists premium_laptops;"""
            )
            
        create_premium_laptop_table = PostgresOperator(
                                task_id='create_premium_laptop_table_task',
                                postgres_conn_id='laptop_db_conn',
                                sql='create_premium_table_laptops.sql'
            )
            
        file_sensor = FileSensor(
                                task_id='file_sensor_task',
                                filepath= FILE_PATH,
                                poke_interval=10,
                                timeout=60*10
            )
            
        insert_laptop_data  = PythonOperator(
                                task_id='insert_laptop_data_task',
                                python_callable=insertLaptopData
            )
            
        wait_for_premium_laptop = SqlSensor(        #for testing purpose only
                                task_id='wait_for_premium_laptop_task',
                                conn_id='laptop_db_conn',
                                sql='select exists(select 1 from laptops where price_euros>500;)',
                                poke_interval=10,
                                timeout=60*10
            )
            
        insert_premium_laptops = PostgresOperator(
                                   task_id='insert_premium_laptops_task',
                                   postgres_conn_id='laptop_db_conn',
                                   sql="Insert into premium_laptops select * from laptops where price_euros>500;"
            )
            
        filter_gaming_laptops     = PythonOperator(
                                task_id='filter_gaming_laptops_task',
                                python_callable=filterGamingLaptops
            )
            
        filter_notebook_laptops  = PythonOperator(
                                task_id='filter_notebook_laptops_task',
                                python_callable=filterNotebookLaptops
            )
            
        filter_ultrabook_laptops = PythonOperator(
                                task_id='filter_ultrabook_laptops_task',
                                python_callable=filterUltrabookLaptops
            )
        
        delete_files = BashOperator(
                                task_id='delete_files_task',
                                bash_command='rm {}'.format(FILE_PATH)
            )  

drop_premium_laptop_table >> create_premium_laptop_table
drop_laptop_table >> create_laptop_table

[create_laptop_table, create_premium_laptop_table] >> file_sensor >> insert_laptop_data >> \
  [filter_gaming_laptops, filter_notebook_laptops, filter_ultrabook_laptops] >> delete_files
insert_laptop_data >> wait_for_premium_laptop >> insert_premium_laptops