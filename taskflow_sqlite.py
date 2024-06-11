import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.models import Variable
from airflow.decorators import dag, task
import json
import pandas as pd

default_args = {'owner': 'admin'}


@dag(
         dag_id='data_transformation_storage_pipeline',
         description='passing data with taskflow',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = '@once',
         tags = ['taskflow', 'decorator']
    )
def data_transformation_storage_pipeline():
    
    @task(task_id='read_csv_task')
    def read_csv():
        df = pd.read_csv('/home/ubuntu/environment/airflow/dataset/car_data.csv')
        print(df)
        return df.to_json()
    
    @task(task_id='read_category_task')
    def read_car_categories():
        df = pd.read_csv('/home/ubuntu/environment/airflow/dataset/car_categories.csv')
        return df.to_json()
        
    @task
    def createCarTable():
        sqlite_operator = SqliteOperator(
              task_id="create_car_table",
              sqlite_conn_id="my_sqlite_conn",
              sql="""CREATE TABLE IF NOT EXISTS car_data (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        model TEXT NOT NULL,
                        body_style TEXT NOT NULL,
                        seat INTEGER NOT NULL,
                        price INTEGER NOT NULL);"""
            )
        sqlite_operator.execute(context=None)
    
    @task
    def insertCarData(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_csv_task')
        print(data)
        df = pd.read_json(data)
        df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]
        
        df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
        dfList = df.to_dict(orient='records')
        
        insertQuery = """insert into car_data (brand, model, body_style, seat, price) values(?,?,?,?,?);"""
        
        for record in dfList:
            print(record)
            insert_operation = SqliteOperator(
                                task_id='insert_car_data_operation',
                                sqlite_conn_id="my_sqlite_conn",
                                sql=insertQuery,
                                parameters=tuple(record.values())
                )
            insert_operation.execute(context=None)
    
    @task
    def createCarCategories():
        sqliteOperator = SqliteOperator(
                             task_id='create_car_categories',
                             sqlite_conn_id="my_sqlite_conn",
                             sql="""Create table if not exists car_categories(
                                id INTEGER PRIMARY KEY,
                                brand TEXT NOT NULL,
                                category TEXT NOT NULL
                             );
                             """
            )
        sqliteOperator.execute(context=None)
    
    @task
    def insertCarCategories(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_category_task')
        df = pd.read_json(data)
        df = df[['Brand', 'Category']]
        df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
        dfList = df.to_dict(orient='records')
        insertQuery = """Insert into car_categories(brand,category) values(?,?);"""
        
        for record in dfList:
            insert_operation = SqliteOperator(
                                              task_id='insert_categories_operation',
                                              sqlite_conn_id="my_sqlite_conn",
                                              sql=insertQuery,
                                              parameters=tuple(record.values())
                                              
                )
            insert_operation.execute(context=None)
    
    @task
    def join():
        # brand, model, price, category
        sqlite_operator = SqliteOperator(
                              task_id='join_table_task',
                              sqlite_conn_id='my_sqlite_conn',
                              sql="""CREATE TABLE IF NOT EXISTS car_details AS
                                     SELECT car_data.brand, 
                                            car_data.model, 
                                            car_data.price, 
                                            car_categories.category
                                      FROM car_data JOIN car_categories 
                                      ON car_data.brand = car_categories.brand;
                    """
            )
        sqlite_operator.execute(context=None)
     
    join_task = join()   
        
    read_csv() >> createCarTable() >> insertCarData()  >> join_task
    read_car_categories() >> createCarCategories() >> insertCarCategories() >> join_task
    
    # def write_csv(filteredTeslaData):
    #     df = pd.read_json(filteredTeslaData)
    #     df.to_csv("/home/ubuntu/environment/airflow/output/teslas.csv", index=False)
    
data_transformation_storage_pipeline()