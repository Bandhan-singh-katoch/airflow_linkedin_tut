from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import csv

default_args = {'owner': 'admin'}


def write_into_csv(ti):
    filtered_data = ti.xcom_pull(task_ids='filtering_task')
    
    with open('/home/ubuntu/environment/airflow/output/filtered_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Name', 'Product', 'Price'])
        
        for row in filtered_data:
            writer.writerow(row)
    
with DAG(
         dag_id = 'postgres_pipeline',
         description='Running postgres pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'postgres'],
         template_searchpath='/home/ubuntu/environment/airflow/sql_statements'
    ) as dag:
        
        create_customer_task = PostgresOperator(
                         task_id='create_customer_task',
                         postgres_conn_id='postgres_connection',
                         sql='create_table_customers.sql'
            )
            
        create_customer_purchase_task = PostgresOperator(
                         task_id='create_customer_purchase_task',
                         postgres_conn_id='postgres_connection',
                         sql='create_table_customer_purchases.sql'
            )
        
        insert_customers_task = PostgresOperator(
                         task_id='insert_customers_task',
                         postgres_conn_id='postgres_connection',
                         sql='insert_customers.sql'
            )
            
        insert_customer_purchases_task = PostgresOperator(
                         task_id='insert_customer_purchases_task',
                         postgres_conn_id='postgres_connection',
                         sql='insert_customer_purchases.sql'
            )
            
        join_task = PostgresOperator(
                         task_id='join_task',
                         postgres_conn_id='postgres_connection',
                         sql='joining_table.sql'
            )
            
        filtering_task = PostgresOperator(
                         task_id='filtering_task',
                         postgres_conn_id='postgres_connection',
                         sql='''select name,product, price from complete_customer_details where price between %(lower_bound)s and %(upper_bound)s''',
                         parameters={'lower_bound': 5, 'upper_bound':9}
            )
            
        write_into_csv_task = PythonOperator(
                        task_id='write_into_csv_task',
                        python_callable=write_into_csv
            )
            
create_customer_task >> create_customer_purchase_task >> insert_customers_task >> insert_customer_purchases_task \
>> join_task >> filtering_task >> write_into_csv_task