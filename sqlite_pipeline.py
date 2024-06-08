from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator

default_args = {'owner': 'admin'}

with DAG(
         dag_id = 'sqlite_pipeline',
         description='Running sqlite pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'sqlite']
    ) as dag:
        create_table = SqliteOperator(
                                      task_id='create_table',
                                      sql= r'''
                                              create table if not exists users(
                                                id Integer primary key,
                                                name varchar(50) not null,
                                                age integer not null,
                                                city varchar(50),
                                                is_active boolean default true,
                                                created_at timestamp default current_timestamp
                                              );
                                      ''',
                                      sqlite_conn_id='my_sqlite_conn',
                                      dag=dag
            )
        
        insert_values_1 = SqliteOperator(
                                         task_id='insert_values_1',
                                         sql= r'''
                                                  insert into users(name, age,is_active)
                                                  values('Bandhan',23,true),
                                                  ('pankaj', 34, false),
                                                  ('jot', 20, false);
                                         ''',
                                         sqlite_conn_id='my_sqlite_conn',
                                         dag=dag
            )
        insert_values_2 = SqliteOperator(
                                         task_id='insert_values_2',
                                         sql= r'''
                                                  insert into users(name, age)
                                                  values('ashu',23),
                                                  ('anu', 19),
                                                  ('kamal', 15);
                                         ''',
                                         sqlite_conn_id='my_sqlite_conn',
                                         dag=dag
            )
            
        delete_values = SqliteOperator(
                                      task_id='delete_values',
                                      sql=r'''delete from users where is_active=0;''',
                                      sqlite_conn_id='my_sqlite_conn',
                                      dag=dag
            )
            
        update_values = SqliteOperator(
                                      task_id='update_values',
                                      sql=r'''update users set city = 'Jammu';''',
                                      sqlite_conn_id='my_sqlite_conn',
                                      dag=dag
            )
            
        display_result = SqliteOperator(
                                        task_id='display_result',
                                        sql=r'''select * from users''',
                                        sqlite_conn_id='my_sqlite_conn',
                                        dag=dag,
                                        do_xcom_push=True
            )
            
create_table >> [insert_values_1, insert_values_2] >> delete_values >> update_values >> display_result