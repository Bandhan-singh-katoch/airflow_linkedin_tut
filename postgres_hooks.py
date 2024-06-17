from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {'owner': 'admin'}

employees_table = Variable.get("emp", default_var=None)
department_table = Variable.get("dept", default_var=None)
employee_department_table = Variable.get("emp_dep", default_var=None)

employees = [
        ('John', 'Doe', 25, 1), 
        ('Sarah', 'Jane', 33, 1), 
        ('Emily', 'Kruglick', 27, 2), 
        ('Katrina', 'Luis', 27, 2), 
        ('Peter', 'Gonsalves', 30, 1),
        ('Nancy', 'Reagan', 43, 3)
    ]
departments = ['Engineering', 'Marketing', 'Sales']

def create_emp_table():
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""
                create table if not exists {employees_table}(
                   id SERIAL PRIMARY KEY,
                   first_name VARCHAR(50) NOT NULL,
                   last_name VARCHAR(50) NOT NULL,
                   age INTEGER NOT NULL,
                   department_id INTEGER NOT NULL
                );
    """
    pg_hook.run(sql=query)

def create_dept_table():
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""
                create table if not exists {department_table}(
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL
                );
    """
    pg_hook.run(sql=query)
    
def insert_emp_data(employees):
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""Insert into {employees_table}(first_name, last_name, age, department_id) values(%s,%s,%s,%s)"""
    
    for employee in employees:
        first_name, last_name, age, department_id = employee
        
        pg_hook.run(sql=query, parameters=(first_name, last_name, age, department_id))

def insert_dept_data(departments):
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""Insert into {department_table} (name) values(%s);"""
    
    for department in departments:
        name = department
        pg_hook.run(sql=query, parameters=(name,)) 

def emp_dept_table():
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""
                 create table if not exists {employee_department_table} as 
                  select {employees_table}.first_name as first_name, {employees_table}.last_name as last_name, {employees_table}.age as age, {department_table}.name as department_name 
                  from {employees_table} join {department_table} on {employees_table}.department_id = {department_table}.id;
    """
    pg_hook.run(sql=query)

def fetch_emp_dept_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""select * from {employee_department_table}"""
    results = pg_hook.get_records(sql=query)
    
    for row in results:
        print(row)

def filter_emp_dept_table(condition):
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    query = f"""
               select * from {employee_department_table} where department_name='{condition}'
    """
    results = pg_hook.get_records(sql=query)
    
    for row in results:
        print(row)
    
with DAG(
         dag_id = 'postgres_pipeline',
         description='Running postgres pipeline',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['pipeline', 'postgres']
    ) as dag:
        create_emp_table_task =  PythonOperator(
               task_id="create_emp_table_task",
               python_callable=create_emp_table
            )
        create_dept_table_task = PythonOperator(
               task_id="create_dept_table_task",
               python_callable=create_dept_table
            )
            
        insert_emp_data_task = PythonOperator(
               task_id="insert_emp_data_task",
               python_callable=insert_emp_data,
               op_kwargs={'employees': employees}
            )
        insert_dept_data_task = PythonOperator(
               task_id="insert_dept_data_task",
               python_callable=insert_dept_data,
               op_kwargs={'departments':departments}
            )
        emp_dept_table_task = PythonOperator(
               task_id="emp_dept_table_task",
               python_callable=emp_dept_table
            )
        fetch_emp_dept_data_task = PythonOperator(
               task_id="fetch_emp_dept_data_task",
               python_callable=fetch_emp_dept_data
            )
        filter_emp_dept_table_task = PythonOperator(
               task_id="filter_emp_dept_table_task",
               python_callable=filter_emp_dept_table,
               op_kwargs={'condition': 'Engineering'}
            )

create_emp_table_task >> insert_emp_data_task >> emp_dept_table_task

create_dept_table_task >> insert_dept_data_task >> emp_dept_table_task

emp_dept_table_task >> fetch_emp_dept_data_task >> filter_emp_dept_table_task 