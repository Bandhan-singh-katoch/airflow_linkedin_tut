from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

default_args = {'owner': 'admin'}


def testFunc():
    print("This is test function for python operator------------------------")


def printName(name):
    print("Name of persion is: {name}".format(name=name))
    return 999
    
def printNameWithAddress(name, address):
    print("The name of the person is: {name} and address is: {address}".format(name=name, address=address))
    return 786
    
with DAG(
         dag_id = 'python_operator_dag',
         description='Dag with python operator',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['python operator']
    ) as dag:
        task = PythonOperator(
                              task_id = 'task',
                              python_callable = testFunc
            )
            
        taskA = PythonOperator(
                               task_id = 'taskA',
                               python_callable = printName,
                               op_kwargs = {'name':'Bandhan'}
            )
            
        taskB = PythonOperator(
                              task_id = 'taskB',
                              python_callable = printNameWithAddress,
                              op_kwargs = {'name':'Bandhan singh', 'address': 'Jammu and kashmir'}
            )

task >> taskA
taskA >> taskB