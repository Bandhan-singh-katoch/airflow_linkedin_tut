from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {'owner': 'admin'}


def increment_by_1(value):
    print("{value} is to be incremented by 1".format(value=value))
    return value+1
    
def multiply_by_10(ti):
    value = ti.xcom_pull(task_ids='increment_by_1')
    print("value in multiply_by_10 func is {value}".format(value=value))
    return value*10
    
def subtract_by_2(ti):
    value = ti.xcom_pull(task_ids='multiply_by_10')
    print("value in subtract_by_2 func is {value}".format(value=value))
    return value-2
    
def printValue(ti):
    value = ti.xcom_pull(task_ids='subtract_by_2')
    print("value in printValue func is {value}".format(value=value))
    
with DAG(
         dag_id = 'xcom_tut',
         description='Dag with python operator and xcom',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = timedelta(days=1),
         tags = ['python operator']
    ) as dag:
        increment_by_1 = PythonOperator(
                                        task_id='increment_by_1',
                                        python_callable=increment_by_1,
                                        op_kwargs={'value':3}
            
            )
        multiply_by_10 = PythonOperator(
                                        task_id='multiply_by_10',
                                        python_callable=multiply_by_10
            
            )
        
        subtract_by_2 = PythonOperator(
                                        task_id='subtract_by_2',
                                        python_callable=subtract_by_2
            
            )
            
        print_value = PythonOperator(
                                        task_id='printValue',
                                        python_callable=printValue
            
            )

increment_by_1 >> multiply_by_10 >> subtract_by_2 >> print_value