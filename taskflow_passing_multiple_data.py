import time
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import json

default_args = {'owner': 'admin'}

@dag(
         dag_id = 'taskflow_pass_data_dag',
         description='passing data with taskflow',
         default_args = default_args,
         start_date = days_ago(1),
         schedule_interval = '@once',
         tags = ['taskflow', 'decorator']
    )
def passing_data_with_taskflow():
    @task
    def getPrices():
        orders = {
            'o1':11,
            'o2':12,
            'o3':21,
            'o4':22,
            'o5':34,
            'o6':10
        }
        return orders
    
    @task(multiple_outputs=True)
    def computeSumAndAvg(orderData: dict):
        total = 0
        count=0
        for x in orderData:
            total += orderData[x]
            count +=1
            
        return {'total': total, 'avgPrice': total/count}
    
    @task
    def displayResult(totalSum, avgPrice):
        print('totalSum===',totalSum)
        print('avgPrice===',avgPrice)
    
    result = computeSumAndAvg(getPrices())
    displayResult(result['total'], result['avgPrice'])
    
  
passing_data_with_taskflow()