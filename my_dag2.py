from copy import error
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import requests
import json

url = 'https://httpbin.org/post'
default_args ={
     'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag =DAG('my_dag2',default_args=default_args,start_date=datetime(2021,1,1),schedule_interval=timedelta(minutes=30),max_active_runs=2)
def api_data(ti):
    try:
        res= requests.post(url)
        ans = res.json()
        print("\n\nResponse recieved from api is\n\n",ans)
        ti.xcom_push(key = "send_origin_data",value = ans)
    except Exception as e:
        print(error)

def analyze_api_data(ti):
    try:
        pulling_response = ti.xcom_pull(key ="send_origin_data",task_id ='task_1')
        pulled_response = pulling_response['origin']
        print("\n\nThe origin of the API request is\n\n",pulled_response)
    except Exception as e:
        print(error)

task_1 = PythonOperator(
    task_id = "task_1",
    python_callable = api_data,
    dag=dag
)    

task_2 =PythonOperator(
    task_id ="task_2",
    python_callable=analyze_api_data,
    dag=dag
)

task_1 >> task_2
