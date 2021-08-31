
from os import error


try:
    import os
    import pandas as pd
    from airflow import DAG
    from datetime import datetime,timedelta
    from airflow.operators.python import PythonOperator
    print("Packages imported successfully")

except Exception as e:
    print("error is {}".format(e))    


dag = DAG("my_dag_1",schedule_interval= '@daily',default_args={'owner':'airflow','depends_on_past':False,'start_date':datetime(2021,7,7),'retries':0},catchup =False) 

def first_fun():
    
        file_path = os.path.join(os.getcwd(),'dags/data_files/gender_submission.csv')
        df =pd.read_csv(file_path)
        print('\n\nFile red successfully\n\n')
        print(df)
   
#first_fun()
def second_fun():
    print("Done")

task_1 = PythonOperator(
    task_id = 'task_1',
    python_callable = first_fun,
    dag = dag
)

task_2 = PythonOperator(
    task_id = 'task_2',
    python_callable= second_fun,
    dag = dag
)

task_1 >> task_2