from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta,time
import os
import sys
import pandas as pd
import numpy as mp

default_args={
    "owner":"airflow",
    "start_date":datetime(2021,6,17),
    #"retries":1,
    #"retry_delay":timedelta(minutes=2),
    "email":['vashisht3398@gmail.com'],
    "email_on_retry":False,
    "email_on_failure":True,
    "catchup":False
    }


def read_file(**kwargs):
    try:

        file_path = os.path.join(os.getcwd(),'dags/data_files/netflix_titles.csv')
        df =pd.read_csv(file_path)
        print(df.dtypes)
        print(df.head())
        #print(df.columns)
        print('/n/nWe will do processing in this dataset/n/n')
        kwargs['ti'].xcom_push(key="whole_dataframe",value=df)
    except Exception as e:
        print(e)


def delete_columns(**kwargs):
    try:
        file_path = os.path.join(os.getcwd(),'dags/data_files/netflix_titles.csv')
        df =pd.read_csv(file_path)
        df=kwargs['ti'].xcom_pull(key="whole_dataframe")
        print("\n\nThere are missing values in many columns :\n\n",df.isnull().sum()) 
        df.drop(['director','cast'],axis=1,inplace=True)
        kwargs['ti'].xcom_push(key="df_with_deleted_columns",value=df)
        return df
    except Exception as e:
        print(e)    



def add_column(**kwargs):
    df = kwargs['ti'].xcom_pull(key = "whole_dataframe")
    ratings_ages = {
    'TV-PG': 'Older Kids',
    'TV-MA': 'Adults',
    'TV-Y7-FV': 'Older Kids',
    'TV-Y7': 'Older Kids',
    'TV-14': 'Teens',
    'R': 'Adults',
    'TV-Y': 'Kids',
    'NR': 'Adults',
    'PG-13': 'Teens',
    'TV-G': 'Kids',
    'PG': 'Older Kids',
    'G': 'Kids',
    'UR': 'Adults',
    'NC-17': 'Adults'
    }
    df['target_ages'] = df['rating'].replace(ratings_ages)
    kwargs['ti'].xcom_push(key="added_targetages",value=df['target_ages'].to_list())
    print(df.dtypes)
    
    return df



def completed_task(**kwargs):
    df=kwargs.get("ti").xcom_pull(key="df_with_deleted_columns")

    df['target_ages'] =kwargs['ti'].xcom_pull(key="added_targetages")

    new_file_path=os.path.join(os.getcwd(),"dags/data_files/new_dataset.csv")

    print("\n\n\n",df.columns)
    df.to_csv(new_file_path)



with DAG("my_dag3",schedule_interval='@once',catchup=False,default_args=default_args) as dag:    

    read_file = PythonOperator(
        task_id ="read_file",
        python_callable= read_file,
        dag =dag\

    )
    delete_columns = PythonOperator(
        task_id = 'delete_columns',
        python_callable= delete_columns,
        dag =dag
    )
    add_column = PythonOperator(
        task_id = "add_column",
        python_callable=add_column,
        dag =dag
    )

    completed_task = PythonOperator(
        task_id ="completed_task",
        python_callable=completed_task,
        dag =dag
    )

read_file >> delete_columns
read_file >> add_column

delete_columns>> completed_task
add_column >> completed_task