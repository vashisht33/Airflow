from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint

def training_model():
    return randint(1,10)

def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids = [
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    best_accuracy = max(accuracies)
    
    if(best_accuracy > 8):
        return 'accurate'
    return 'Inaccurate'    

with DAG("my_dag",start_date=datetime(2021,1,1),schedule_interval="@daily",catchup=False) as dag:
    training_model_A = PythonOperator(
        task_id = "training_model_A",
        python_callable = training_model
    )

    training_model_B = PythonOperator(
        task_id = "training_model_B",
        python_callable = training_model
    )

    training_model_C = PythonOperator(
        task_id = "training_model_C",
        python_callable = training_model
    )

    best_model = BranchPythonOperator(
        task_id ="best_model",
        python_callable = choose_best_model
    )

    accurate = BashOperator(
        task_id = "accurate",
        bash_command="echo 'accurate'"
    )

    Inaccurate = BashOperator(
        task_id = "Inaccurate",
        bash_command="echo 'Inaccurate'"
    )


    [training_model_A,training_model_B,training_model_C]>> best_model >>[accurate,Inaccurate]