from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

# def _cleaning():
#     print('Clearning from target DAG')

# with DAG('target_dag',
#     schedule_interval='@daily',
#     default_args=default_args,
#     catchup=False) as dag:

#     storing = BashOperator(
#         task_id='storing',
#         bash_command='sleep 30'
#     )

#     cleaning = PythonOperator(
#         task_id='cleaning',
#         python_callable=lambda: print('Clearning from target DAG')
#     )

#     storing >> cleaning


def _training_model(**context):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    # push an Airflow XCom with airflow default key-value settings.
    # return accuracy
    # push an Airflow XCom with self.
    context['task_instance'].xcom_push(key='model_accuracy', value=accuracy)


def _choose_best_model(**context):
    print('choose best model')
    fetch = context['task_instance'].xcom_pull(
        key='model_accuracy', task_ids='training_model_A')
    print(f"training_model_A's model accuracy: {fetch}")


with DAG('target_xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"'
    )

    training_model_task = [
        PythonOperator(
            task_id=f'training_model_{task}',
            python_callable=_training_model
        ) for task in ['A', 'B', 'C']]

    choose_model = PythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

    downloading_data >> training_model_task >> choose_model
