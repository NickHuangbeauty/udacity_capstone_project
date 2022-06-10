from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _training_model(**context):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    # push an Airflow XCom with airflow default key-value settings.
    # return accuracy
    # push an Airflow XCom with self.
    context['task_instance'].xcom_push(key='model_accuracy', value=accuracy)

def _choose_best_model(**context):
    print('choose best model')
    # fetch = context['task_instance'].xcom_pull(key='model_accuracy', task_ids='training_model_A')



with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

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

    std_out = BashOperator(
        task_id='std_out',
        bash_command='echo "{{ task_instance.xcom_pull(key=\'model_accuracy\', \
                                                       task_ids=[\'training_model_A\', \'training_model_B\', \'training_model_C\'])|last }}"'
    )

    downloading_data >> training_model_task >> choose_model >> std_out
