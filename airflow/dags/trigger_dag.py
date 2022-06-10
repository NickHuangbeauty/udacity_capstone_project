from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date': datetime(2022, 6, 6)
}

with DAG('trigger_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
    ) as dag:

    downloading = PythonOperator(
        task_id='downloading',
        python_callable=lambda: print('Downloading...')
    )

    #The execution_date
    # How to keep the same execution date between the trigger dag and the target dag?
    # → Might be useful to share XCOMs between DAGs(I do not recommend to do that)
    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target',
        trigger_dag_id='target_xcom_dag',
        execution_date= '{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
    )

    downloading >> trigger_target
