from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


def get_dag_ids():
    postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
    records = postgres_hook.get_records(sql="select * from xcom")
    print(records)


with DAG(
    "connect_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    # catchup=False # enable if you don't want historical dag runs to run
) as dag:

    t1 = PythonOperator(
        task_id="get_dag_nums",
        python_callable=get_dag_ids,
    )