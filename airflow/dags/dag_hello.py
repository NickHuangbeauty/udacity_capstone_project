import datetime


from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime.datetime(2019, 1, 1),
    schedule_interval="@daily",
) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
    # t2 = BashOperator(task_id="sleep", bash_command="sleep 5")
    t3 = BashOperator(task_id="print1_date", bash_command="date")
    
    t1 >> t3