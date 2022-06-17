from airflow.models import DAG
from airflow.utils.helpers import chain
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator
)
from airflow.utils.task_group import TaskGroup

# This table variable is a placeholder, in a live environment, it is better
# to pull the table info from a Variable in a template
TABLE = Variable.get('TABLE')
DATES = Variable.get('DATES')

# By putting conn_id as a default_arg, the arg is passed to every task,
# reducing boilerplate
with DAG("sql_create_table",
         start_date=datetime(2021, 7, 7),
         description="A sample Airflow DAG to perform create data before quality checks using SQL Operators.",
         schedule_interval=None,
        #  default_args={"conn_id": "pg_conn"},
         template_searchpath="/Users/oneforall_nick/workspace/Udacity_capstone_project/airflow/sql/sql_samples/",
         catchup=False) as dag:
    """
    ### SQL Check Operators Data Quality Example

    Before running the DAG, ensure you have an active and reachable SQL database
    running, with a connection to that database in an Airflow Connection, and
    the data loaded. This DAG **will not** run successfully as-is. For an
    out-of-the-box working demo, see the sql_data_quality_redshift_etl DAG.

    Note: The data files for this example do **not** include an `upload_date`
    column. This column is needed for the interval check, and is added as a
    Task in sql_check_redshift_etl.py.
    """

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    """
    #### Run Table-Level Quality Check
    Ensure that the correct number of rows are present in the table.
    """
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='pg_conn',
        sql='create_redshift_yellow_tripdata_table.sql'
    )

    insert_table = PostgresOperator(
        task_id="insert_table",
        postgres_conn_id='pg_conn',
        sql='copy_data.sql'
    )

    data_check = SQLCheckOperator(
        task_id="data_check",
        conn_id='pg_conn',
        sql='SELECT COUNT(*) FROM {{ var.value.TABLE }}'
    )

    
    
    begin >> create_table >> insert_table >> data_check >> end