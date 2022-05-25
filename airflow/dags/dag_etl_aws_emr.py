import os
import re
import logging
import datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


SPARK_STEP = {

}

# Format the json
JOB_FLOW_OVERRIDES = {

}

DEFAULT_ARGS = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_retry': False
}

DAG_ID = f"Step2:{os.join.basename(__file__).replace('.py', '')}"

logging.info("Starting DAG_ID: {DAG_ID}")


with DAG(DAG_ID,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval='@hour',
         tags=['Step2_aws_emr_to_s3']
         ) as dag:
    
    start = DummyOperator(task_id='start to Add  EMR Step')

    # Creates an EMR JobFlow, reading the config from the EMR connection.A dictionary of JobFlow overrides can be passed that override the config from the connection.
    create_job_flow = EmrCreateJobFlowOperator()

    # An operator that adds steps to an existing EMR job_flow.
    add_steps = EmrAddStepOperator()

    # Asks for the state of the step until it reaches any of the target states. If it fails the sensor errors, failing the task.
    wait_for_step = EmrStepSensor()