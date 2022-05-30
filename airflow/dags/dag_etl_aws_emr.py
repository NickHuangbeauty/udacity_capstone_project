import os
import logging
import datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

SPARK_STEP = {
    "Name": "Spark Step for ETL",
    "ActionONFailure": "CANCEL_AND_WAIT",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--packages",
            "spark-sas7bdat:3.0.0-s_2.12",
            "--deploy-mod",
            "client",
            "s3://s3://mydatapool/upload_data/script/data_spark_on_emr.py"
        ]
    }
}

# Format the json
JOB_FLOW_OVERRIDES = {

}

DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_retry': False
}

DAG_ID = f"Step2_{os.path.basename(__file__).replace('.py', '')}"

logging.info("Starting DAG_ID: {DAG_ID}")


with DAG(DAG_ID,
         default_args=DEFAULT_ARGS,
        #  maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won't create new active DAG runs
         max_active_runs=1,
         #  Perform scheduler catchup (or only run latest)? Defaults to True
         catchup=False,
         #  Schedule once and only once
         schedule_interval='@hour',
         #  List of tags to help filtering DAGs in the UI.
         tags=['Step2_aws_emr_to_s3']
         ) as dag:

    start = DummyOperator(task_id='Start to Add EMR Step')

    # TODO: Check create AWS EMR all connection and config for using AWS boto3 API to create EMR instances.

    # Creates an EMR JobFlow, reading the config from the EMR connection.A dictionary of JobFlow overrides can be passed that override the config from the connection.
    create_job_flow = EmrCreateJobFlowOperator(
        task_id='Create_Emr_Cluster',
        aws_conn_id='aws_default',
        emr_conn_id='aws_emr_default',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name='us-east-2'
    )

    # An operator that adds steps to an existing EMR job_flow.
    add_steps = EmrAddStepsOperator(
        task_id='Add_EMR_Step',
        aws_conn_id='aws_default',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create EMR Job Flow") }}',
        cluster_states=['WAITING'],
        steps=SPARK_STEP,
    )

    # Asks for the state of the step until it reaches any of the target states. If it fails the sensor errors, failing the task.
    wait_for_step = EmrStepSensor(
        task_id='Wait_for_Step',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create EMR Job Flow") }}',
        step_id='{{ task_instance.xcom_pull(task_ids="Add_EMR_Step") }}',
        aws_conn_id='aws_default',
        target_states=['COMPLETED'],
        failed_states=['FAILED'],
    )

    EmrTerminateJobFlowOperator(
        task_id='terminal_emr_cluster',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create EMR Job Flow") }}',
        aws_conn_id='aws_default',
    )

    end = DummyOperator(task_id='End to Add EMR Step')

    start >> create_job_flow >> add_steps >> wait_for_step >> EmrTerminateJobFlowOperator >> end
