import os
import logging
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from signal import signal, SIGPIPE, SIG_DFL
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ***** Without Catching SIGPIPE *********
signal(SIGPIPE, SIG_DFL)
# ****************************************

AWS_CONN_ID = 'aws_conn'


# Airflow variables
Bootstrap_Bucket = Variable.get('Bootstrap_Bucket')
Ec2_Key_Pair_Name = Variable.get('Ec2_Key_Pair_Name')
Ec2_Subnet_Id = Variable.get('Ec2_Subnet_Id')
Job_Flow_Role = Variable.get('Job_Flow_Role')
Log_Bucket = Variable.get('Log_Bucket')
Data_Bucket = Variable.get('Data_Bucket')
Service_Role = Variable.get('Service_Role')
Postgres_conn_DB = Variable.get('Postgres_conn_DB')
Data_Bucket_params = 'mydatapool'

DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'email_on_retry': False
}


# ******* SPARK_STEPS & JobFlow *******
# spark-sas7bdat: 2.0.0-s_2.11 -> Scala version: 2.11
SPARK_STEPS = [
    {
        "Name": "For Dealing with data and analytics using Spark on AWS EMR",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                "--deploy-mode",
                "client",
                "s3://{{ var.value.Data_Bucket }}/upload_data/script/data_spark_on_emr.py",
            ],
        },
    }
]


JOB_FLOW_OVERRIDES = {
    "Name": "Udacity_Capstone_ETL_On_EMR",
    "ReleaseLabel": "emr-5.36.0",
    "Applications": [
        {
            "Name": "Hadoop"
        },
        {
            "Name": "Spark"
        }
    ],
    "BootstrapActions": [
        {
            "Name": "bootstrap_emr",
            "ScriptBootstrapAction": {
                "Path": "s3://{{ var.value.Bootstrap_Bucket }}/bootstrap_emr.sh"
            }
        }
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ],
    "Instances": {
        "Ec2KeyName": "{{ var.value.Ec2_Key_Pair_Name }}",
        "Ec2SubnetId": "{{ var.value.Ec2_Subnet_Id }}",
        "InstanceGroups": [
            {
                "InstanceCount": 1,
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "Market": "ON_DEMAND",
                "Name": "Primary_Node"
            },
            {
                "InstanceCount": 2,
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "Market": "ON_DEMAND",
                "Name": "Core_Node_2"
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "{{ var.value.Job_Flow_Role }}",
    "LogUri": "s3://{{ var.value.Log_Bucket }}/emrlogs/",
    "ServiceRole": "{{ var.value.Service_Role }}",
    "VisibleToAllUsers": True
}

# *************************************


DAG_ID = f"Main_ETL_Process_{os.path.basename(__file__).replace('.py', '')}"

logging.info("Starting DAG_ID: {DAG_ID}")


with DAG(DAG_ID,
         description="aws_etl_emr_dag",
         default_args=DEFAULT_ARGS,
         #  maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won't create new active DAG runs
         max_active_runs=1,
         schedule_interval=None,
         catchup=False,
         #  List of tags to help filtering DAGs in the UI.
         tags=['main, aws_emr_to_s3, ETL, Pyspark, EMR']
         ) as dag:

    start = DummyOperator(task_id='Start')

    # Before start etl, I should remove all xcom records from postgres database
    postgres_clear_xcom_records = PostgresOperator(
        task_id='delete_xcom_task',
        postgres_conn_id='pg_conn',
        autocommit=True,
        sql=f"DELETE FROM xcom WHERE dag_id = '{DAG_ID}' AND run_id = '{{{{ dag_run.logical_date | ds }}}}';"
    )

    # Trigger 1: for upland etl_emr file from local to aws s3
    trigger_upload_etl_emr_to_s3 = TriggerDagRunOperator(
        task_id='Trigger_upload_etl_emr_step',
        trigger_dag_id='dag_upload_emr_script',
        execution_date= '{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED, State.UPSTREAM_FAILED]
    )

    # Trigger 2: for upland source and sas jars data from local to aws s3
    trigger_upload_source_data_to_s3 = TriggerDagRunOperator(
        task_id='Trigger_upload_source_data_step',
        trigger_dag_id='dag_upload_data_to_aws_s3',
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED, State.UPSTREAM_FAILED]
    )

    # Creates an EMR JobFlow, reading the config from the EMR connection.A dictionary of JobFlow overrides can be passed that override the config from the connection.
    create_job_flow = EmrCreateJobFlowOperator(
        task_id='Create_Emr_Cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name='us-west-2'
    )

    # An operator that adds steps to an existing EMR job_flow.
    add_steps = EmrAddStepsOperator(
        task_id='Add_EMR_Step',
        aws_conn_id=AWS_CONN_ID,
        job_flow_id='{{ task_instance.xcom_pull(key="return_value", task_ids="Create_Emr_Cluster") }}',
        steps=SPARK_STEPS,
        params={'Data_Bucket': Data_Bucket_params},
        do_xcom_push=True
    )

    # Data Check for Data Quality using SparkSubmitOperator

    # Asks for the state of the step until it reaches any of the target states. If it fails the sensor errors, failing the task.
    watch_step = EmrStepSensor(
        task_id='Add_Steps',
        job_flow_id='{{ task_instance.xcom_pull(key="return_value", task_ids="Create_Emr_Cluster") }}',
        step_id='{{ task_instance.xcom_pull(key="return_value", task_ids="Add_EMR_Step")|last }}',
        aws_conn_id=AWS_CONN_ID,
        failed_states=[State.FAILED, State.UPSTREAM_FAILED]
    )

    stop_and_remove_emr = EmrTerminateJobFlowOperator(
        task_id='terminal_emr_cluster',
        job_flow_id='{{ task_instance.xcom_pull(key="return_value", task_ids="Create_Emr_Cluster") }}',
        aws_conn_id=AWS_CONN_ID
    )

    end = DummyOperator(task_id='End_to_Add_EMR_Step')


    start >> postgres_clear_xcom_records >> [
        trigger_upload_etl_emr_to_s3, trigger_upload_source_data_to_s3] >> create_job_flow >> add_steps >> watch_step >> stop_and_remove_emr >> end
