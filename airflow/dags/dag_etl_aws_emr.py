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
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


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
# def how_to_do_branch(**context) -> str:
#     # TODO: Dealing with is xcom_pull!!
#     """
#     Purpose:
#         Check json files are available to upload to aws s3.

#     Returns:
#         str: files are success uploaded to s3: return completed upload files
#              files are not success uploaded to s3: return failed upload files
#     """
#     fetched_upload_filename = context['task_instance'].xcom_pull(key=['job_flow_overrides', 'aws_emr_steps'],
#                                                                  task_id=['upload_job_config_json_file_from_local_to_s3',
#                                                                           'upload_spark_step_json_file_from_local_to_s3'])
#     for filename in range(len(fetched_upload_filename)):
#         if fetched_upload_filename[filename] in ['job_flow_overrides', 'aws_emr_steps']:

#             return 'completed upload files'
#         else:
#             return 'failed upload files'



# ******* SPARK_STEPS & JobFlow *******
SPARK_STEPS = [
    {
        "Name": "For Dealing with data and analytics using Spark on AWS EMR",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ var.value.Data_Bucket }}/upload_data/script/data_spark_on_emr.py",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
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
    "Name": "Udacity_Capstone_Spark_On_EMR",
    "ReleaseLabel": "emr-5.36.0",
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

    # Before start etl, I should remove all xcom records from postgres database
    postgres_clear_xcom_records = PostgresOperator(
        task_id='delete_xcom_task',
        postgres_conn_id='pg_conn',
        autocommit=True,
        sql=f"DELETE FROM xcom WHERE dag_id = '{DAG_ID}' AND execution_date = '{{{{ dag_run.logical_date | ds }}}}';"
    )

    start = DummyOperator(task_id='Start')

    # Check if json files are uploaded from local to AWS S3 or not!
    # how_to_do_next_step = BranchPythonOperator(
    #     task_id='how_to_do_branch',
    #     python_callable=how_to_do_branch,
    # )

    # Trigger 1: for upland etl_emr file from local to aws s3
    trigger_upload_etl_emr_to_s3 = TriggerDagRunOperator(
        task_id='Trigger_upload_etl_emr_step',
        trigger_dag_id='dag_upload_emr_script',
        execution_date= '{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=[State.SUCCESS, State.RUNNING],
        failed_states=[State.FAILED, State.UPSTREAM_FAILED]
    )

    # Trigger 2: for upland source and sas jars data from local to aws s3
    # trigger_upload_source_data_to_s3 = TriggerDagRunOperator(
    #     task_id='Trigger_upload_source_data_step',
    #     trigger_dag_id='dag_upload_data_to_aws_s3',
    #     execution_date='{{ ds }}',
    #     reset_dag_run=True,
    #     wait_for_completion=True,
    #     poke_interval=15,
    #     allowed_states=[State.SUCCESS, State.RUNNING],
    #     failed_states=[State.FAILED, State.UPSTREAM_FAILED]
    # )

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
        failed_states=[State.FAILED, State.UPSTREAM_FAILED, State.UP_FOR_RETRY]
    )

    stop_and_remove_emr = EmrTerminateJobFlowOperator(
        task_id='terminal_emr_cluster',
        job_flow_id='{{ task_instance.xcom_pull(key="return_value", task_ids="Create_Emr_Cluster") }}',
        aws_conn_id=AWS_CONN_ID
    )

    end = DummyOperator(task_id='End_to_Add_EMR_Step')

    # no_reachable = DummyOperator(task_id='No_Reachable_Step')


    # start >> [trigger_upload_etl_emr_to_s3, trigger_upload_source_data_to_s3] >> how_to_do_next_step >> create_job_flow >> add_steps >> wait_for_step >> terminal_job >> end

    # start >> [trigger_upload_etl_emr_to_s3, trigger_upload_source_data_to_s3] >> how_to_do_next_step >> no_reachable

    start >> postgres_clear_xcom_records >> trigger_upload_etl_emr_to_s3 >> create_job_flow >> add_steps >> watch_step >> stop_and_remove_emr >> end
    # start >> postgres_clear_xcom_records >> create_job_flow >> add_steps >> watch_step >> stop_and_remove_emr >> end
