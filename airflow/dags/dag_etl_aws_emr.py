import os
import json
import logging
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators import UploadJsonFileFromLocalToS3
from airflow.operators.python import BranchPythonOperator

AWS_CONN_ID = 'aws_conn'

JOB_FLOW_AWS_S3_KEY = 'job_flow/job_flow_overrides.json'
SPARK_STEPS_AWS_S3_KEY = 'aws_emr_steps/aws_emr_steps.json'
BUCKET_NAME = 'mydatapool'


DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

# Get Job Flow and Spark Step Json files from AWS S3
def get_job_flow(s3_key, bucket_name):
    """
    Purpose:
        Get the job flow from the variable.
    :param s3_key:              s3 key
    :type s3_key:               string
    :param bucket_name:         AWS S3 bucket name
    :type bucket_name:          string
    return:                     json file as a object
    """
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    file_content = hook.get_key(key=s3_key, bucket_name=bucket_name)
    file_content = file_content.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def how_to_do_branch(**context) -> str:
    """
    Purpose:
        Check json files are available to upload to aws s3.

    Returns:
        str: files are success uploaded to s3: return completed upload files
             files are not success uploaded to s3: return failed upload files
    """
    fetched_upload_filename = context['task_instance'].xcom_pull(key=['job_flow_overrides', 'aws_emr_steps'],
                                                                 task_id=['upload_job_config_json_file_from_local_to_s3',
                                                                          'upload_spark_step_json_file_from_local_to_s3'])
    for filename in range(len(fetched_upload_filename)):
        if fetched_upload_filename[filename] in ['job_flow_overrides', 'aws_emr_steps']:

            return 'completed upload files'
        else:
            return 'failed upload files'


# ******* SPARK_STEPS & JobFlow *******
SPARK_STEPS = [
    {
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ var.value.SAS_Jars_Bucket }}/spark-sas7bdat-3.0.0-s_2.12.jar",
                "--dest=/usr/lib/spark/jars"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": "Upload sas jars file from local to aws s3"
    },
    {
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Args": [
                "Spark-Submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "--name",
                "data_spark_on_emr",
                "s3://{{ var.value.Bootstrap_Bucket }}/data_spark_on_emr.py"
            ],
            "Jar": "command-runner.jar"
        },
        "Name": "For Dealing with data and analytics using Spark on AWS EMR"
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
                "Path": "s3://{{ var.value.Bootstrap_Bucket }}/bootstrap.sh"
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
        "Ec2KeyName": "{{ var.value.Ec2_Key_Name }}",
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
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False
    },
    "JobFlowRole": "{{ var.value.Job_Flow_Role }}",
    "LogUri": "s3://{{ var.value.Log_Bucket }}/emrlogs/",
    "Name": "Udacity_Capstone_Spark_On_EMR",
    "ReleaseLabel": "emr-5.29.0",
    "ServiceRole": "{{ var.value.Service_Role }}",
    "VisibleToAllUsers": True
}

# *************************************


DAG_ID = f"Step2_{os.path.basename(__file__).replace('.py', '')}"

logging.info("Starting DAG_ID: {DAG_ID}")


with DAG(DAG_ID,
         description="aws_etl_emr_dag",
         default_args=DEFAULT_ARGS,
         #  maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won't create new active DAG runs
         max_active_runs=1,
         #  Perform scheduler catchup (or only run latest)? Defaults to True
         catchup=False,
         #  Schedule once and only once
         schedule_interval='0 * * * *',
         #  List of tags to help filtering DAGs in the UI.
         tags=['Step2_aws_emr_to_s3, ETL, Pyspark, EMR']
         ) as dag:

    start = DummyOperator(task_id='Start')


    # Check if json files are uploaded from local to AWS S3 or not!
    how_to_do_next_step = BranchPythonOperator(
        task_id='how_to_do_branch',
        python_callable=how_to_do_branch,
    )

    start_emr_step = DummyOperator(task_id='Start_to_EMR_Step')

    # Check create AWS EMR all connection and config for using AWS boto3 API to create EMR instances.

    # Creates an EMR JobFlow, reading the config from the EMR connection.A dictionary of JobFlow overrides can be passed that override the config from the connection.
    create_job_flow = EmrCreateJobFlowOperator(
        task_id='Create_Emr_Cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name='us-east-2'
    )

    # An operator that adds steps to an existing EMR job_flow.
    add_steps = EmrAddStepsOperator(
        task_id='Add_EMR_Step',
        aws_conn_id='aws_default',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create_Emr_Cluster", key="return_value") }}',
        steps=SPARK_STEPS
    )

    # Asks for the state of the step until it reaches any of the target states. If it fails the sensor errors, failing the task.
    wait_for_step = EmrStepSensor(
        task_id='Add_Steps',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create_Emr_Cluster", key="return_value") }}',
        step_id='{{ task_instance.xcom_pull(task_ids="Add_EMR_Step", key="return_value")[0] }}',
        aws_conn_id=AWS_CONN_ID
    )

    # terminal_job = EmrTerminateJobFlowOperator(
    #     task_id='terminal_emr_cluster',
    #     job_flow_id='{{ task_instance.xcom_pull(task_ids="Create_Emr_Cluster", key="return_value")) }}',
    #     aws_conn_id=AWS_CONN_ID
    # )

    end = DummyOperator(task_id='End_to_Add_EMR_Step')

    no_reachable = DummyOperator(task_id='No_Reachable_Step')

    # upload_job_config_json_file >> upload_spark_step_json_file >> how_to_do_next_step >> start >> create_job_flow >> add_steps >> wait_for_step >> terminal_job >> end
    start >> how_to_do_next_step >> start_emr_step >> create_job_flow >> add_steps >> wait_for_step >> end
    start >> how_to_do_next_step >> start_emr_step >> create_job_flow >> add_steps >> wait_for_step >> no_reachable
