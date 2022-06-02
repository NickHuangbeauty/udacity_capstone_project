import os
import re
import logging
import configparser
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from operators import UploadJsonFileFromLocalToS3

# https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from operators import UploadJsonFileFromLocalToS3


# ******* Access AWS Server *******
AWS_CONN_ID = 'aws_conn'
config = configparser.ConfigParser()
config.read_file(open('/Users/oneforall_nick/.aws/credentials'))
os.environ['AWS_ACCESS_KEY_ID'] = config['default']['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['aws_secret_access_key']
# **********************************


# Path to the S3 bucket
DEST_BUCKET = 'mydatapool'
JOB_FLOW_AWS_S3_KEY = 'job_flow/job_flow_overrides.json'
SPARK_STEPS_AWS_S3_KEY = 'aws_emr_steps/aws_emr_steps.json'

JOB_FLOW_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/job_flow/'
dict_job_file_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(
    JOB_FLOW_FILE_PATH) for file_ in files if file_.endswith('.json'))

SPARK_STEP_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/'
dict_spark_file_info = dict([os.path.join(root, file_), file_.split(".")[0]] for root, dirs, files in os.walk(
    SPARK_STEP_FILE_PATH) for file_ in files if file_.endswith('.json'))


# Start: DAG
# This file name.
DAG_ID = f"{os.path.basename(__file__).replace('.py', '')}"

# Default args for DAG
DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

logging.info(f"Starting DAG_ID: {DAG_ID}")

with DAG(DAG_ID,
         description="upload_json_files_to_aws_s3",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval='0 * * * *',
         tags=['Step1_upload_json_files_to_aws_s3']) as dag:

    start = DummyOperator(task_id='Start_upload_files_from_local_to_aws_s3')

    # Task Group
    with TaskGroup(group_id='upload_json_files') as task_group_upload_json_files:

        logging.info("Start to upload files to aws s3: job flow and spark spark step files")

        # Upload Job flow file from local to aws s3
        upload_job_flow_file_from_local_to_s3 = UploadJsonFileFromLocalToS3(
            task_id='upload_job_flow_file_from_local_to_s3',
            s3_key=JOB_FLOW_AWS_S3_KEY,
            s3_bucket=DEST_BUCKET,
            filename_dict=dict_job_file_info,
            aws_conn_id=AWS_CONN_ID,
            replace=True
        )

        # Upload Spark Step file from local to aws s3
        upload_spark_step_file_from_local_to_s3 = UploadJsonFileFromLocalToS3(
            task_id='upload_spark_step_file_from_local_to_s3',
            s3_key=SPARK_STEPS_AWS_S3_KEY,
            s3_bucket=DEST_BUCKET,
            filename_dict=dict_spark_file_info,
            aws_conn_id=AWS_CONN_ID,
            replace=True
        )

        logging.info("Completely to upload files to aws s3: job flow and spark spark step files")

    end = DummyOperator(task_id='Completed_upload_job_and_spark_step_files_from_local_to_aws_s3')

    start >> task_group_upload_json_files >> end
