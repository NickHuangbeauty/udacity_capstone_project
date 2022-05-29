import os
import re
import logging
import datetime

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

# https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


BUCKET_KEY = 's3://mydatapool/upload_data/'  # Path to the S3 bucket
DEST_BUCKET = 'mydatapool'
UPLOAD_EMR_FILE = 's3://mydatapool/upload_data/script/'

# ******************** access each data by filepath **********************
filepath = '/Users/oneforall_nick/workspace/Udacity_capstone_project/airflow/data'

# ****** local data absolute path which is uploaded to S3 ******
filepath_all = [os.path.join(root, file) for root, dirs, files in os.walk(filepath) for file in files]

# ****** get the task name, s3_ky and file name from the file_path ******
files = [file_ for root, dirs, files in os.walk(filepath) for file_ in files]

# s3 key where is saved upload of destination of aws s3 location
s3_key_filename = [re.search(r'/data/*.*', each_filepath)[0] for each_filepath in filepath_all]

each_file = [re.search(r'(^.+\.)', files[i])[0] + str(i) for i in range(len(files)) ]

files_path = list(zip(each_file, s3_key_filename, filepath_all))
# ************************************************************************


# ******************** access emr file by filepath **********************
emr_filepath = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/data_spark_on_emr.py'
# ************************************************************************


# Start: DAG
DAG_ID = f"Step1:{os.path.basename(__file__).replace('.py', '')}" # This file name.

# Default args for DAG
DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_retry': False
}


logging.info(f"Starting DAG_ID: {DAG_ID}")

with DAG(DAG_ID,
         description="upload_to_aws_s3",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval='@hour',
         tags=['Step1_upload_to_aws_s3']) as dag:

    start = DummyOperator(task_id='Start load data from local to aws s3')

    # Task Group
    with TaskGroup(task_id='upload_to_aws_s3',
                   provide_context=True,
                   max_active_runs=1) as task_group_upload_to_aws_s3:

        logging.info("Start to upload files to aws s3: data_spark_on_emr")

        # Upload data_spark_on_emr.py from local to aws s3
        upload_emr_file_from_local_to_s3 = LocalFilesystemToS3Operator(
            task_id='upload_emr_file_from_local_to_s3',
            # local target file path
            source_path=emr_filepath,
            dest_key=UPLOAD_EMR_FILE,
            # Destination bucket key
            dest_bucket=DEST_BUCKET,
            replace=True,
            task_group=task_group_upload_to_aws_s3
        )

        logging.info("Completely to upload files to aws s3: data_spark_on_emr")

        for each_filepath in files_path:
            # Show log for each task
            logging.info(f"Uploading: {each_filepath[0]}")

            upload_data_from_local_to_s3 = LocalFilesystemToS3Operator(
                task_id=f"upload_to_s3_{each_filepath[0]}",
                source_path=each_filepath[2],
                destination_path=each_filepath[1],
                bucket_key=BUCKET_KEY,
                dest_bucket=DEST_BUCKET,
                replace=True,
                task_group=task_group_upload_to_aws_s3
            )

    end = DummyOperator(task_id='Completely load data and emr file from local to aws s3')

    # schedule for this dag processes
    start >> task_group_upload_to_aws_s3 >> end