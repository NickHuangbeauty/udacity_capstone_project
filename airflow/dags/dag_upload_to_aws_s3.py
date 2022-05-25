import email
import os
import re
import logging
import datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

# https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


filename = '' # Path the local file
bucket_key = 's3://mydatapool/upload_data/'  # Path to the S3 bucket
dest_bucket = 'mydatapool'


# access each data by filepath
filepath = '/Users/oneforall_nick/workspace/Udacity_capstone_project/airflow/data'

# local data absolute path which is uploaded to S3
filepath_all = [os.path.join(root, file) for root, dirs, files in os.walk(filepath) for file in files]


files = [file_ for root, dirs, files in os.walk(filepath) for file_ in files]

# s3 key where is saved upload of destination of aws s3 location
s3_key_filename = [re.search(r'/data/*.*', each_filepath)[0] for each_filepath in filepath_all]

each_file = [re.search(r'(^.+\.)', files[i])[0] + str(i) for i in range(len(files)) ]

files_path = list(zip(each_file, s3_key_filename, filepath_all))

DAG_ID = f"Step1:{os.path.basename(__file__).replace('.py', '')}" # This file name.

# Default args for DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_retry': False
}


with DAG(DAG_ID,
         description="upload_to_aws_s3",
         default_args=default_args,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval='@hour',
         tags=['test_v1']) as dag:

    start = DummyOperator(task_id='Start load data from local to aws s3')

    # Task Group
    with TaskGroup(task_id='upload_to_aws_s3',
                   provide_context=True,
                   max_active_runs=1) as task_group_upload_to_aws_s3:

        for each_filepath in files_path:
            # Task
            task = LocalFilesystemToS3Operator(
                task_id=f"upload_to_s3_{each_filepath[0]}",
                source_path=each_filepath[2],
                destination_path=each_filepath[1],
                bucket_key=bucket_key,
                dest_bucket=dest_bucket,
                replace=True,
                task_group=task_group_upload_to_aws_s3
            )

    end = DummyOperator(task_id='Completely load data from local to aws s3')

        # schedule for this dag processes
    start >> task_group_upload_to_aws_s3 >> end