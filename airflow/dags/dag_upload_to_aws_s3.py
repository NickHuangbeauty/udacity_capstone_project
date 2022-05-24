import email
import os
import re
import logging

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from airflow.providers.amazon.aws import LocalFilesystemToS3Operator

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


# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
}
