import os
import re
import logging
import configparser
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import UploadFilesFromLocalToS3

# https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


# ******* Access AWS Services *******
AWS_CONN_ID = 'aws_conn'
# config = configparser.ConfigParser()
# config.read_file(open('/Users/oneforall_nick/.aws/credentials'))
# os.environ['AWS_ACCESS_KEY_ID'] = config['default']['aws_access_key_id']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['aws_secret_access_key']
# **********************************

# ******* ETL file information *******
Data_Bucket = Variable.get('Data_Bucket', default_var=None)
# DEST_BUCKET = 'mydatapool'
# UPLOAD_EMR_FILE = 's3://mydatapool/upload_data/script/'
# ************************************

# ******************** Access config file by filepath **********************
ETL_EMR_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/'
dict_etl_file_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(
    ETL_EMR_FILE_PATH) for file_ in files if file_.endswith('.py'))
# e.g. {'~/workspace/Udacity_capstone_project/aws_emr_steps/data_spark_on_emr.py': 'data_spark_on_emr'}
# **************************************************************************

# Start: DAG
# This file name.
DAG_ID = f"{os.path.basename(__file__).replace('.py', '')}"

# Default args for DAG
DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}


logging.info(f"Starting DAG_ID: {DAG_ID}")

with DAG(DAG_ID,
         description="upload_emr_etl_script_to_aws_s3",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval=None,
         tags=['step2, upload, emr_etl_script, aws_s3']) as dag:

    start = DummyOperator(
        task_id='Start_upload_emr_script_from_local_to_aws_s3')

# # ***** Upland ETL_EMR files from local to AWS S3 *****
    for i in dict_etl_file_info.items():
        upload_emr_script_file = UploadFilesFromLocalToS3(
            task_id=f'upload_etl_{i[1]}_from_local_to_s3',
            s3_bucket=Data_Bucket,
            s3_key=f'upload_data/script/{i[1]}.py',
            filename_dict=dict_etl_file_info,
            aws_conn_id=AWS_CONN_ID,
            replace=True
        )

    end = DummyOperator(
        task_id='Completely_upload_ETLdata_from_local_to_aws_s3')

    start >> upload_emr_script_file >> end
