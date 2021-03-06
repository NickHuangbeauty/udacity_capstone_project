import os
import re
import logging
import configparser
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from operators import UploadFilesFromLocalToS3

# https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


# ******* Access AWS Server *******
AWS_CONN_ID = 'aws_conn'
# config = configparser.ConfigParser()
# config.read_file(
#     open('/Users/oneforall_nick/workspace/Udacity_capstone_project/dl.cfg'))
# os.environ['AWS_ACCESS_KEY_ID'] = config['ACCESS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['ACCESS']['AWS_SECRET_ACCESS_KEY']
# **********************************


# Path to the S3 bucket
# UPLOAD_SAS_JARS_S3_KEY = 'upload_data/jars/spark-sas7bdat-3.0.0-s_2.12.jar'
UPLOAD_CONFIG_FILE_S3_KEY = 'config/dl.cfg'
UPLOAD_BOOTSTRAP_FILE_S3_KEY = 'bootstrap_emr.sh'

# DEST AWS S3 bucket
Data_Bucket = Variable.get('Data_Bucket')
Bootstrap_Bucket = Variable.get('Bootstrap_Bucket')

DEST_BUCKET = 'mydatapool'
DEST_BOOTSTRAP_BUCKET = 'mywsbucketbigdata'

# ******************** Access each data by filepath **********************
filepath = "/Users/oneforall_nick/workspace/Udacity_capstone_project/airflow/data"
# ****** local data absolute path which is uploaded to S3 ******
filepath_all = [os.path.join(root, file) for root,
                dirs, files in os.walk(filepath) for file in files]

# ****** Get the task name, s3_ky and file name from the file_path ******
files = [file_ for root, dirs, files in os.walk(filepath) for file_ in files]

# s3 key where is saved upload of destination of aws s3 location
s3_key_filename = [re.search(r'data/*.*', each_filepath)[0]
                   for each_filepath in filepath_all]

each_file = [re.search(r'(^.+\.)', files[i])[0] + str(i)
             for i in range(len(files))]

files_path = list(zip(each_file, s3_key_filename, filepath_all))
# ************************************************************************

# ******************** Access config file by filepath **********************
CONFIG_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/cfg'
dict_config_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(
    CONFIG_FILE_PATH) for file_ in files if file_.endswith('.cfg'))
# **************************************************************************

# ******************** Access bootstrap file by filepath **********************
BOOTSTRAP_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project'
dict_bootstrap_emr_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(
    BOOTSTRAP_FILE_PATH) for file_ in files if file_.endswith('.sh'))
# *****************************************************************************

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
         description="upload_sas_jars_and_source_data_to_aws_s3",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=False,
         #  Schedule once and only once
         schedule_interval=None,
         tags=['step1, upload, sas_jars, source_data, aws_s3']) as dag:

    start = DummyOperator(task_id='Start_load_jars_and_data_from_local_to_aws_s3')

    # Task Group
    with TaskGroup(group_id='upload_to_aws_s3') as task_group_upload_to_aws_s3:

        logging.info("Start to upload files to aws s3: config file")

        for i in dict_config_info.items():
            upload_config_file = UploadFilesFromLocalToS3(
                task_id=f"upload_{i[1]}_from_local_to_s3",
                s3_bucket=Data_Bucket,
                s3_key=f'config/{i[1]}.cfg',
                filename_dict={i[0]: i[1]},
                aws_conn_id=AWS_CONN_ID,
                replace=True
                )

        logging.info("Completely to upload files to aws s3: config file")

        logging.info("Start to upload files to aws s3: bootstrap_emr file")

        for i in dict_bootstrap_emr_info.items():
            upload_config_file = UploadFilesFromLocalToS3(
                task_id=f"upload_{i[1]}_from_local_to_s3",
                s3_bucket=Bootstrap_Bucket,
                s3_key=f'{i[1]}.sh',
                filename_dict={i[0]: i[1]},
                aws_conn_id=AWS_CONN_ID,
                replace=True
                )

        logging.info("Completely to upload files to aws s3: bootstrap_emr file")

        logging.info("Start to upload files to aws s3: source data")

        for each_filepath in files_path:
            # Show log for each task
            logging.info(f"Uploading: {each_filepath[0]}")  # usCitiesDemo.0 etc..

            upload_data_from_local_to_s3 = UploadFilesFromLocalToS3(
                task_id=f"upload_{each_filepath[0].split('.')[0]}_from_local_to_s3",
                s3_bucket=Data_Bucket,
                s3_key=each_filepath[1],
                filename_dict={each_filepath[2]: each_filepath[0].split('.')[0]},
                aws_conn_id=AWS_CONN_ID,
                replace=True
            )

        logging.info("Completely to upload files to aws s3: source data")

    end = DummyOperator(task_id='Completely_upload_sas_jars_and_source_data_from_local_to_aws_s3')

    # schedule for this dag processes
    start >> task_group_upload_to_aws_s3 >> end