# import os
# import re
# import logging
# import configparser
# from datetime import datetime, timedelta

# from airflow.models import DAG
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.dummy_operator import DummyOperator
# from operators import UploadJsonFileFromLocalToS3

# # https://airflow.apache.org/docs/apache-airflow-providers-amazon/3.3.0/_modules/airflow/providers/amazon/aws/transfers/local_to_s3.html
# from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


# # ******* Access AWS Server *******
# AWS_CONN_ID = 'aws_conn'
# config = configparser.ConfigParser()
# config.read_file(open('/Users/oneforall_nick/.aws/credentials'))
# os.environ['AWS_ACCESS_KEY_ID'] = config['default']['aws_access_key_id']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['aws_secret_access_key']
# # **********************************


# # Path to the S3 bucket
# UPLOAD_ETL_EMR_S3_KEY = 'upload_data/script/data_spark_on_emr.py'
# UPLOAD_SAS_JARS_S3_KEY = 'upload_data/jars/spark-sas7bdat-3.0.0-s_2.12.jar'
# DEST_BUCKET = 'mydatapool'
# UPLOAD_EMR_FILE = 's3://mydatapool/upload_data/script/'


# JOB_FLOW_AWS_S3_KEY = 'job_flow/job_flow_overrides.json'
# SPARK_STEPS_AWS_S3_KEY = 'aws_emr_steps/aws_emr_steps.json'
# BUCKET_NAME = 'mydatapool'
# JOB_FLOW_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/job_flow/'
# dict_job_file_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(
#     JOB_FLOW_FILE_PATH) for file_ in files if file_.endswith('.json'))
# SPARK_STEP_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/'
# dict_spark_file_info = dict([os.path.join(root, file_), file_.split(".")[0]] for root, dirs, files in os.walk(
#     SPARK_STEP_FILE_PATH) for file_ in files if file_.endswith('.json'))


# # ******************** Access each data by filepath **********************
# filepath = "/Users/oneforall_nick/workspace/Udacity_capstone_project/airflow/data"
# # ****** local data absolute path which is uploaded to S3 ******
# filepath_all = [os.path.join(root, file) for root,
#                 dirs, files in os.walk(filepath) for file in files]

# # ****** Get the task name, s3_ky and file name from the file_path ******
# files = [file_ for root, dirs, files in os.walk(filepath) for file_ in files]

# # s3 key where is saved upload of destination of aws s3 location
# s3_key_filename = [re.search(r'/data/*.*', each_filepath)[0]
#                    for each_filepath in filepath_all]

# each_file = [re.search(r'(^.+\.)', files[i])[0] + str(i)
#              for i in range(len(files))]

# files_path = list(zip(each_file, s3_key_filename, filepath_all))
# # ************************************************************************

# # ******************** Access emr file by filepath ***********************
# EMR_FILEPATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/data_spark_on_emr.py'
# # ************************************************************************

# # ******************** Access sas jars file by filepath **********************
# SAS_JARS_FILEPATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/jars/spark-sas7bdat-3.0.0-s_2.12.jar'
# # ****************************************************************************

# # Start: DAG
# # This file name.
# DAG_ID = f"Step1_{os.path.basename(__file__).replace('.py', '')}"

# # Default args for DAG
# DEFAULT_ARGS = {
#     'owner': 'OneForALL',
#     'depends_on_past': False,
#     'start_date': datetime(2019, 1, 12),
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
#     'email_on_retry': False
# }


# logging.info(f"Starting DAG_ID: {DAG_ID}")

# with DAG(DAG_ID,
#          description="upload_to_aws_s3",
#          default_args=DEFAULT_ARGS,
#          max_active_runs=1,
#          catchup=False,
#          #  Schedule once and only once
#          schedule_interval='0 * * * *',
#          tags=['Step1_upload_to_aws_s3']) as dag:

#     start = DummyOperator(task_id='Start_load_data_from_local_to_aws_s3')

#     # # ***** Upland Job flow and Spark Step json files from local to AWS S3 *****
#     # # Using Python Operations and get Xcom to upload json when completed upload files then get return_value go next steps(EmrCreateJobFlowOperator)
#     # upload_job_config_json_file = UploadJsonFileFromLocalToS3(
#     #     task_id='upload_job_config_json_file_from_local_to_s3',
#     #     s3_bucket=BUCKET_NAME,
#     #     s3_key=JOB_FLOW_AWS_S3_KEY,
#     #     filename_dict=dict_job_file_info,
#     #     aws_conn_id=AWS_CONN_ID
#     # )

#     # upload_spark_step_json_file = UploadJsonFileFromLocalToS3(
#     #     task_id='upload_spark_step_json_file_from_local_to_s3',
#     #     s3_bucket=BUCKET_NAME,
#     #     s3_key=SPARK_STEPS_AWS_S3_KEY,
#     #     filename_dict=dict_spark_file_info,
#     #     aws_conn_id=AWS_CONN_ID
#     # )

#     # # Task Group
#     with TaskGroup(group_id='upload_to_aws_s3') as task_group_upload_to_aws_s3:

#         logging.info("Start to upload files to aws s3: data_spark_on_emr")

#         upload_emr_file_from_local_to_s3 = UploadJsonFileFromLocalToS3(
#         task_id='upload_emr_file_from_local_to_s3',
#         s3_bucket=DEST_BUCKET,
#         s3_key=UPLOAD_ETL_EMR_S3_KEY,
#         filename_dict=dict_spark_file_info,
#         aws_conn_id=AWS_CONN_ID
#     )

#         # Upload data_spark_on_emr.py from local to aws s3
#         upload_emr_file_from_local_to_s3 = LocalFilesystemToS3Operator(
#             task_id='upload_emr_file_from_local_to_s3',
#             # local target file path
#             filename=EMR_FILEPATH,
#             dest_key=UPLOAD_ETL_EMR_S3_KEY,
#             dest_bucket=DEST_BUCKET,
#             replace=True,
#             aws_conn_id=AWS_CONN_ID
#         )

#         logging.info("Completely to upload files to aws s3: data_spark_on_emr")

#         logging.info("Start to upload files to aws s3: sas jars")

#         # Upload sas jars file from local to aws s3
#         upload_emr_file_from_local_to_s3 = LocalFilesystemToS3Operator(
#             task_id='upload_sas_jars_file_from_local_to_s3',
#             # local target file path
#             filename=SAS_JARS_FILEPATH,
#             dest_key=UPLOAD_SAS_JARS_S3_KEY,
#             dest_bucket=DEST_BUCKET,
#             replace=True,
#             aws_conn_id=AWS_CONN_ID
#         )

#         logging.info("Completely to upload files to aws s3: sas jars")

#         for each_filepath in files_path:
#             # Show log for each task
#             logging.info(f"Uploading: {each_filepath[0]}")

#             upload_data_from_local_to_s3 = LocalFilesystemToS3Operator(
#                 task_id=f"upload_to_s3_{each_filepath[0]}",
#                 filename=each_filepath[2],
#                 dest_key=each_filepath[1],
#                 dest_bucket=DEST_BUCKET,
#                 replace=True,
#                 aws_conn_id=AWS_CONN_ID
#             )

#     end = DummyOperator(
#         task_id='Completely_load_data_and_emr_file_from_local_to_aws_s3')

#     # schedule for this dag processes
#     start >> task_group_upload_to_aws_s3 >> end
