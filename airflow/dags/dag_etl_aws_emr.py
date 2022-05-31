import os
import logging
import datetime
import json

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

AWS_CONN_ID = 'aws_credentials'

JOB_FLOW_AWS_S3_KEY = ''
SPARK_STEPS_AWS_S3_KEY = ''
BUCKET_NAME = 'mydatapool'
JOB_FLOW_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/job_flow/'
dict_job_file_info = dict([os.path.join(root, file_), file_.split(".")[0]]for root, dirs, files in os.walk(JOB_FLOW_FILE_PATH) for file_ in files if file_.endswith('.json'))


SPARK_STEP_FILE_PATH = '/Users/oneforall_nick/workspace/Udacity_capstone_project/aws_emr_steps/'
dict_spark_file_info = dict([os.path.join(root, file_), file_.split(".")[0]] for root, dirs, files in os.walk(SPARK_STEP_FILE_PATH) for file_ in files if file_.endswith('.json'))

DEFAULT_ARGS = {
    'owner': 'OneForALL',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 12),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
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

DAG_ID = f"Step2_{os.path.basename(__file__).replace('.py', '')}"

logging.info("Starting DAG_ID: {DAG_ID}")


with DAG(DAG_ID,
         default_args=DEFAULT_ARGS,
        #  maximum number of active DAG runs, beyond this number of DAG runs in a running state, the scheduler won't create new active DAG runs
         max_active_runs=1,
         #  Perform scheduler catchup (or only run latest)? Defaults to True
         catchup=False,
         #  Schedule once and only once
         schedule_interval='@hour',
         #  List of tags to help filtering DAGs in the UI.
         tags=['Step2_aws_emr_to_s3, ETL, Pyspark, EMR']
         ) as dag:

    # ***** Upland Job flow and Spark Step json files from local to AWS S3 *****
    # TODO: Using Python Operations and get Xcom to upload json when completed upload files then get return_value go next steps(EmrCreateJobFlowOperator)
    upload_job_config_json_file = UploadJsonFileFromLocalToS3(
        task_id='upload_job_config_json_file_from_local_to_s3',
        s3_bucket=BUCKET_NAME,
        s3_key=JOB_FLOW_AWS_S3_KEY,
        filename_dict=dict_job_file_info
    )

    upload_spark_step_json_file = UploadJsonFileFromLocalToS3(
        task_id='upload_spark_step_json_file_from_local_to_s3',
        s3_bucket=BUCKET_NAME,
        s3_key=SPARK_STEPS_AWS_S3_KEY,
        filename_dict=dict_spark_file_info
    )
    # Check if json files are uploaded from local to AWS S3 or not!
    how_to_do_next_step = BranchPythonOperator(
        task_id='how_to_do_branch',
        python_callable=how_to_do_branch,
    )

    start = DummyOperator(task_id='Start_to_Add_EMR_Step')

    # TODO: Check create AWS EMR all connection and config for using AWS boto3 API to create EMR instances.

    # Creates an EMR JobFlow, reading the config from the EMR connection.A dictionary of JobFlow overrides can be passed that override the config from the connection.
    create_job_flow = EmrCreateJobFlowOperator(
        task_id='Create_Emr_Cluster',
        job_flow_overrides=get_job_flow(s3_key=JOB_FLOW_AWS_S3_KEY, bucket_name=BUCKET_NAME),
        region_name='us-east-2'
    )

    # An operator that adds steps to an existing EMR job_flow.
    add_steps = EmrAddStepsOperator(
        task_id='Add_EMR_Step',
        aws_conn_id='aws_default',
        job_flow_id='{{ task_instance.xcom_pull(task_ids="Create_Emr_Cluster", key="return_value") }}',
        steps=get_job_flow(s3_key=SPARK_STEPS_AWS_S3_KEY, bucket_name=BUCKET_NAME)
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
    upload_job_config_json_file >> upload_spark_step_json_file >> how_to_do_next_step >> start >> create_job_flow >> add_steps >> wait_for_step >>  end
    upload_job_config_json_file >> upload_spark_step_json_file >> how_to_do_next_step >> no_reachable
