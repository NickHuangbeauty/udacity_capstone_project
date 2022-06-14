from typing import Sequence
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class UploadFilesFromLocalToS3(BaseOperator):
    """
    Purpose:
        1. Upload files from local to aws s3 after access Aws Service.
    :param s3_bucket:              s3 bucket name
    :type s3_bucket                str (templated)
    :param s3_key                  s3 key name (is json file saved in s3 bucket location name)
    :type s3_key                   str
    :param filename_dict           files path and json filename
    format type: {'path/to/your/project/script_name.xxx': 'script_name'}
    :type filename_dict            dictionary
    """

    # template_fields: Sequence[str] = ('s3_bucket')

    # Setting the task background color
    # RPG: 53, 129, 64 -> Green
    ui_color = '#358140'

    def __init__(self,
                 *,
                 s3_bucket: str = '',
                 s3_key: str = '',
                 filename_dict: dict = '',
                 aws_conn_id: str = '',
                 replace: bool = False,
                 **kwargs):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.filename_dict = filename_dict
        self.aws_conn_id = aws_conn_id
        self.replace = replace

    def execute(self, context) -> None:
        """
        Purpose:
            Upload files from local to aws s3 after access Aws Service.

        :return: None
        """

        self.log.info("Starting to upload json files to aws s3")

        self.upload_data_from_local_to_s3(context)

    def upload_data_from_local_to_s3(self, context) -> None:
        """
        Purpose:
            Upload files from local to aws s3 after access Aws Service.
            To push data information to Xcom for using next step.
        """
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        for file_path, filename in self.filename_dict.items():
            context['ti'].xcom_push(key=filename, value=file_path)
            # param filename: path to the file to load.
            hook.load_file(filename=file_path,
                           key=self.s3_key,
                           bucket_name=self.s3_bucket,
                           replace=self.replace)

            self.log.info(f"Uploaded: {filename}")
