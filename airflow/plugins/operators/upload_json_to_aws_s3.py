from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class UploadJsonFileFromLocalToS3(BaseOperator):
    """
    Purpose:
        1. Upload json files from local to aws s3 after access Aws Service.
    :param s3_bucket:              s3 bucket name
    :type s3_bucket                str
    :param s3_key                  s3 key name (is json file saved in s3 bucket location name)
    :type s3_key                   str
    :param filename_dict           json file path and json filename
    :type filename_dict            dictionary
    """

    # Setting the task background color
    # RPG: 53, 129, 64 -> Green
    ui_color = '#358140'

    def __init__(self,
                 *,
                 s3_bucket: str = '',
                 s3_key: str = '',
                 filename_dict: dict = '',
                 aws_conn_id: str = '',
                 **kwargs):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.filename_dict = filename_dict
        self.aws_conn_id = aws_conn_id

    def execute(self, **context) -> None:
        """
        Purpose:
            Upload json files from local to aws s3 after access Aws Service.

        :return: None
        """

        self.log.info("Starting to upload json files to aws s3")

        self.upload_data_from_local_to_s3(**context)

    def upload_data_from_local_to_s3(self, **context) -> None:
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        ti = context['task_instance']

        for file_path, filename in self.filename_dict.items():
            ti.xcom_push(key=filename, value=file_path)

            hook.load_file(filename=file_path,
                           key=self.s3_key,
                           bucket_name=self.s3_bucket,
                           replace=True)

            self.log.info(f"Uploaded: {filename}")
