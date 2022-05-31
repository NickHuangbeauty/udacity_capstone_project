# https://stackoverflow.com/questions/38649099/import-all-future-features
import __future__

from airflow.plugins_manager import AirflowPlugin

import operators

class OneForALLPlugin(AirflowPlugin):
    name = 'OneForALL_Plugin'
    operators = [
        operators.UploadJsonFileFromLocalToS3,
    ]
