import configparser
import boto3
from airflow.models import Variable


# ***** Access AWS Cloud configure ************
config = configparser.ConfigParser()
config.read_file(
    open('/Users/oneforall_nick/workspace/Udacity_capstone_project/cfg/dl.cfg'))
# config.read_file(open('dl.cfg'))

aws_access_key = config["ACCESS"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = config["ACCESS"]["AWS_SECRET_ACCESS_KEY"]
aws_token = config["ACCESS"]["AWS_TOKEN"]
DEST_BOOTSTRAP_BUCKET = 'destetlbucket'

session = boto3.Session(
    # service_name='s3',
    # region_name='us-west-2',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_token
)

client = session.client('s3')


Data_Bucket = Variable.get('Data_Bucket')

resp = client.select_object_content(
    Bucket=DEST_BOOTSTRAP_BUCKET,
    Key='dimension_table/df_immigration_personal/imm_person_birth_year=1921/part-00001-4d920f82-bc2b-4f4a-bf17-cba974e4ba7a.c000.snappy.parquet',
    Expression='SELECT * FROM S3Object',
    ExpressionType='SQL',
    InputSerialization={
        'Parquet': {}
    },
    OutputSerialization={
        'JSON': {
            'RecordDelimiter': ','
        }
    }
)

for e in resp['Payload']:
    if 'Records' in e:
        print(e['Records']['Payload'].decode('utf-8'))
    elif 'Stats' in e:
        print(e['Stats'])
