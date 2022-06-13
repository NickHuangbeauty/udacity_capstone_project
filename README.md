# udacity_capstone_project


{
        "Name": "Move dl from s3 to .aws directory",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.Data_Bucket }}/config/dl.cfg",
                "--dest=/home/hadoop/.aws/",
            ],
        },
    },


spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.11 --deploy-mode client s3://mydatapool/upload_data/script/data_spark_on_emr.py


aws s3 cp s3://mydatapool/config/dl.cfg /home/hadoop/.aws/dl.cfg

s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS
s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS