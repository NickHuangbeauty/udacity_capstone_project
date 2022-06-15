# udacity_capstone_project


    {
        "Name": "Move dl from s3 to .aws directory",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Type":"CUSTOM_JAR",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.Data_Bucket }}/config/dl.cfg",
                "--dest=hdfs:///home/hadoop/.aws/",
            ],
        },
    },
ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-18-236-97-114.us-west-2.compute.amazonaws.com

spark-submit --packages "saurfang:spark-sas7bdat:2.0.0-s_2.11" --deploy-mode client s3://mydatapool/upload_data/script/data_spark_on_emr.py


aws s3 cp s3://mydatapool/config/dl.cfg /home/hadoop/.aws/dl.cfg

s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS
s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS


ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-35-166-178-17.us-west-2.compute.amazonaws.com


aws emr add-steps —cluster-id j-3H6EATEWWRWS 
                  —steps Type=spark,
                  Name=ParquetConversion,
                  Args=[ — deploy-mode,
                           cluster, 
                         — master,
                           yarn, 
                         — conf,
                           spark.yarn.submit.waitAppCompletion=true,
                           s3a://test/script/pyspark.py
                        ],
                   ActionOnFailure=CONTINUE


aws emr create-cluster \
        --os-release-label 2.0.20220426.0 \
        --applications Name=Spark Name=Zeppelin \
        --ec2-attributes '{"KeyName":"nick_key_pair",\
                           "InstanceProfile":"EMR_EC2_DefaultRole",\
                           "SubnetId":"subnet-bfd668f5",\
                           "EmrManagedSlaveSecurityGroup":"sg-0335d2150620c6ad7",\
                           "EmrManagedMasterSecurityGroup":"sg-01fcdf749f9568b06"}'\
        --release-label emr-5.36.0 \
        --log-uri 's3n://aws-logs-324206118356-us-west-2/elasticmapreduce/' \
        --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"}]' \
        --configurations '[{"Classification":"spark","Properties":{}}]' \
        --service-role EMR_DefaultRole \
        --enable-debugging \
        --auto-termination-policy '{"IdleTimeout":3600}' \
        --name 'My cluster' \
        --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
        --region us-west-2