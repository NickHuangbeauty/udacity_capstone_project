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
ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-54-71-196-102.us-west-2.compute.amazonaws.com

spark-submit --deploy-mode client --executor-memory 10G --packages "saurfang:spark-sas7bdat:2.0.0-s_2.11" s3://mydatapool/upload_data/script/data_spark_on_emr.py


aws s3 cp s3://mydatapool/config/dl.cfg /home/hadoop/.aws/dl.cfg

s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS
s3://mydatapool/data/immigration_data/immigration_labels_descriptions.SAS


ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-35-166-178-17.us-west-2.compute.amazonaws.com
ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-35-85-63-59.us-west-2.compute.amazonaws.com


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

Spark information: <pyspark.sql.session.SparkSession object at 0x7f87815353d0>
[('spark.eventLog.enabled', 'true'),
 ('spark.driver.host', 'ip-172-31-19-215.us-west-2.compute.internal'),
 ('spark.executor.memory', '9486M'),
 ('spark.yarn.executor.memoryOverheadFactor', '0.1875'),
 ('spark.driver.extraLibraryPath', '/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native'),
 ('spark.sql.parquet.output.committer.class', 'com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter'),
 ('spark.blacklist.decommissioning.timeout', '1h'),
 ('spark.yarn.appMasterEnv.SPARK_PUBLIC_DNS',
 '$(hostname -f)'),
 ('spark.sql.emr.internal.extensions', 'com.amazonaws.emr.spark.EmrSparkSessionExtensions'),
 ('spark.eventLog.dir', 'hdfs:///var/log/spark/apps'),
 ('spark.sql.warehouse.dir', 'hdfs:///user/spark/warehouse'),
 ('spark.history.fs.logDirectory', 'hdfs:///var/log/spark/apps'),
 ('spark.ui.filters', 'org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter'),
 ('spark.driver.appUIAddress', 'http://ip-172-31-19-215.us-west-2.compute.internal:4040'),
 ('spark.hadoop.yarn.timeline-service.enabled', 'false'),
 ('spark.ui.proxyBase', '/proxy/application_1655371680908_0001'),
 ('spark.executor.id', 'driver'),
 ('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11'),
 ('spark.driver.port', '40781'),
 ('spark.driver.memory', '2048M'),
 ('spark.yarn.dist.files', 'file:/etc/hudi/conf/hudi-defaults.conf'),
 ('spark.hadoop.mapreduce.output.fs.optimized.committer.enabled', 'true'),
 ('spark.decommissioning.timeout.threshold', '20'),
 ('spark.stage.attempt.ignoreOnDecommissionFetchFailure', 'true'),
 ('spark.executor.extraLibraryPath', '/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native'),
 ('spark.hadoop.fs.s3.getObject.initialSocketTimeoutMilliseconds', '2000'),
 ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS', 'ip-172-31-19-215.us-west-2.compute.internal'),
 ('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem', '2'),
 ('spark.yarn.dist.jars', 'file:///home/hadoop/.ivy2/jars/saurfang_spark-sas7bdat-2.0.0-s_2.11.jar,file:///home/hadoop/.ivy2/jars/com.epam_parso-2.0.8.jar,file:///home/hadoop/.ivy2/jars/org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar,file:///home/hadoop/.ivy2/jars/org.slf4j_slf4j-api-1.7.5.jar,file:///home/hadoop/.ivy2/jars/org.scala-lang_scala-reflect-2.11.8.jar'),
 ('spark.executor.extraClassPath', '/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar'),
 ('spark.fileMetadataCache.enabled', 'false'),
 ('spark.executor.cores', '4'), ('spark.driver.extraClassPath', '/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar'), ('spark.sql.hive.metastore.sharedPrefixes', 'com.amazonaws.services.dynamodbv2'), ('spark.serializer.objectStreamReset', '100'), ('spark.submit.deployMode', 'client'), ('spark.sql.parquet.fs.optimized.committer.optimization-enabled', 'true'), ('spark.repl.local.jars', 'file:///home/hadoop/.ivy2/jars/saurfang_spark-sas7bdat-2.0.0-s_2.11.jar,file:///home/hadoop/.ivy2/jars/com.epam_parso-2.0.8.jar,file:///home/hadoop/.ivy2/jars/org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar,file:///home/hadoop/.ivy2/jars/org.slf4j_slf4j-api-1.7.5.jar,file:///home/hadoop/.ivy2/jars/org.scala-lang_scala-reflect-2.11.8.jar'), ('spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem', 'true'), ('spark.submit.pyFiles', '/home/hadoop/.ivy2/jars/saurfang_spark-sas7bdat-2.0.0-s_2.11.jar,/home/hadoop/.ivy2/jars/com.epam_parso-2.0.8.jar,/home/hadoop/.ivy2/jars/org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar,/home/hadoop/.ivy2/jars/org.slf4j_slf4j-api-1.7.5.jar,/home/hadoop/.ivy2/jars/org.scala-lang_scala-reflect-2.11.8.jar'), ('spark.history.ui.port', '18080'), ('spark.shuffle.service.enabled', 'true'), ('spark.app.name', 'spark_emr_udactity'), ('spark.driver.defaultJavaOptions', "-XX:OnOutOfMemoryError='kill -9 %p'"), ('spark.resourceManager.cleanupExpiredHost', 'true'), ('spark.executor.defaultJavaOptions', "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'"), ('spark.yarn.historyServer.address', 'ip-172-31-19-215.us-west-2.compute.internal:18080'), ('spark.yarn.dist.pyFiles', 'file:///home/hadoop/.ivy2/jars/saurfang_spark-sas7bdat-2.0.0-s_2.11.jar,file:///home/hadoop/.ivy2/jars/com.epam_parso-2.0.8.jar,file:///home/hadoop/.ivy2/jars/org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar,file:///home/hadoop/.ivy2/jars/org.slf4j_slf4j-api-1.7.5.jar,file:///home/hadoop/.ivy2/jars/org.scala-lang_scala-reflect-2.11.8.jar'), ('spark.executorEnv.PYTHONPATH', '{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.7-src.zip<CPS>{{PWD}}/saurfang_spark-sas7bdat-2.0.0-s_2.11.jar<CPS>{{PWD}}/com.epam_parso-2.0.8.jar<CPS>{{PWD}}/org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar<CPS>{{PWD}}/org.slf4j_slf4j-api-1.7.5.jar<CPS>{{PWD}}/org.scala-lang_scala-reflect-2.11.8.jar'), ('spark.files.fetchFailure.unRegisterOutputOnHost', 'true'), ('spark.yarn.secondary.jars', 'saurfang_spark-sas7bdat-2.0.0-s_2.11.jar,com.epam_parso-2.0.8.jar,org.apache.logging.log4j_log4j-api-scala_2.11-2.7.jar,org.slf4j_slf4j-api-1.7.5.jar,org.scala-lang_scala-reflect-2.11.8.jar'), ('spark.app.id', 'application_1655371680908_0001'), ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES', 'http://ip-172-31-19-215.us-west-2.compute.internal:20888/proxy/application_1655371680908_0001'),
 ('spark.master', 'yarn'),
 ('spark.emr.default.executor.memory', '9486M'),
 ('spark.rdd.compress', 'True'),
 ('spark.yarn.isPython', 'true'), ('spark.dynamicAllocation.enabled', 'true'),
 ('spark.emr.default.executor.cores', '4'),
 ('spark.blacklist.decommissioning.enabled', 'true')]


ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-35-84-39-18.us-west-2.compute.amazonaws.com

 spark-submit --executor-memory 1g --class org.apache.spark.examples.SparkPi /usr/lib/spark/examples/jars/spark-examples.jar 10



 ssh -i ~/.ssh/nick_key_pair.pem hadoop@ec2-18-237-81-69.us-west-2.compute.amazonaws.com