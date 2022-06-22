# Udacity Data Engineer Nanodegree - Capstone Project

![Notification](doc_photo/mika-baumeister-Zk4QPB3-5NY-unsplash.jpeg "News")
<font size="2"> Reference unsplash Mika Baumeister </font>

## Table of contents

- [Scope the Project and Gather Data](#step-1-scope-the-project-and-gather-data)
  - [ETL Tools](#etl-tools)
  - [Description Data](#description)

- [Explore and Assess the Data](#step-2-explore-and-assess-the-data)
  - [Accessing the Data](#accessing-the-data)
  - [Accessing the Data Methods](#accessing-the-data-methods)

- [Define the Data Model](#step-3-define-the-data-model)

- [Run ETL to Model the Data](#step-4-run-etl-to-model-the-data)
  - [Introduction](#introduction)
  - [Triggers](#trigger-dags)
  - [Spark Submit](#spark-submit)
  - [Job Flow](#job-flow)

- [Complete Project Write Up](#step-5-complete-project-write-up)

---


*Purpose for this project*

A health digital company, HealthCare, has decided that it is time to provide more automation and monitoring COVID-19 news information for their users. In order to complete the ETL pipelines that the best tool to achieve this is Apache Airflow.

They have decided to use hadoop ecosystem to process the data on Amazon EMR. The data is stored in s3 bucket.

Their company's data science project team will create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

They have also noted that the data quality plays a big part when analyses are executed on top the data lake and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in open website data of data lake in Amazon EMR. The source datasets consist of news, immigration and cities demographics data that tell about airport, pandemic news in the application after this company's data team to get legal web open data.

*Project Infra*
<p align="center">
  <img src="doc_photo/project_info.png" width="600"  height = "300" alt="ETL Workflow of Result">
</p>


### Step 1: Scope the Project and Gather Data
- This project data will integrate immigration, news and us cities demographics.

#### ETL Tools
**Local Machine**
  - Spark, Hadoop
  - Python pandas
  - Apache Airflow
    - Airflow UI
      - Airflow Variables
      - Airflow DAGs
      - Airflow Aws Operator
      - Airflow Postgres Operator
      - Airflow S3 Operator
      - Airflow EMR Operator
        - Airflow create AWS EMR cluster
        - Airflow Add EMR Steps
        - Airflow watch EMR cluster
        - Airflow terminate EMR cluster
      - Airflow Xcom
      - Jinja2 Template

**Cloud: AWS**
  - EMR
    - ReleaseLabel: emr-5.36.0
    - InstanceType: m5.xlarge
    - InstanceRole: one master, two cores
  - S3

#### Description Datasets
| Source                  | Data Set Description                                                                                                                                                                                                                                                                                                                                                                    |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [*i94*](https://www.trade.gov/national-travel-and-tourism-office)                     | This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.   This  is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project. |
| [*News*]()                    | News about top of biological                                                                                                                                                                                                                                                                                                                                                            |
| [*US Cities: Demographics*](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/) | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.                                                                                                                                                                                                                                    |
---
### Step 2: Explore and Assess the Data
#### Accessing the Data
- i94 immigration data folder
  - included 12 months of 2016
  - sas data format
  - one label sas file for mapping

- News Metadata
  - csv data format

- US Cities Demographics
  - csv data format

#### Accessing the Data Methods
- SAS Data Format
  - Pyspark:
     read data using -> pachage: saurfang:spark-sas7bdat:2.0.0-s_2.11
- CSV Data Format
  - Pyspark:
     read data using -> read.options(header=True, delimiter=';').csv()


Please Refer to: [Capstone Project access data on local machine](aws_emr_steps/Explore_and_Assess_the_Data.ipynb)
<font size="2"> *This .ipynb file is worked on local machine, so it should be modified parameters when use it on AWS EMR and AWS S3.* </font>

---
### Step 3: Define the Data Model
<!-- TODO: Using spark printSchema function to display the dimensions and fact tables-->


---
### Step 4: Run ETL to Model the Data

*Udacity Capstone - ETL Workflow*

<p align="center">
  <img src="doc_photo/dag_main_etl_process_graph_from_airflowUI.jpeg" width="500"  height = "400" alt="ETL Workflow of Result">
</p>


#### Introduction
In this project, I have created two triggers for upload data when I finished download all source data from kaggle or udacity.

I created a dag and named delete xcom for deleting upload data tasks when I rerun airflow each time. This step for confirm my watch step dag certainly get latest step id from AWS EMR cluster.

Combine two dags for automatic and monitored to control more data transfer information. It means I want designing a scenario that dags are more than one, so I can using airflow to monitor multiple tasks more conveniently.

Use EMR Operators for automatic and monitored my AWS EMR cluster status and terminate when all tasks are completed. It's for control AWS EMR cluster runtime cost not over printing.

#### Trigger DAGS

<span style="color:blue">*Trigger 1: Upload Source data from Local to AWS S3*</span>

<p align="center">
  <img src="doc_photo/dag_upload_data_to_s3_from_airflowUI.jpeg" width="500"  height = "400" alt="Upload config and bootstrap files">
</p>


<span style="color:blue">*Trigger 2: Upload etl_emr script from Local to AWS S3*</span>

<p align="center">
  <img src="doc_photo/dag_upload_emr_script_from_airflowUI.jpeg" width="500"  height = "400" alt="Upload EMR Step Script">
</p>

#### Spark Submit
- The main of this step is executing etl_emr script for completing etl data process then reside in AWS S3.
> ➡️ **packages saurfang:spark-sas7bdat:2.0.0-s_2.11** <br>
>  [*HINT*]: This config will automate download jar package.

```python
PARK_STEPS = [
    {
        "Name": "For Dealing with data and analytics using Spark on AWS EMR",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                "--deploy-mode",
                "client",
                "s3://{{ var.value.Data_Bucket }}/upload_data/script/data_spark_on_emr.py",
            ],
        },
    }
]
```

#### Job Flow
- This Job Flow is created AWS EMR Cluster

```python
JOB_FLOW_OVERRIDES = {
    "Name": "Udacity_Capstone_ETL_On_EMR",
    "ReleaseLabel": "emr-5.36.0",
    "Applications": [
        {
            "Name": "Hadoop"
        },
        {
            "Name": "Spark"
        }
    ],
    "BootstrapActions": [
        {
            "Name": "bootstrap_emr",
            "ScriptBootstrapAction": {
                "Path": "s3://{{ var.value.Bootstrap_Bucket }}/bootstrap_emr.sh"
            }
        }
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ],
    "Instances": {
        "Ec2KeyName": "{{ var.value.Ec2_Key_Pair_Name }}",
        "Ec2SubnetId": "{{ var.value.Ec2_Subnet_Id }}",
        "InstanceGroups": [
            {
                "InstanceCount": 1,
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "Market": "ON_DEMAND",
                "Name": "Primary_Node"
            },
            {
                "InstanceCount": 2,
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "Market": "ON_DEMAND",
                "Name": "Core_Node_2"
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "{{ var.value.Job_Flow_Role }}",
    "LogUri": "s3://{{ var.value.Log_Bucket }}/emrlogs/",
    "ServiceRole": "{{ var.value.Service_Role }}",
    "VisibleToAllUsers": True
}
```

---
### Step 5: Complete Project Write Up
