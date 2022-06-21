# Udacity Data Engineer Nanodegree - Capstone Project

![Notification](doc_photo/mika-baumeister-Zk4QPB3-5NY-unsplash.jpg, "news")
reference by unsplash Mika Baumeister

## Table of contents
---

- [Scope the Project and Gather Data](#step-1-scope-the-project-and-gather-data)

- [Explore and Assess the Data](#step-2-explore-and-assess-the-data)

- [Define the Data Model](#step-3-define-the-data-model)

- [Run ETL to Model the Data](#step-4-run-etl-to-model-the-data)

- [Complete Project Write Up](#step-5-complete-project-write-up)

---


*Purpose for this project*

A health digital company, HealthCare, has decided that it is time to provide more automation and monitoring COVID-19 news information for their users. In order to complete the ETL pipelines that the best tool to achieve this is Apache Airflow.

They have decided to use hadoop ecosystem to process the data on Amazon EMR. The data is stored in s3 bucket.

Their company's data science project team will create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

They have also noted that the data quality plays a big part when analyses are executed on top the data lake and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in open website data of data lake in Amazon EMR. The source datasets consist of news, immigration and cities demographics data that tell about airport, pandemic news in the application after this company's data team to get legal web open data.
### Step 1: Scope the Project and Gather Data
- This project data will integrate immigration, news and us cities demographics.

**Dataset**
1. [*i94*](https://www.trade.gov/national-travel-and-tourism-office)

2. [*US Cities: Demographics*](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

3. [*news data*]()

**ETL Tools**
1. Local Machine
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
2. Cloud: AWS
  - EMR
    - ReleaseLabel: 
    - InstanceType: m5.xlarge
    - InstanceRole: one master, two cores
  - S3

**Description Data**
| Source                  | Data Set Description                                                                                                                                                                                                                                                                                                                                                                    |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| i94                     | This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.   This  is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project. |
| News                    | News about top of biological                                                                                                                                                                                                                                                                                                                                                            |
| US Cities: Demographics | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.                                                                                                                                                                                                                                    |

### Step 2: Explore and Assess the Data
**Accessing the Data**
- i94 immigration data folder
  - included 12 months of 2016
  - sas data format
  - one label sas file for mapping

- News Metadata
  - csv data format

- US Cities Demographics
  - csv data format

**Accessing the Data Methods**
- SAS Data Format
  - Pyspark:
     read data using -> pachage: saurfang:spark-sas7bdat:2.0.0-s_2.11
- CSV Data Format
  - Pyspark:
     read data using -> read.options(header=True, delimiter=';').csv()


### Step 3: Define the Data Model

### Step 4: Run ETL to Model the Data

### Step 5: Complete Project Write Up
