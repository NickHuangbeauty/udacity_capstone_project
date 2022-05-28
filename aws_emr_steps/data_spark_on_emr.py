import os
import configparser
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, udf, to_date
from pyspark.sql.types import (StructType,
                               StructField,
                               StringType,
                               IntegerType,
                               DoubleType,
                               DateType,
                               FloatType)

# Access AWS Cloud configure
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"] = config["ACCESS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["ACCESS"]["AWS_SECRET_ACCESS_KEY"]

def create_spark_session():
    """
    Purpose:
        Build an access spark session for dealing data ETL of Data Lake
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .appName("spark_emr_udactity") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_dim_immigration(spark, source_bucket, dest_bucket) -> None:
    pass


def process_dim_news(spark, source_bucket, dest_bucket) -> None:
    pass


def process_dim_us_cities_demographics(spark, source_bucksource_bucket, dest_bucket) -> None:
    pass


def rocess_dim_label(spark, source_bucket, dest_bucket) -> None:
    pass


def process_dim_label(spark, source_bucket, dest_bucket) -> None:
    pass


def process_fact_notifications(spark, source_bucket, dest_bucket) -> None:
    pass

def main():

    # Start to access spark by spark session
    spark = create_spark_session()

    # Access data from AWS S3
    source_bucket = ""
    # Write data to AWS S3
    dest_bucket = ""

    # Process data for creating dimension table: immigration
    process_dim_immigration(spark, source_bucket, dest_bucket)

    # Process data for creating dimension table: news
    process_dim_news(spark, source_bucket, dest_bucket)

    # Process data for creating dimension table: us cities Demographics
    process_dim_us_cities_demographics(spark, source_bucksource_bucket, dest_bucket)

    # Process data for creating dimension table: label for immigration
    process_dim_label(spark, source_bucket, dest_bucket)

    # Process data for creating fact table: notifications
    process_fact_notifications(spark, source_bucket, dest_bucket)



if __name__ == "__main__":
    main()