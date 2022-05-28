import os
import configparser
import logging
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

# Access data from AWS S3
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
# Write data to AWS S3
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']


def create_spark_session():
    # sourcery skip: inline-immediately-returned-variable
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

    logging.info("Spark information: {spark}")

    logging.info(spark.sparkContext.getConf().getAll())

    return spark


def process_dim_immigration(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET) -> None:
    imm_data = os.path.join(
        SOURCE_S3_BUCKET, "data/immigration_data/immigration_apr16_sub.sas7bdat")
    df_imm_data = spark.read.format(
        "com.github.saurfang.sas.spark").load(imm_data)

    # Dimension Table: Immigration personal data
    df_immigration_personal = df_imm_data.withColumn("imm_per_cic_id", col("cicid").cast("String"))\
        .withColumn("imm_person_birth_year", col("biryear").cast("Integer"))\
        .withColumn("imm_person_gender", col("gender").cast("String"))\
        .withColumn("imm_visatype", col("visatype").cast("String")).select(col("imm_per_cic_id"),
                                                                           col(
            "imm_person_birth_year"),
        col("imm_person_gender"),
        col("imm_visatype"))

    df_immigration_personal_tmp = df_immigration_personal.createOrReplaceTempView("imm_personal")

    df_immigration_personal_tmp = spark.sql("SELECT * FROM imm_personal")

    df_immigration_personal_tmp.persist()

    df_immigration_personal_tmp.explain()

    df_immigration_personal_tmp.write.mode("override").partitionBy("imm_person_birth_year").parquet(
        path=f'{DEST_S3_BUCKET}dimension_table/df_immigration_personal')

    # Dimension Table: Immigration main data

    def convert_to_datetime(days: DoubleType) -> datetime:
        """convert_to_datetime converts days to datetime format

        Args:
            days (DoubleType): from sas arrive or departure date

        Returns:
            datetime: added days to datetime format result.
        """
        if days is not None:
            date = datetime.strptime('1960-01-01', '%Y-%m-%d')

            return date + timedelta(days=days)


        udf_convert_to_datetime = udf(lambda x: convert_to_datetime(x), DateType())

        immigration_main_information = df_imm_data.withColumn("imm_main_cic_id", col("cicid").cast("Integer"))\
            .withColumn("imm_year", col("i94yr").cast("Integer"))\
            .withColumn("imm_month", col("i94mon").cast("Integer"))\
            .withColumn("imm_cntyl", col("i94cit").cast("Integer"))\
            .withColumn("imm_visa", col("i94visa").cast("Integer"))\
            .withColumn("imm_port", col("i94port").cast("String"))\
            .withColumn("imm_arrival_date", udf_convert_to_datetime(col("arrdate")))\
            .withColumn("imm_departure_date", udf_convert_to_datetime(col("depdate")))\
            .withColumn("imm_model", col("i94mode").cast("Integer"))\
            .withColumn("imm_address", col("i94addr").cast("String"))\
            .withColumn("imm_airline", col("airline").cast("String"))\
            .withColumn("imm_flight_no", col("fltno").cast("String"))\
            .select(col('imm_main_cic_id'),
                    col('imm_year'),
                    col('imm_month'),
                    col('imm_cntyl'),
                    col('imm_visa'),
                    col('imm_port'),
                    col('imm_arrival_date'),
                    col('imm_departure_date'),
                    col('imm_model'),
                    col('imm_address'),
                    col('imm_airline'),
                    col('imm_flight_no'))

        df_immigration_main_information_tmp = immigration_main_information.createOrReplaceTempView("immigration_main_information_data")

        df_immigration_main_information_tmp = spark.sql(
            "SELECT * FROM immigration_main_information_data")

        df_immigration_main_information_tmp.persist()

        df_immigration_main_information_tmp.explain()

        df_immigration_main_information_tmp.write.mode("override").partitionBy("imm_year", "imm_month").parquet(
            path=f'{DEST_S3_BUCKET}dimension_table/immigration_main_information')

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

    # Process data for creating dimension table: immigration
    process_dim_immigration(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET)

    # Process data for creating dimension table: news
    process_dim_news(spark, source_bucket, dest_bucket)

    # Process data for creating dimension table: us cities Demographics
    process_dim_us_cities_demographics(
        spark, source_bucksource_bucket, dest_bucket)

    # Process data for creating dimension table: label for immigration
    process_dim_label(spark, source_bucket, dest_bucket)

    # Process data for creating fact table: notifications
    process_fact_notifications(spark, source_bucket, dest_bucket)


if __name__ == "__main__":
    main()
