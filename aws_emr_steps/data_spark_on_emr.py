import os
import configparser
import logging
import boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from signal import signal, SIGPIPE, SIG_DFL
from pyspark.sql.functions import col, monotonically_increasing_id, udf, to_date, round
from pyspark.sql.types import (StructType,
                               StructField,
                               StringType,
                               IntegerType,
                               DoubleType,
                               DateType,
                               FloatType)

# ***** Without Catching SIGPIPE *********
signal(SIGPIPE, SIG_DFL)
# ****************************************


# ***** Access AWS Cloud configure ************
config = configparser.ConfigParser()
config.read_file(open('/home/hadoop/.aws/dl.cfg'))
# config.read_file(open('dl.cfg'))

aws_access_key = config["ACCESS"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = config["ACCESS"]["AWS_SECRET_ACCESS_KEY"]
aws_token = config["ACCESS"]["AWS_TOKEN"]
# Access data from AWS S3
# SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
SOURCE_S3_BUCKET = 's3://mydatapool'
# Write data to AWS S3
# DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']
DEST_S3_BUCKET = 's3://destetlbucket'
# *********************************************


# ***** Local Testing configure *****************

session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_token
)

s3_access = session.resource('s3')
# .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\

def create_spark_session():
    # source skip: inline-immediately-returned-variable
    """
    Purpose:
        Build an access spark session for dealing data ETL of Data Lake

    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .appName("spark_emr_udactity") \
        .getOrCreate()

    # spark.conf.set("spark.sql.shuffle.partitions", "300")

    print(f"Spark information: {spark}")

    # print(f"{spark.sparkContext.getConf().getAll()}")

    return spark


def process_dim_immigration(spark_session, source_s3_bucket, dest_s3_bucket):
    """
    Purpose:
        Load immigration data from AWS S3 bucket and go though the ETL process by AWS ERM and then restored AWS S3

    Args:
        spark_session: spark session
        SOURCE_S3_BUCKET str: AWS S3 Bucket where is stored source data.
        DEST_S3_BUCKET str: AWS S3 Bucket where is stored processed data

    Returns:
        DataFrame: df_immigration_main_information and df_immigration_personal
    """
    imm_path = os.path.join(
        source_s3_bucket, "data/immigration_data/immigration_apr16_sub.sas7bdat")

    df_imm_data = spark_session.read.format(
        "com.github.saurfang.sas.spark").load(imm_path)

    # Dimension Table: Immigration personal data
    df_immigration_personal = df_imm_data.withColumn("imm_per_cic_id", col("cicid").cast("String")) \
        .withColumn("imm_person_birth_year", col("biryear").cast("Integer")) \
        .withColumn("imm_person_gender", col("gender").cast("String")) \
        .withColumn("imm_visatype", col("visatype").cast("String")) \
        .select(col("imm_per_cic_id"),
                col("imm_person_birth_year"),
                col("imm_person_gender"),
                col("imm_visatype"))

    # df_immigration_personal_tmp = df_immigration_personal.createOrReplaceTempView(
    #     "imm_personal")

    # df_immigration_personal_tmp = spark.sql("SELECT * FROM imm_personal")

    # df_immigration_personal_tmp.persist()

    # df_immigration_personal_tmp.explain()

    numPartitions = 300
    df_immigration_personal.repartition(numPartitions) \
                               .write.partitionBy("imm_person_birth_year") \
                               .parquet(mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/df_immigration_personal')

    # Dimension Table: Immigration main data
    def convert_to_datetime(days: DoubleType) -> datetime:
        """
        Purpose:
            convert_to_datetime converts days to datetime format

        Args:
            days (DoubleType): from sas arrive or departure date

        Returns:
            datetime: added days to datetime format result.
        """
        if days is not None:
            date = datetime.strptime('1960-01-01', '%Y-%m-%d')

            return date + timedelta(days=days)

    udf_convert_to_datetime = udf(lambda x: convert_to_datetime(x), DateType())

    df_immigration_main_information = df_imm_data.withColumn("imm_main_cic_id", col("cicid").cast("Integer"))\
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

    # df_immigration_main_information_tmp = immigration_main_information.createOrReplaceTempView(
    #     "immigration_main_information_data")

    # df_immigration_main_information_tmp = spark.sql(
    #     "SELECT * FROM immigration_main_information_data")

    # df_immigration_main_information_tmp.persist()

    # df_immigration_main_information_tmp.explain()
    numPartitions = 300
    df_immigration_main_information.repartition(numPartitions) \
        .write.partitionBy("imm_year", "imm_month"). \
        parquet(mode="overwrite",
                path=f'{DEST_S3_BUCKET}/dimension_table/immigration_main_information')
    return df_immigration_main_information, df_immigration_personal


def process_dim_news(spark_session, source_s3_bucket, dest_s3_bucket):
    """
    Purpose
        Load news data from AWS S3 bucket and go though the ETL process by AWS ERM and then restored AWS S3

    Args:
        spark_session: spark session
        SOURCE_S3_BUCKET str: AWS S3 Bucket where is stored source data.
        DEST_S3_BUCKET str: AWS S3 Bucket where is stored processed data.

    Returns:
        DataFrame: df_news
    """
    news_path = os.path.join(source_s3_bucket, "data/news_data/metadata.csv")

    df_news = spark_session.read.options(header=True, delimiter=',').csv(path=news_path)

    df_news = df_news.withColumn("news_cord_uid", col("cord_uid").cast("String")) \
        .withColumn("news_source", col("source_x").cast("String")) \
        .withColumn("news_title", col("title").cast("String")) \
        .withColumn("news_licence", col("license").cast("String")) \
        .withColumn("news_abstract", col("abstract").cast("String")) \
        .withColumn("news_publish_time", to_date(col("publish_time"), "yyyy-MM-dd")) \
        .withColumn("news_authors", col("authors").cast("String")) \
        .withColumn("news_url", col("url").cast("String")) \
        .select(col("news_cord_uid"),
                col("news_source"),
                col("news_title"),
                col("news_licence"),
                col("news_abstract"),
                col("news_publish_time"),
                col("news_authors"),
                col("news_url"))

    # df_news_tmp = df_news.createOrReplaceTempView("news_article_data")

    # df_news_tmp = spark.sql(
    #     "SELECT * FROM news_article_data WHERE news_publish_time BETWEEN '2016-01-01' AND '2016-12-31'")

    # df_news_tmp.persist()

    # df_news_tmp.explain()
    numPartitions = 300
    df_news.repartition(numPartitions) \
               .write.partitionBy("news_publish_time") \
               .parquet(mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/news_article_data')

    return df_news


def process_dim_us_cities_demographics(spark_session, source_s3_bucket, dest_s3_bucket):
    """
    Purpose
        Load us-cities-demographics data from AWS S3 bucket and go though the ETL process by AWS ERM and then restored AWS S3

    Args:
        spark_session: spark session
        SOURCE_S3_BUCKET str: AWS S3 Bucket where is stored source data.
        DEST_S3_BUCKET str: AWS S3 Bucket where is stored processed data.

    Returns:
        DataFrame: df_us_cities_demographics
    """
    us_cities_demographics_path = os.path.join(source_s3_bucket, "data/usCitiesDemographics_data/usCitiesDemo.csv")

    df_us_cities_demographics = spark_session.read.options(header=True, delimiter=';').csv(us_cities_demographics_path)

    df_us_cities_demographics = df_us_cities_demographics.withColumn("cidemo_city", col("City").cast("String")) \
        .withColumn("cidemo_state", col("State").cast("String")) \
        .withColumn("cidemo_median_age", col("Median Age").cast("Float")) \
        .withColumn("cidemo_male_population", col("Male Population").cast("Integer")) \
        .withColumn("cidemo_female_population", col("Female Population").cast("Integer")) \
        .withColumn("cidemo_total_population", col("Total Population").cast("Integer")) \
        .withColumn("cidemo_number_of_veterans", col("Number of Veterans").cast("Integer")) \
        .withColumn("cidemo_foreign_born", col("Foreign-born").cast("Integer")) \
        .withColumn("cidemo_average_household_size", col("Average Household Size").cast("Float")) \
        .withColumn("cidemo_state_code", col("State Code").cast("String")) \
        .withColumn("cidemo_race", col("Race").cast("String")) \
        .withColumn("cidemo_count", col("Count").cast("Integer")) \
        .select(col("cidemo_city"),
                col("cidemo_state"),
                col("cidemo_median_age"),
                col("cidemo_total_population"),
                col("cidemo_state_code"),
                col("cidemo_count"))

    # Auto-generated series of id
    df_us_cities_demographics = df_us_cities_demographics.withColumn(
        "cidemo_id", monotonically_increasing_id())

    # df_us_cities_demographics_temp = df_us_cities_demographics.createOrReplaceTempView(
    #     "us_cities_demographics_data")

    # df_us_cities_demographics_temp = spark.sql(
    #     "SELECT * FROM us_cities_demographics_data")

    # df_us_cities_demographics_temp.persist()

    # df_us_cities_demographics_temp.explain()
    numPartitions = 300
    df_us_cities_demographics.repartition(numPartitions) \
                                  .write \
                                  .parquet(mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/us_cities_demographics_data')
                                
    return df_us_cities_demographics


def process_dim_label(spark_session, s3_access, dest_s3_bucket):
    """
    Purpose
        Load immigration-label data from AWS S3 bucket and go though the ETL process by AWS ERM and then restored AWS S3

    Args:
        spark_session: spark session
        SOURCE_S3_BUCKET str: AWS S3 Bucket where is stored source data.
        DEST_S3_BUCKET str: AWS S3 Bucket where is stored processed data.

    Returns:
        DataFrame: df_imm_destination_city
    """

    s3_object = s3_access.Bucket('mydatapool').Object(
        'data/immigration_data/immigration_labels_descriptions.SAS').get()
    text = s3_object['Body'].read()
    context = text.decode(encoding='utf-8')

    context = context.replace('\t', '')

    def code_mapping(context, idx):
        content_mapping = context[context.index(idx):]
        content_line_split = content_mapping[:content_mapping.index(
            ';')].split('\n')
        content_line_list = [line.replace("'", "")
                             for line in content_line_split]
        content_two_dims = [i.strip().split('=')
                            for i in content_line_list[1:]]
        content_three_dims = [[i[0].strip(), i[1].strip().split(', ')[:][0], e]
                              for i in content_two_dims if len(i) == 2 for e in i[1].strip().split(', ')[1:]]
        return content_two_dims, content_three_dims

    # ***** imm_cit_res *****
    imm_cit_res_two, imm_cit_res_three = code_mapping(context, "i94cntyl")

    df_imm_city_res_label = spark_session.sparkContext.parallelize(imm_cit_res_three).toDF(["col_of_imm_cntyl", "value_of_imm_cntyl", "value_of_imm_cntyl_organizations"]) \
        .withColumn("col_of_imm_cntyl", col("col_of_imm_cntyl").cast("Integer")) \
        .withColumn("value_of_imm_cntyl", col("value_of_imm_cntyl").cast("String")) \
        .withColumn("value_of_imm_cntyl", col("value_of_imm_cntyl_organizations").cast("String")) \

    # Saved in AWS S3
    df_imm_city_res_label.write.parquet(
        mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/imm_city_res_label')
    # **********************

    # ***** imm_port *****
    imm_port_two, imm_port_three = code_mapping(context, "i94prtl")
    df_imm_destination_city = spark_session.sparkContext.parallelize(imm_port_three).toDF(["code_of_imm_destination_city", "value_of_imm_destination_city", "value_of_alias_imm_destination_city"]) \
                                                .withColumn("code_of_imm_destination_city", col("code_of_imm_destination_city").cast("String")) \
                                                .withColumn("value_of_imm_destination_city", col("value_of_imm_destination_city").cast("String")) \
                                                .withColumn("value_of_alias_imm_destination_city", col("value_of_alias_imm_destination_city").cast("String"))
    # For querying and joining other tables.
    # df_imm_destination_city_tmp = df_imm_destination_city.createOrReplaceTempView(
    #     "imm_destination_city_data")
    # df_imm_destination_city_tmp = spark.sql(
    #     "SELECT * FROM imm_destination_city_data")
    # df_imm_destination_city_tmp.persist()
    # Saved in AWS S3
    df_imm_destination_city.write.parquet(mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/imm_destination_city')
    # ********************

    # ***** imm_mod *****
    imm_mode_two, imm_mode_three = code_mapping(context, "i94model")
    df_imm_travel_code = spark_session.sparkContext.parallelize(imm_mode_two).toDF(["code_of_imm_travel_code", "value_of_imm_travel_code"]) \
                                           .withColumn("code_of_imm_travel_code", col("code_of_imm_travel_code").cast("Integer")) \
                                           .withColumn("value_of_imm_travel_code", col("value_of_imm_travel_code").cast("String"))
    # Saved in AWS S3
    df_imm_travel_code.write.parquet(
        mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/imm_travel_code')
    # *******************

    # ***** imm_addr *****
    imm_addr_two, imm_addr_three = code_mapping(context, "i94addrl")
    df_imm_address = spark_session.sparkContext.parallelize(imm_addr_two).toDF(["code_of_imm_address", "value_of_imm_address"]) \
        .withColumn("code_of_imm_address", col("code_of_imm_address").cast("String")) \
        .withColumn("value_of_imm_address", col("value_of_imm_address").cast("String"))
    # Saved in AWS S3
    df_imm_address.write.parquet(
        mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/imm_address')
    # ********************

    # ***** imm_visa *****
    imm_visa = {'1': 'Business',
                '2': 'Pleasure',
                '3': 'Student'}

    df_imm_visa = spark_session.sparkContext.parallelize(imm_visa.items()).toDF(["code_of_imm_visa", "value_of_imm_visa"]) \
                                    .withColumn("code_of_imm_visa", col("code_of_imm_visa").cast("Integer")) \
                                    .withColumn("value_of_imm_visa", col("value_of_imm_visa").cast("String"))
    # Saved in AWS S3
    df_imm_visa.write.parquet(mode="overwrite", path=f'{dest_s3_bucket}/dimension_table/imm_visa')
    # ********************

    return df_imm_destination_city


def process_fact_notifications(dest_s3_bucket,
                               imm_information,
                               imm_personal,
                               news_article_data,
                               us_cities_demographics_data,
                               imm_destination_city_data) -> None:
    """
    Purpose
        Join dimension tables for ETL processed by AWS ERM and then restored fact table to AWS S3.
        ** t1: join imm two tables (imm_personal and imm_information)
        ** t2: join news table with t1 (news_article_data)
        ** us_cities_dest: join us_cities_demographics_data and imm_destination_city_data
        ** df_notification: join us cities table with t2

    Args:
        spark_session: spark session
        SOURCE_S3_BUCKET str: AWS S3 Bucket where is stored source data.
        dest_s3_bucket str: AWS S3 Bucket where is stored processed data.
        imm_information DataFrame: After ETL Processed dimension data. (immigration data)
        imm_personal DataFrame: After ETL Processed dimension data. (immigration data)
        news_article_data DataFrame: After ETL Processed dimension data. (news articles data)
        us_cities_demographics_data DataFrame: After ETL Processed dimension data. (us cities demographics data)
        imm_destination_city_data DataFrame: After ETL Processed dimension data. (immigration data)

    """

    t1 = imm_information.join(imm_personal, imm_information.imm_main_cic_id ==
                              imm_personal.imm_per_cic_id, "inner").filter(round(imm_information.imm_month, 0) == 4)

    t2 = t1.join(news_article_data, t1.imm_arrival_date == news_article_data.news_publish_time, "inner")

    us_cities_dest = us_cities_demographics_data.join(imm_destination_city_data, us_cities_demographics_data.cidemo_state_code ==
                                                      imm_destination_city_data.value_of_alias_imm_destination_city, "inner").\
                                                          filter((us_cities_demographics_data.cidemo_count >= 30000) & (round(us_cities_demographics_data.cidemo_median_age, 0).between(30, 31)))

    df_notification = t2.join(us_cities_dest, t2.imm_port == us_cities_dest.code_of_imm_destination_city).filter(t2.news_publish_time.between('2016-04-01', '2016-04-02'))


    # df_notification = spark_session.sql(
    #     "WITH t1 AS \
    #         (SELECT * \
    #            FROM immigration_main_information_data imid \
    #          INNER JOIN imm_personal ip \
    #                 ON imid.imm_main_cic_id = ip.imm_per_cic_id \
    #              WHERE imid.imm_year = 2016 \
    #         ), t2 AS \
    #             (SELECT * \
    #                FROM t1 \
    #              INNER JOIN news_article_data nad \
    #                     ON t1.imm_arrival_date = nad.news_publish_time \
    #         ) \
    #         SELECT  t2.imm_main_cic_id \
    #                ,t2.imm_per_cic_id \
    #                ,t2.news_cord_uid \
    #                ,src.cidemo_id \
    #                ,src.value_of_imm_destination_city \
    #                ,t2.news_title \
    #                ,t2.news_abstract \
    #                ,t2.news_publish_time \
    #                ,t2.news_authors \
    #           FROM t2 \
    #         INNER JOIN (SELECT * \
    #                       FROM us_cities_demographics_data ucdd \
    #                     INNER JOIN imm_destination_city_data idcd \
    #                             ON ucdd.cidemo_state_code = idcd.value_of_alias_imm_destination_city \
    #                      WHERE ucdd.cidemo_count >= 30000 \
    #                        AND ROUND(ucdd.cidemo_median_age, 0) BETWEEN 30 AND 60 ) src \
    #                ON t2.imm_port = src.code_of_imm_destination_city \
    #          WHERE t2.news_title is not null \
    #            AND t2.news_abstract is not null \
    #            AND t2.news_publish_time BETWEEN '2016-04-01' AND '2016-05-01'\
    #     "
    # )

    # Saved in AWS S3
    numPartitions = 300
    df_notification.repartition(numPartitions) \
                   .write.partitionBy("news_publish_time") \
                         .parquet(mode="overwrite",
                                  path=f'{dest_s3_bucket}/fact_table/notification')


def main():

    # Start to access spark by spark session
    spark = create_spark_session()

    # Process data for creating dimension table: immigration
    df_imm_main_info, df_imm_person = process_dim_immigration(spark_session=spark, source_s3_bucket=SOURCE_S3_BUCKET, dest_s3_bucket=DEST_S3_BUCKET)

    # Process data for creating dimension table: news
    df_news = process_dim_news(spark_session=spark, source_s3_bucket=SOURCE_S3_BUCKET, dest_s3_bucket=DEST_S3_BUCKET)

    # Process data for creating dimension table: us cities Demographics
    df_demo = process_dim_us_cities_demographics(spark_session=spark, source_s3_bucket=SOURCE_S3_BUCKET, dest_s3_bucket=DEST_S3_BUCKET)

    # Process data for creating dimension table: label for immigration
    df_dest_city = process_dim_label(spark_session=spark, s3_access=s3_access, dest_s3_bucket=DEST_S3_BUCKET)

    # Process data for creating fact table: notifications
    process_fact_notifications(dest_s3_bucket=DEST_S3_BUCKET,
                               imm_information=df_imm_main_info,
                               imm_personal=df_imm_person,
                               news_article_data=df_news,
                               us_cities_demographics_data=df_demo,
                               imm_destination_city_data=df_dest_city)


if __name__ == "__main__":
    main()
