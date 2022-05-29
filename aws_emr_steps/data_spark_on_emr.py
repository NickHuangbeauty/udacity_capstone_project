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
    """process_dim_immigration _summary_

    _extended_summary_

    Args:
        spark (_type_): _description_
        SOURCE_S3_BUCKET (_type_): _description_
        DEST_S3_BUCKET (_type_): _description_

    Returns:
        _type_: _description_
    """
    imm_path = os.path.join(SOURCE_S3_BUCKET, "data/immigration_data/immigration_apr16_sub.sas7bdat")

    df_imm_data = spark.read.format("com.github.saurfang.sas.spark").load(imm_path)

    # Dimension Table: Immigration personal data
    df_immigration_personal = df_imm_data.withColumn("imm_per_cic_id", col("cicid").cast("String")) \
                                            .withColumn("imm_person_birth_year", col("biryear").cast("Integer")) \
                                                .withColumn("imm_person_gender", col("gender").cast("String")) \
                                                    .withColumn("imm_visatype", col("visatype").cast("String")) \
                                        .select(col("imm_per_cic_id"),
                                                    col("imm_person_birth_year"),
                                                        col("imm_person_gender"),
                                                            col("imm_visatype"))

    df_immigration_personal_tmp = df_immigration_personal.createOrReplaceTempView("imm_personal")

    df_immigration_personal_tmp = spark.sql("SELECT * FROM imm_personal")

    df_immigration_personal_tmp.persist()

    # df_immigration_personal_tmp.explain()

    df_immigration_personal_tmp.write.mode("override") \
                               .partitionBy("imm_person_birth_year") \
                               .parquet(path=f'{DEST_S3_BUCKET}dimension_table/df_immigration_personal')

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

        # df_immigration_main_information_tmp.explain()

        df_immigration_main_information_tmp.write.mode("override") \
                                           .partitionBy("imm_year", "imm_month") \
                                           .parquet(path=f'{DEST_S3_BUCKET}dimension_table/immigration_main_information')


def process_dim_news(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET) -> None:
    news_path = os.path.join(SOURCE_S3_BUCKET, "data/news_data/metadata.csv")

    df_news = spark.read.options(header=True, delimiter=',').csv(path=news_path)

    df_news = df_news.withColumn("news_cord_uid", col("cord_uid").cast("String")) \
                        .withColumn("news_source", col("source_x").cast("String")) \
                            .withColumn("news_title", col("title").cast("String")) \
                                .withColumn("news_licence", col("license").cast("String")) \
                                    .withColumn("news_abstract", col("abstract").cast("String")) \
                                        .withColumn("news_publish_time", to_date(df_news.publish_time, "yyyy-MM-dd")) \
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

    df_news_tmp = df_news.createOrReplaceTempView("news_article_data")

    df_news_tmp = spark.sql("SELECT DISTINCT publish_time FROM news_article_data")

    df_news_tmp.persist()

    # df_news_tmp.explain()

    df_news_tmp.write.mode("overwrite") \
                     .partitionBy("news_publish_time") \
                     .parquet(path=f'{DEST_S3_BUCKET}dimension_table/news_article_data')


def process_dim_us_cities_demographics(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET) -> None:
    us_cities_demographics_path = os.path.join(SOURCE_S3_BUCKET, "data/usCitiesDemographics_data/usCitiesDemo.csv")

    df_us_cities_demographics = spark.read.options(header=True, delimiter=';').csv(us_cities_demographics_path)

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
    df_us_cities_demographics = df_us_cities_demographics.withColumn("cidemo_id", monotonically_increasing_id())

    df_us_cities_demographics_temp = df_news.createOrReplaceTempView("us_cities_demographics_data")

    df_us_cities_demographics_temp = spark.sql("SELECT * FROM us_cities_demographics_data")

    df_us_cities_demographics_temp.persist()

    # df_us_cities_demographics_temp.explain()

    df_us_cities_demographics_temp.write.mode("overwrite") \
                                  .parquet(path = f'{DEST_S3_BUCKET}dimension_table/us_cities_demographics_data')


def process_dim_label(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET) -> None:
    imm_label_path = os.path.join(SOURCE_S3_BUCKET, "data/immigration_data/immigration_labels_descriptions.SAS")

    with open(imm_label_path) as f:
        context = f.read().replace('\t', '')

    def code_mapping(context, idx):
        content_mapping = context[context.index(idx):]
        content_line_split = content_mapping[:content_mapping.index(';')].split('\n')
        content_line_list = [line.replace("'", "") for line in content_line_split]
        content_dict = [i.split('=') for i in content_line_list[1:]]
        content_dict = [[i[0].strip(), i[1].strip().split(', ')[:][0], e] for i in content_dict if len(i) == 2 for e in i[1].strip().split(', ')[1:]]
        return content_dict
    
    imm_cit_res = code_mapping(context, "i94cntyl")
    imm_port = code_mapping(context, "i94prtl")
    imm_mode = code_mapping(context, "i94model")
    imm_addr = code_mapping(context, "i94addrl")
    imm_visa = {'1': 'Business',
                '2': 'Pleasure',
                '3': 'Student'}

    df_imm_city_res_label = spark.sparkContext.parallelize(imm_cit_res.items()).toDF(["col_of_imm_cntyl", "value_of_imm_cntyl"]) \
                                              .withColumn("col_of_imm_cntyl", col("col_of_imm_cntyl").cast("Integer")) \
                                              .withColumn("value_of_imm_cntyl", col("value_of_imm_cntyl").cast("String"))

    # Saved in AWS S3
    df_imm_city_res_label.write.mode("overwrite") \
                         .parquet(path=f'{DEST_S3_BUCKET}dimension_table/imm_city_res_label')

    df_imm_destination_city = spark.sparkContext.parallelize(imm_port).toDF(["code_of_imm_destination_city", "value_of_imm_destination_city", "value_of_alias_imm_destination_city"]) \
                                                .withColumn("code_of_imm_destination_city", col("code_of_imm_destination_city").cast("String")) \
                                                .withColumn("value_of_imm_destination_city", col("value_of_imm_destination_city").cast("String")) \
                                                .withColumn("value_of_alias_imm_destination_city", col("value_of_alias_imm_destination_city").cast("String"))

    # For querying and joining other tables.
    df_imm_destination_city_tmp = df_imm_destination_city.createOrReplaceTempView("imm_destination_city_data")

    df_imm_destination_city_tmp = spark.sql("SELECT * FROM imm_destination_city_data")

    df_imm_destination_city_tmp.persist()

    # Saved in AWS S3
    df_imm_destination_city_tmp.write.mode("overwrite") \
                               .parquet(path = f'{DEST_S3_BUCKET}dimension_table/imm_destination_city')

    df_imm_travel_code = spark.sparkContext.parallelize(imm_mode.items()).toDF(["code_of_imm_travel_code", "value_of_imm_travel_code"]) \
                                           .withColumn("code_of_imm_travel_code", col("code_of_imm_travel_code").cast("Integer")) \
                                           .withColumn("value_of_imm_travel_code", col("value_of_imm_travel_code").cast("String"))
    # Saved in AWS S3
    df_imm_travel_code.write.mode("overwrite") \
                      .parquet(path = f'{DEST_S3_BUCKET}dimension_table/imm_travel_code')

    df_imm_address = spark.sparkContext.parallelize(imm_addr.items()).toDF(["code_of_imm_address", "value_of_imm_address"]) \
                                       .withColumn("code_of_imm_address", col("code_of_imm_address").cast("String")) \
                                       .withColumn("value_of_imm_address", col("value_of_imm_address").cast("String"))

    # Saved in AWS S3
    df_imm_address.write.mode("overwrite") \
                  .parquet(path = f'{DEST_S3_BUCKET}dimension_table/imm_address')

    df_imm_visa = spark.sparkContext.parallelize(imm_visa.items()).toDF(["code_of_imm_visa", "value_of_imm_visa"]) \
                                    .withColumn("code_of_imm_visa", col("code_of_imm_visa").cast("Integer")) \
                                    .withColumn("value_of_imm_visa", col("value_of_imm_visa").cast("String"))

    # Saved in AWS S3
    df_imm_visa.write.mode("overwrite") \
               .parquet(path = f'{DEST_S3_BUCKET}dimension_table/imm_visa')


def process_fact_notifications(spark, DEST_S3_BUCKET) -> None:
    """process_fact_notifications _summary_

    _extended_summary_
        ** t1: join imm two tables
        ** t2: join news table with t1
        ** t3: join us cities table with t2
    Args:
        spark (_type_): _description_
        SOURCE_S3_BUCKET (_type_): _description_
        DEST_S3_BUCKET (_type_): _description_
    """
    df_notification = spark.sql(
        "WITH t1 AS \
            (SELECT * \
               FROM immigration_main_information_data imid \
             LEFT JOIN imm_personal ip \
                    ON imid.imm_main_cic_id = ip.imm_per_cic_id \
                 WHERE imid.imm_year = 2016 \
            ), t2 AS \
                (SELECT * \
                   FROM t1 \
                 LEFT JOIN news_article_data nad \
                        ON t1.imm_arrival_date = nad.news_publish_time \
            ) \
            SELECT t2.imm_main_cic_id \
                   t2.imm_per_cic_id \
                   t2.news_cord_uid \
                   src.cidemo_id \
                   src.value_of_imm_destination_city \
                   t2.news_title \
                   t2.news_abstract \
                   t2.news_publish_time \
                   t2.news_authors \
              FROM t2 \
            LEFT JOIN (SELECT * FROM us_cities_demographics_data ucdd INNER JOIN imm_destination_city_data idcd ON ucdd.cidemo_state_code = idcd.value_of_alias_imm_destination_city) src \
                   ON t2.imm_port = src.code_of_imm_destination_city \
        "
    )

    # Saved in AWS S3
    df_notification.write.mode("overwrite") \
                   .partitionBy("news_publish_time") \
                   .parquet(path = f'{DEST_S3_BUCKET}fact_table/notification')

def main():

    # Start to access spark by spark session
    spark = create_spark_session()

    # Process data for creating dimension table: immigration
    process_dim_immigration(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET)

    # Process data for creating dimension table: news
    process_dim_news(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET)

    # Process data for creating dimension table: us cities Demographics
    process_dim_us_cities_demographics(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET)

    # Process data for creating dimension table: label for immigration
    process_dim_label(spark, SOURCE_S3_BUCKET, DEST_S3_BUCKET)

    # Process data for creating fact table: notifications
    process_fact_notifications(spark, DEST_S3_BUCKET)


if __name__ == "__main__":
    main()
