import pendulum
from airflow.hooks.base import BaseHook
from airflow.sdk import dag, task
from include.configuration import configuration
from include.pyspark.bronze_to_silver import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

@dag(
    dag_id="bronze-to-silver-pipeline",
    schedule="0 11 * * 1-5", # Every Monday - Friday, 11:00 UTC or 18:00 WIB after ingestion
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=True,
    tags=["bronze", "market_data", "news_data"]
)
def initial_load_pipeline():
    # Spark builder and Spark context setup
    spark = SparkSession.builder \
        .appName("bronze-to-silver-compute") \
        .getOrCreate()

    # Use Airflow aws_default connection
    aws_conn = BaseHook.get_connection("aws_default")
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    
    # Inject Airflow credentials to Hadoop Spark
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Spark tasks
    transform_company_profiles(spark)


def daily_pipeline():
    # Spark builder and Spark context setup
    spark = SparkSession.builder \
        .appName("bronze-to-silver-compute") \
        .getOrCreate()
    sc = spark.sparkContext

    # Use Airflow aws_default connection
    aws_conn = BaseHook.get_connection("aws_default")
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    
    # Inject Airflow credentials to Hadoop Spark
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Spark tasks
    transform_company_profiles(spark, sc)


def weekly_pipeline():
    # Spark builder and Spark context setup
    spark = SparkSession.builder \
        .appName("bronze-to-silver-compute") \
        .getOrCreate()
    sc = spark.sparkContext

    # Use Airflow aws_default connection
    aws_conn = BaseHook.get_connection("aws_default")
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    
    # Inject Airflow credentials to Hadoop Spark
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Spark tasks
    transform_company_profiles(spark, sc)


initial_load_pipeline()