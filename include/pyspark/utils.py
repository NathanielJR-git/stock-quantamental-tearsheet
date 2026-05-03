import os
from airflow.sdk.bases.hook import BaseHook
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# S3A JARs downloaded at Docker build time into this directory (see Dockerfile).
# spark.jars uses direct file paths — no Ivy, no runtime downloads, no overlay2 renames.
_S3A_JARS_DIR = "/opt/airflow/spark-ext-jars"
_S3A_JARS = ",".join([
    f"{_S3A_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_S3A_JARS_DIR}/aws-java-sdk-core-1.12.262.jar",
    f"{_S3A_JARS_DIR}/aws-java-sdk-s3-1.12.262.jar",
    f"{_S3A_JARS_DIR}/aws-java-sdk-sts-1.12.262.jar",
    f"{_S3A_JARS_DIR}/aws-java-sdk-kms-1.12.262.jar",
    f"{_S3A_JARS_DIR}/aws-java-sdk-dynamodb-1.12.262.jar",
    f"{_S3A_JARS_DIR}/jmespath-java-1.12.262.jar",
    f"{_S3A_JARS_DIR}/ion-java-1.0.2.jar",
])


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a SparkSession pre-configured with the S3A JARs and credentials
    so that s3a:// paths work out of the box on both driver and executors.
    """
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName(app_name)
        .config("spark.jars", _S3A_JARS)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def apply_s3_config(sc: SparkContext):
    """
    Retrieve Airflow AWS connection credentials then 
    inject them to SparkContext (sc)
    """
    # Use Airflow aws_default connection
    aws_conn = BaseHook.get_connection("aws_default")
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    
    # Inject Airflow credentials to Hadoop Spark and set configurations
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.ap-southeast-3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.maximum", "100")