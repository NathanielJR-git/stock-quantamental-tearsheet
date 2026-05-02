from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lower


@task.pyspark(conn_id="spark_default")
def transform_to_stock_tearsheet(spark: SparkSession, sc: SparkContext):
    ...


@task.pyspark(conn_id="spark_default")
def transform_to_chart_data(spark: SparkSession, sc: SparkContext):
    ...