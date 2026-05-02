from airflow.sdk import task
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F

@task.pyspark(conn_id="spark_default")
def separated_example(spark: SparkSession):
    ...