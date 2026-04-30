import pendulum
from airflow.sdk import dag, task
from include.ingestion.configuration import configuration
from pyspark import SparkContext
from pyspark.sql import SparkSession

@dag(
    dag_id="bronze-to-silver-pipeline",
    schedule="0 11 * * 1-5", # Every Monday - Friday, 11:00 UTC or 18:00 WIB after ingestion
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=True,
    tags=["bronze", "market_data", "news_data"]
)
def pipeline():
    
    @task.pyspark(conn_id="spark_default")
    def template(spark: SparkSession, sc: SparkContext):
        ...

    template()

pipeline()