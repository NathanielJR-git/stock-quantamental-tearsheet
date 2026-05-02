import pendulum
from airflow.sdk import dag
from include.configuration import configuration
from include.pyspark.bronze_to_silver import *

@dag(
    dag_id="bronze-to-silver-pipeline",
    schedule="0 11 * * 1-5", # Every Monday - Friday, 11:00 UTC or 18:00 WIB after ingestion
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=True,
    tags=["bronze", "market_data", "news_data"]
)
def initial_load_pipeline():
    ...

    # Spark tasks
    transform_company_profiles()


def daily_pipeline():
    ...


def weekly_pipeline():
    ...


initial_load_pipeline()