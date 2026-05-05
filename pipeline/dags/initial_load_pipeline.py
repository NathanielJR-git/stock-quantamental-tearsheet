import datetime
import pendulum
from airflow.sdk import dag, task
from include.configuration import configuration
from include.ingestion.initial_data_pull import (
    download_historical_ohlcv_data, 
    download_company_profiles,
    download_initial_news
)
from include.pyspark.bronze_to_silver import transform_company_profiles

@dag(
    dag_id="initial-loading-pipeline",
    schedule=None,
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=False,
    tags=["stock_tearsheet", "bronze", "market_data", "news_data"]
)
def pipeline():
    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_company_profile():
        download_company_profiles()
        
    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_market_data():
        download_historical_ohlcv_data()
        
    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_news(**kwargs):
        download_initial_news(**kwargs)

    # Initial loading tasks dependencies
    ingest_company_profile = download_company_profile()
    ingest_market_data = download_market_data()
    ingest_news = download_news()
    silver_company_profiles = transform_company_profiles()

    [ingest_company_profile, ingest_market_data, ingest_news] >> silver_company_profiles

pipeline()