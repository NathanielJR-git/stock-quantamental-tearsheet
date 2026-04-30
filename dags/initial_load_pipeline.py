import datetime
import pendulum
from airflow.sdk import dag, task
from include.ingestion.configuration import configuration
from include.ingestion.initial_data_pull import (
    download_historical_ohlcv_data, 
    download_company_profiles,
    download_initial_news
)

@dag(
    dag_id="initial-loading-pipeline",
    schedule=None,
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=False,
    tags=["bronze", "market_data", "news_data"]
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

    download_company_profile()
    download_market_data()
    download_news()

pipeline()