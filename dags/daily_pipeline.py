import datetime
import pendulum
from airflow.sdk import dag, task
from include.ingestion.configuration import configuration
from include.ingestion.daily_market_data_pull import download_daily_market_data
from include.ingestion.news_pull import download_news_from_gnews

@dag(
    dag_id="daily-loading-pipeline",
    schedule="0 10 * * 1-5", # Every Monday - Friday, 10:00 UTC or 17:00 WIB
    start_date=pendulum.datetime(configuration.START_YEAR, configuration.START_MONTH, configuration.START_DAY, tz="Asia/Jakarta"),
    catchup=True,
    tags=["bronze", "market_data", "news_data"]
)
def pipeline():
    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_market_data(**kwargs):
        download_daily_market_data(**kwargs)

    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_gnews(**kwargs):
        download_news_from_gnews(**kwargs)

    download_market_data()
    download_gnews()


pipeline()