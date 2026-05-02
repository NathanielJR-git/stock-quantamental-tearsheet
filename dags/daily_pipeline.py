import datetime
import pendulum
from airflow.sdk import dag, task
from include.configuration import configuration
from include.ingestion.daily_market_data_pull import download_daily_market_data
from include.ingestion.news_pull import download_news_from_gnews
from include.pyspark.bronze_to_silver import transform_news_data, transform_market_and_risk_data

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

    # Data ingestion
    ingest_market_data = download_market_data()
    ingest_news = download_gnews()

    # Transform bronze to silver format
    silver_transform_news_data = transform_news_data()
    silver_transform_market_and_risk = transform_market_and_risk_data()


    # Transform silver to gold format
    [ingest_market_data, ingest_news] >> \
    [silver_transform_market_and_risk, silver_transform_news_data]

pipeline()