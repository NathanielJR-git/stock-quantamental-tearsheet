import datetime
import pendulum
from airflow.sdk import dag, task
from include.ingestion.weekly_market_data_pull import (
    download_weekly_market_data, 
    download_risk_free_rate_data
)

@dag(
    dag_id="weekly-loading-pipeline",
    schedule="0 17 * * 1",
    start_date=pendulum.datetime(2026, 4, 29, tz="Asia/Jakarta"),
    catchup=True,
    tags=["bronze", "market_data", "news_data"]
)
def pipeline():
    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_market_data():
        download_weekly_market_data()

    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_macro_data():
        download_risk_free_rate_data()

    download_market_data()
    download_macro_data()

pipeline()