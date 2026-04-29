import datetime
import pendulum
from airflow.sdk import dag, task
from include.ingestion.weekly_market_data_pull import (
    download_weekly_market_data, 
    download_risk_free_rate_data
)

@dag(
    dag_id="weekly-loading-pipeline",
    schedule="0 10 * * 1", # Every Monday, 10:00 UTC or 17:00 WIB
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
    def download_market_data(**kwargs):
        download_weekly_market_data(**kwargs)

    @task(
        retries=3,
        retry_delay=datetime.timedelta(minutes=2),
        retry_exponential_backoff=True
    )
    def download_macro_data(**kwargs):
        download_risk_free_rate_data(**kwargs)

    download_market_data()
    download_macro_data()

pipeline()