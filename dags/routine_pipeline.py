import pandas as pd
import pendulum
from airflow.sdk import dag, task
from include.ingestion.daily_market_data_pull import download_daily_market_data
from include.ingestion.weekly_market_data_pull import download_weekly_market_data

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 28, tz="Asia/Jakarta"),
    catchup=False,
    tags=["Stock Quantamental Tearsheet Pipeline DAG"]
)
def pipeline():
    @task
    def test_yfinance_pull():
        ...

    test_yfinance_pull()

pipeline()