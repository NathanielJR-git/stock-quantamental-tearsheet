import pandas as pd
import pendulum
from airflow.sdk import dag, task
from include.ingestion.yfinance_pull import download_stocks_data

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 28, tz="Asia/Jakarta"),
    catchup=False,
    tags=["Stock Quantamental Tearsheet Pipeline DAG"]
)
def pipeline():
    @task
    def test_yfinance_pull():
        stocks_data = download_stocks_data()
        stocks_data.to_csv("/opt/airflow/include/test.csv", index=False)

    test_yfinance_pull()

pipeline()