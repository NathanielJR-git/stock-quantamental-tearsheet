import datetime
import pandas as pd
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration


def download_initial_stocks_data():
    """Downloads stocks data through yfinance

    Returns:
        pd.DataFrame: ...
    """
    return yf.download(
        tickers=configuration.TICKERS,
        period="1mo",
        interval="1d",
        group_by="ticker",
        auto_adjust=True
    )


def download_company_profiles():
    """Downloads stocks' company profile data

    Returns:
        pd.DataFrame: ...
    """
    return yf.download(
        tickers=configuration.TICKERS,
        period="1mo",
        interval="1d",
        group_by="ticker",
        auto_adjust=True
    )


def download_daily_stocks_data():
    """Downloads daily stocks data through yfinance

    Returns:
        pd.DataFrame: stokcs data with selected columns 
    """
    # Log: starts fetching daily stocks data
    print("Starts fetching daily stocks data")

    for ticker in configuration.TICKERS:
        print(f"Start fetching market data for {ticker}")

        # Close, High, Low, Open, Volume
        df = yf.download(
            ticker, 
            period="1d", 
            interval="1d",
            auto_adjust=True
        )

        if df.empty:
            raise AirflowSkipException(f"Market is not open today, skipping task")
        df.columns = df.columns.droplevel(1)

        # Ticker Info
        info = yf.Ticker(ticker).info



        # Load to S3
        now = datetime.now()
        csv_data = df.to_csv(index=True)
        s3_key = f"bronze/market_data/ticker={ticker}/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.csv"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=csv_data,
            key=s3_key,
            bucket_name=configuration.BUCKET_NAME,
            replace=True
        )

        print(f"Successfully fetched market data for {ticker}")

    # Log: done fetching daily stocks data
    print("Done fetching daily stocks data")

def download_daily_macro_data():
    """Downloads daily risk free rate data

    Returns:
        pd.DataFrame: ...
    """
    return yf.download(
        tickers=configuration.TICKERS,
        period="1mo",
        interval="1d",
        group_by="ticker",
        auto_adjust=True
    )


if __name__ == "__main__":
    print(configuration.TICKERS)