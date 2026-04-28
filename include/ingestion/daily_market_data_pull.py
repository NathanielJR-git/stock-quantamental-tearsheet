import datetime
import json
import pandas as pd
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration


def download_daily_market_data():
    """
    Downloads daily stocks data through yfinance
    """
    # Check if market is open
    market_check = yf.download("^JKSE", period="1d", interval="1d")
    if market_check.empty:
        raise AirflowSkipException("Market is closed today, aborting daily market data ingestion")
    
    print("Starts fetching daily stocks data")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    now = datetime.datetime.now()

    # Company specific data
    for ticker in configuration.TICKERS:
        print(f"Start fetching market data for {ticker}")
        
        # Today's market data
        df = yf.download(ticker, period="1d", interval="1d", auto_adjust=True)
            
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)


        # Save OHLCV CSV to S3
        csv_key = f"bronze/market_data/ticker={ticker}/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.csv"
        s3_hook.load_string(
            string_data=df.to_csv(index=True), 
            key=csv_key, 
            bucket_name=configuration.BUCKET_NAME, 
            replace=True
        )

        print(f"Successfully fetched market data & metrics for {ticker}")
        
    print("Done fetching daily stocks data")