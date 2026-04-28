import pandas as pd
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration


def download_historical_ohlcv_data():
    """
    Downloads historical OHLCV data through yfinance
    """
    print("Starts fetching initial stocks data (5 Years)")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    for ticker in configuration.TICKERS:
        df = yf.download(
            ticker, 
            period="5y", 
            interval="1d", 
            auto_adjust=True
        )
        if df.empty:
            continue
            
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
            
        # Save OHLCV to S3
        csv_data = df.to_csv(index=True)
        s3_key = f"bronze/market_data/ticker={ticker}/historical_data.csv"
        s3_hook.load_string(
            string_data=csv_data, 
            key=s3_key, 
            bucket_name=configuration.BUCKET_NAME, 
            replace=True
        )


def download_company_profiles():
    """
    Downloads company profiles data for all tickers
    """
    print("Starts fetching company profiles")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    profiles = []

    # Fetch each stock's company profile
    for ticker in configuration.TICKERS:
        info = yf.Ticker(ticker).info
        profile = {
            "ticker": ticker,
            "company_name": info.get("longName"),
            "logo_url": info.get("logo_url", ""),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "shares_outstanding": info.get("sharesOutstanding"),
            "free_float": info.get("floatShares"),
            "beta": info.get("beta")
        }
        profiles.append(profile)

    # Save company profiles to S3
    df_profiles = pd.DataFrame(profiles)
    s3_hook.load_string(
        string_data=df_profiles.to_csv(index=False),
        key="bronze/company_profiles/profiles.csv",
        bucket_name=configuration.BUCKET_NAME,
        replace=True
    )