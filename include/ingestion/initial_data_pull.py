import datetime
import json
import pandas as pd
import yfinance as yf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration
from gnews import GNews


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
        
    print("Done fetching initial stocks data (5 Years)")


def download_company_profiles():
    """
    Downloads company profiles data for all tickers
    """
    print("Starts fetching company profiles")
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Save company profiles to S3
    s3_hook.load_string(
        string_data=json.dumps(configuration.STATIC_COMPANY_PROFILES),
        key="bronze/company_profiles/profiles.json",
        bucket_name=configuration.BUCKET_NAME,
        replace=True
    )

    print("Done fetching company profiles")
    
    
def download_initial_news(**kwargs):
    """
    Downloads initial news data ranging from today to 6 months prior from gnews
    """
    print("Starts fetching stocks news data from gnews")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = kwargs.get('ds')
    if not ds:
        raise ValueError("Macro {{ds}} is not found, make sure function is called via PythonOperator(provide_context=True)")
   
    end_dt = datetime.datetime.strptime(ds, "%Y-%m-%d")
    start_dt = start_dt - datetime.timedelta(days=90)
    
    # Setup GNews for Indonesian news search
    google_news = GNews(
        language='id', 
        country='ID', 
        start_date=(start_dt.year, start_dt.month, start_dt.day),
        end_date=(end_dt.year, end_dt.month, end_dt.day),
        max_results=20
    )

    # Fetch news for each stock ticker
    for profile in configuration.STATIC_COMPANY_PROFILES:
        ticker = profile["ticker"]
        company_name = profile["company_name"]
        
        print(f"Fetching news for {ticker} ({company_name})")
        
        # Keywords used for searching news (can be further improved)
        search_query = f'"{company_name}" OR "{ticker.replace(".JK", "")}"'
        news_data = google_news.get_news(search_query)
        
        if not news_data:
            print(f"No news found for {ticker}")
            continue

        # Fetch title, description and published date of each news item
        extracted_news = []
        for news_item in news_data:
            extracted_news.append({
                "title": news_item["title"],
                "summary": news_item["description"],
                "publication_date": news_item["pulished date"]
            })
            
        # Save ticker's news data to S3
        s3_key = f"bronze/news_data/ticker={ticker}/year={start_dt.year}/month={start_dt.month:02d}/day={start_dt.day:02d}/gnews.json"
        s3_hook.load_string(
            string_data=json.dumps(extracted_news),
            key=s3_key,
            bucket_name=configuration.BUCKET_NAME,
            replace=True
        )
        
    print("Done fetching stocks news data from gnews")