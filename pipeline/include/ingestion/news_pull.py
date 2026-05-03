import json
import datetime
import pandas as pd
import yfinance as yf
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.configuration import configuration
from gnews import GNews


def download_news_from_gnews(**kwargs):
    """
    Downloads news data for all tickes from gnews (Google News)
    """
    print("Starts fetching stocks news data from gnews")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = kwargs.get('ds')
    if not ds:
        raise ValueError("Macro {{ds}} is not found, make sure function is called via PythonOperator(provide_context=True)")
   
    start_dt = datetime.datetime.strptime(ds, "%Y-%m-%d")
    end_dt = start_dt + datetime.timedelta(days=1)
    
    # Setup GNews for Indonesian news search
    google_news = GNews(
        language='id', 
        country='ID', 
        start_date=(start_dt.year, start_dt.month, start_dt.day),
        end_date=(end_dt.year, end_dt.month, end_dt.day),
        max_results=15
    )

    # Fetch news for each stock ticker
    for profile in configuration.STATIC_COMPANY_PROFILES:
        ticker = profile.get("ticker")
        company_name = profile.get("company_name")
        
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
                "title": news_item.get("title"),
                "summary": news_item.get("description"),
                "publication_date": news_item.get("published date")
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