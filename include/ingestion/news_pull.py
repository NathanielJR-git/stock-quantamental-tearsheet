import json
import pandas as pd
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration


def download_news_from_yfinance(**kwargs):
    print("Starts fetching stocks news data from yfinance")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = kwargs.get('ds')
    if not ds:
        raise ValueError("Macro {{ds}} is not found, make sure function is called via PythonOperator(provide_context=True)")
    year, month, day = ds.split("-")
   
    for ticker in configuration.TICKERS:
        ticker_news = yf.Ticker(ticker)
        news = ticker_news.news
            
        # Check if news is empty
        if not news:
            continue

        extracted_news = []
        # Load news
        for news_item in news:
            content = news_item['content']
            extracted_news.append({
                "title": content["title"],
                "summary": content["summary"],
                "publication_date": content["pubDate"]
            })
            
        # Save news data to S3
        s3_key = f"bronze/news_data/ticker={ticker}/year={year}/month={month:02d}/day={day:02d}/historical_data.json"
        s3_hook.load_string(
            string_data=json.dumps(extracted_news), 
            key=s3_key, 
            bucket_name=configuration.BUCKET_NAME, 
            replace=True
        )
        
    print("Done fetching stocks news data from yfinance")