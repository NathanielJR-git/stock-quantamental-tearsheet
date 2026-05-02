import json
import re
import requests
import yfinance as yf
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from include.configuration import configuration


def download_weekly_metrics_data(**kwargs):
    """
    Downloads daily stocks data through yfinance
    """
    print("Starts fetching daily stocks data")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = kwargs.get('ds')
    if not ds:
        raise ValueError("Macro {{ds}} is not found, make sure function is called via PythonOperator(provide_context=True)")
    year, month, day = ds.split("-")
    month = int(month)
    day = int(day)

    # Company specific data
    for ticker in configuration.TICKERS:
        print(f"Start fetching market data for {ticker}")
        
        # Fundamental and sentiment metrics (daily snapshot)
        current_ticker = yf.Ticker(ticker)
        info = current_ticker.info
        fin = current_ticker.financials

        # Helper to safely grab the most recent value from financial statements
        def get_latest(df, label):
            if df is not None and not df.empty and label in df.index:
                return df.loc[label].iloc[0]
            return None

        metrics = {
            # Valuation (related)
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "book_value": info.get("bookValue"),
            "earnings": info.get("netIncomeToCommon") or get_latest(fin, "Net Income Common Stockholders"),
            # Dividends
            "dividend_yield": info.get("dividendYield"),
            "payout_ratio": info.get("payoutRatio"),
            # Other data
            "target_mean_price": info.get("targetMeanPrice"),
            "recommendation_mean": info.get("recommendationMean"),
            "market_cap": info.get("marketCap"), 
            "shares_outstanding": info.get("sharesOutstanding"), 
            "free_float": info.get("floatShares"),
            # Price
            "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
        }

        # Save metrics JSON to S3
        json_key = f"bronze/market_metrics/ticker={ticker}/year={year}/month={month:02d}/day={day:02d}/metrics.json"
        s3_hook.load_string(
            string_data=json.dumps(metrics), 
            key=json_key, 
            bucket_name=configuration.BUCKET_NAME, 
            replace=True
        )

        print(f"Successfully fetched market data & metrics for {ticker}")
        
    print("Done fetching daily stocks data")
    
    
def download_risk_free_rate_data(**kwargs):
    """
    Downloads risk-free rate data from investing.com
    """
    print("Starts fetching daily risk-free rate data")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    ds = kwargs.get('ds')
    if not ds:
        raise ValueError("Macro {{ds}} is not found, make sure function is called via PythonOperator(provide_context=True)")
    year, month, day = ds.split("-")
    month = int(month)
    day = int(day)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(
            configuration.RISK_FREE_RATE_URL, 
            headers=headers, 
            timeout=10
        )
        response.raise_for_status() 
        
        soup = BeautifulSoup(response.text, 'html.parser')
        text_content = soup.find("h2", id="description").get_text()
        match = re.search(r'([0-9]+\.[0-9]+)%', text_content, re.IGNORECASE)
        
        if not match:
            raise ValueError("Regex pattern not found inside text content")
            
        # Save in decimal format
        yield_decimal = float(match.group(1)) / 100 
            
    except Exception as e:
        print(f"Error scraping data: {e}")
        raise AirflowSkipException("Risk-free rate data scraping failed")
    
    # Save risk-free rate data to S3
    rf_data = {"risk-free-rate": yield_decimal}
    rf_key = f"bronze/risk-free-rate/year={year}/month={month:02d}/day={day:02d}/risk-free-rate.json"
    
    s3_hook.load_string(
        string_data=json.dumps(rf_data),
        key=rf_key, 
        bucket_name=configuration.BUCKET_NAME, 
        replace=True
    )
    
    print("Done fetching daily risk-free rate data")