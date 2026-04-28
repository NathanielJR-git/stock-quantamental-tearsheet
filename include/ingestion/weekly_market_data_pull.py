import datetime
import json
import yfinance as yf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.ingestion.configuration import configuration


def download_daily_market_data():
    """Downloads daily stocks data through yfinance

    Returns:
        pd.DataFrame: stokcs data with selected metrics
    """
    print("Starts fetching daily stocks data")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    now = datetime.datetime.now()

    # Company specific data
    for ticker in configuration.TICKERS:
        print(f"Start fetching market data for {ticker}")
        
        # Fundamental and sentiment metrics (daily snapshot)
        info = yf.Ticker(ticker).info
        metrics = {
            "revenue": info.get("totalRevenue"),
            "earnings": info.get("netIncomeToCommon"),
            "ebitda": info.get("ebitda"),
            "enterprise_value": info.get("enterpriseValue"),
            "book_value": info.get("bookValue"),
            "total_debt": info.get("totalDebt"),
            "total_equity": info.get("totalStockholderEquity"),
            "total_assets": info.get("totalAssets"),
            "npm": info.get("profitMargins"),
            "dividend_yield": info.get("dividendYield"),
            "payout_ratio": info.get("payoutRatio"),
            "target_mean_price": info.get("targetMeanPrice"),
            "recommendation_mean": info.get("recommendationMean")
        }

        # Save metrics JSON to S3
        json_key = f"bronze/market_metrics/ticker={ticker}/year={now.year}/month={now.month:02d}/day={now.day:02d}/metrics.json"
        s3_hook.load_string(
            string_data=json.dumps(metrics), 
            key=json_key, 
            bucket_name=configuration.BUCKET_NAME, 
            replace=True
        )

        print(f"Successfully fetched market data & metrics for {ticker}")
        
    print("Done fetching daily stocks data")

    # Macro data
    print("Starts fetching daily macro data")
    
    # TO-DO: fetch latest risk-free rate data
    
    macro_key = f"bronze/market_metrics/ticker={ticker}/year={now.year}/month={now.month:02d}/day={now.day:02d}/metrics.json"
    s3_hook.load_string(
        string_data=..., 
        key=macro_key, 
        bucket_name=configuration.BUCKET_NAME, 
        replace=True
    )
    
    print("Done fetching daily macro data")


if __name__ == "__main__":
    print(configuration.TICKERS)