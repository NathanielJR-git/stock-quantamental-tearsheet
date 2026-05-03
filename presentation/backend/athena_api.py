from fastapi import FastAPI, HTTPException
from pyathena import connect
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
import os

app = FastAPI(
    title="Tearsheet Athena API",
    description="API to Execute AWS Athena Query to S3",
    version="1.0.0",
    docs_url=None,
    redoc_url=None
)

# Setup AWS Athena connection through cursor
load_dotenv()
cursor = connect(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            s3_staging_dir=os.getenv("ATHENA_S3_OUTPUT"),
            region_name=os.getenv("AWS_REGION"),
            work_group="primary"
        ).cursor()
    
# Tearsheet API request model
class TearsheetRecord(BaseModel):
    ticker: str
    trading_date: date
    metric_1: float
    metric_2: float
    volume: int
    turnover: float
    high_52w: float
    low_52w: float
    ytd_return: float
    market_cap: float
    optional_metric: Optional[float] = None 
    company_name: str
    sector: str
    industry: str
    news_title: str
    sentiment_score: float
    news_summary: str
    news_date: date

# Tearsheet API response model
class TearsheetResponse(BaseModel):
    status: str
    record_count: int
    data: List[TearsheetRecord]
    
# Chart Data API request model
class ChartDataPoint(BaseModel):
    trading_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    sma_20: float
    sma_50: float
    sma_100: float
    sma_200: float
    ticker: str
    
# Chart Data API response  model
class ChartDataResponse(BaseModel):
    status: str
    ticker: str
    record_count: int
    data: List[ChartDataPoint]


@app.get("/api/tearsheet-data")
async def get_tearsheet_data():
    try:
        query = f"""
        SELECT * 
        FROM "tearsheet-database"."stock_tearsheet";
        """
        cursor.execute(query)
        columns = [col_meta[0] for col_meta in cursor.description]
        raw_rows = cursor.fetchall()
        parsed_data = [dict(zip(columns, row)) for row in raw_rows]
        
        return TearsheetResponse(
            status="success",
            record_count=len(parsed_data),
            data=parsed_data
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")
    
    
@app.get("/api/chart-data/{ticker}")
async def get_tearsheet_data(ticker: str):
    try:
        query = f"""
        SELECT * 
        FROM "tearsheet-database"."chart_data"
        WHERE ticker = '{ticker}'
        ORDER BY trading_date ASC;
        """
        cursor.execute(query)
        columns = [col_meta[0] for col_meta in cursor.description]
        raw_rows = cursor.fetchall()
        if not raw_rows:
             return ChartDataResponse(
                 status="success",
                 ticker=ticker,
                 record_count=0,
                 data=[]
             )
        parsed_data = [dict(zip(columns, row)) for row in raw_rows]
        
        return ChartDataResponse(
            status="success",
            ticker=ticker,
            record_count=len(parsed_data),
            data=parsed_data
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")