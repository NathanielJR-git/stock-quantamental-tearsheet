from datetime import date
import os

from typing import List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pyathena import connect

app = FastAPI(
    title="Tearsheet Athena API",
    description="API to Execute AWS Athena Query to S3",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
)

load_dotenv()
cursor = connect(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    s3_staging_dir=os.getenv("ATHENA_S3_OUTPUT"),
    region_name=os.getenv("AWS_REGION"),
    work_group="primary",
).cursor()


class TearsheetRecord(BaseModel):
    """Stock tearsheet rows: Athena CSV columns validated; common API names on serialize."""
    
    model_config = ConfigDict(populate_by_name=True)

    ticker: str
    trading_date: date = Field(
        validation_alias="date",
        serialization_alias="trading_date",
    )
    news_title: Optional[str] = Field(
        default=None,
        validation_alias="title",
        serialization_alias="news_title",
    )
    news_summary: Optional[str] = Field(
        default=None,
        validation_alias="summary",
        serialization_alias="news_summary",
    )

    book_value: Optional[float] = None
    dividend_yield: Optional[float] = None
    earnings: Optional[float] = None
    ev_ebitda: Optional[float] = None
    free_float: Optional[float] = None
    market_cap: Optional[float] = None
    payout_ratio: Optional[float] = None
    recommendation_mean: Optional[float] = None
    shares_outstanding: Optional[float] = None
    target_mean_price: Optional[float] = None
    risk_free_rate: Optional[float] = None
    return_1d: Optional[float] = Field(
        default=None,
        validation_alias="1d_return",
        serialization_alias="1d_return",
    )
    return_1w: Optional[float] = Field(
        default=None,
        validation_alias="1w_return",
        serialization_alias="1w_return",
    )
    return_1m: Optional[float] = Field(
        default=None,
        validation_alias="1m_return",
        serialization_alias="1m_return",
    )
    return_3m: Optional[float] = Field(
        default=None,
        validation_alias="3m_return",
        serialization_alias="3m_return",
    )
    return_6m: Optional[float] = Field(
        default=None,
        validation_alias="6m_return",
        serialization_alias="6m_return",
    )
    sharpe_ratio_252d: Optional[float] = None
    var_95_252d: Optional[float] = None
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sentiment_score: Optional[float] = None
    news_date: Optional[date] = None

    high_52w: Optional[float] = Field(
        default=None,
        validation_alias="fifty_two_week_high",
        serialization_alias="high_52w",
    )
    low_52w: Optional[float] = Field(
        default=None,
        validation_alias="fifty_two_week_low",
        serialization_alias="low_52w",
    )
    metric_1: Optional[float] = Field(
        default=None,
        validation_alias="pbv",
        serialization_alias="metric_1",
    )
    metric_2: Optional[float] = Field(
        default=None,
        validation_alias="eps",
        serialization_alias="metric_2",
    )
    optional_metric: Optional[float] = Field(
        default=None,
        validation_alias="per",
        serialization_alias="optional_metric",
    )
    ytd_return: Optional[float] = Field(
        default=None,
        validation_alias="12m_return",
        serialization_alias="ytd_return",
    )


class TearsheetResponse(BaseModel):
    status: str
    record_count: int
    data: List[TearsheetRecord]


class ChartDataPoint(BaseModel):
    """chart_data Athena/CSV: date + open/high/low/close + volume + sma_* + ticker."""

    model_config = ConfigDict(populate_by_name=True)

    trading_date: date = Field(
        validation_alias="date",
        serialization_alias="trading_date",
    )
    open_price: Optional[float] = Field(
        default=None,
        validation_alias="open",
        serialization_alias="open_price",
    )
    high_price: Optional[float] = Field(
        default=None,
        validation_alias="high",
        serialization_alias="high_price",
    )
    low_price: Optional[float] = Field(
        default=None,
        validation_alias="low",
        serialization_alias="low_price",
    )
    close_price: Optional[float] = Field(
        default=None,
        validation_alias="close",
        serialization_alias="close_price",
    )
    volume: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_100: Optional[float] = None
    sma_200: Optional[float] = None
    ticker: str


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
            data=parsed_data,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")


@app.get("/api/chart-data/{ticker}")
async def get_tearsheet_data(ticker: str):
    try:
        safe = ticker.replace("'", "''")
        query = f"""
        SELECT *
        FROM "tearsheet-database"."chart_data"
        WHERE ticker = '{safe}'
        ORDER BY "date" ASC;
        """
        cursor.execute(query)
        columns = [col_meta[0] for col_meta in cursor.description]
        raw_rows = cursor.fetchall()
        if not raw_rows:
            return ChartDataResponse(
                status="success",
                ticker=ticker,
                record_count=0,
                data=[],
            )
        parsed_data = [dict(zip(columns, row)) for row in raw_rows]

        return ChartDataResponse(
            status="success",
            ticker=ticker,
            record_count=len(parsed_data),
            data=parsed_data,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {e}")