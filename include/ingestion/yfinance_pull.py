import pandas as pd
import yfinance as yf
from include.ingestion.tickers import TICKERS


def download_stocks_data():
    """Downloads stocks data through yfinance

    Returns:
        pd.DataFrame: stokcs data containing the following columns: ... 
    """
    return yf.download(
        tickers=TICKERS,
        period="1mo",
        interval="1d",
        group_by="ticker",
        auto_adjust=True
    )


if __name__ == "__main__":
    print(TICKERS)