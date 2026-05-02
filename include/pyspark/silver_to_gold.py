from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F


@task.pyspark(conn_id="spark_default")
def transform_to_stock_tearsheet(spark: SparkSession, sc: SparkContext):
    """
    Read company profiles, news, and market and risk data,
    then join all of them and drop irrelevant columns
    (e.g. chart data columns used in chart data gold storage)
    """
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts stock tearsheet silver to gold transformation")

    # Read company profiles, news, and market and risk data
    df_company_profiles = spark.read.parquet(configuration.SILVER_COMPANY_PROFILES_PATH)
    df_news = spark.read.parquet(configuration.SILVER_NEWS_PATH)
    df_market_and_risk_data = spark.read \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH) \
        .drop(
            "close", "high", "low", "open", "volume",
            "sma_20", "sma_50", "sma_100", "sma_200", 
        )
    
    # Combine all silver data
    df_profiles_and_news = df_company_profiles.join(
        df_news,
        on="ticker",
        how="inner"
    )

    df_stock_tearsheet = df_market_and_risk_data.join(
        df_profiles_and_news,
        on=["ticker", "date"],
        how="left"
    ).dropna(subset=["title"])

    # Save to stock tearsheet S3 gold path as Parquet
    df_stock_tearsheet.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.GOLD_STOCK_TEARSHEET)

    print("Done processing stock tearsheet silver to gold transformation")


@task.pyspark(conn_id="spark_default")
def transform_to_chart_data(spark: SparkSession, sc: SparkContext):
    """
    Read silver market and risk data, then extract chart data related columns
    """
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts stock tearsheet silver to gold transformation")

    # Read silver market dan risk data and only extract chart data
    df_chart_data = spark.read \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH) \
        .select(
            "ticker", "date", "close", "high", "low", "open", "volume",
            "sma_20", "sma_50", "sma_100", "sma_200", 
        )
    
    # Save to chart data S3 gold path as Parquet
    df_chart_data.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.GOLD_CHART_DATA_PATH)

    print("Done processing stock tearsheet silver to gold transformation")