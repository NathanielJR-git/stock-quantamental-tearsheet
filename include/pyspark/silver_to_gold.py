import os
from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F


@task
def transform_to_stock_tearsheet():
    """
    Read company profiles, news, and market and risk data,
    then join all of them and drop irrelevant columns
    (e.g. chart data columns used in chart data gold storage)
    """
    # Create SparkSession
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("silver_to_gold_tearsheet") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
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

    df_combined = df_market_and_risk_data.join(
        df_profiles_and_news,
        on=["ticker", "date"],
        how="inner"
    )

    # Take the latest data for each ticker 
    ticker_max_date_window = Window.partitionBy("ticker").orderBy(F.col("date").desc())

    df_stock_tearsheet = df_combined \
        .withColumn("row_number", F.row_number().over(ticker_max_date_window)) \
        .filter(F.col("row_number") == 1) \
        .drop("row_number")
        
    # Save to stock tearsheet S3 gold path as Parquet
    df_stock_tearsheet.write \
        .mode("overwrite") \
        .parquet(configuration.GOLD_STOCK_TEARSHEET)

    spark.stop()
    print("Done processing stock tearsheet silver to gold transformation")


@task
def transform_to_chart_data():
    """
    Read silver market and risk data, then extract chart data related columns
    """
    # Create SparkSession
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("silver_to_gold_chart") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
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
        .partitionBy("ticker") \
        .parquet(configuration.GOLD_CHART_DATA_PATH)

    spark.stop()
    print("Done processing stock tearsheet silver to gold transformation")