from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config, create_spark_session
from pyspark.sql.window import Window
import pyspark.sql.functions as F


@task
def transform_to_stock_tearsheet():
    """
    Read company profiles, news, and market and risk data,
    then join all of them and drop irrelevant columns
    (e.g. chart data columns used in chart data gold storage)
    """
    spark = create_spark_session("silver_to_gold_tearsheet")
    
    sc = spark.sparkContext
    
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts stock tearsheet silver to gold transformation")

    df_company_profiles = spark.read.parquet(configuration.SILVER_COMPANY_PROFILES_PATH)
    df_news = spark.read.parquet(configuration.SILVER_NEWS_PATH)
    df_market_and_risk_data = spark.read \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH) \
        .drop("close", "high", "low", "open", "volume",
              "sma_20", "sma_50", "sma_100", "sma_200")

    # Get the single latest market-and-risk snapshot per ticker.
    latest_window = Window.partitionBy("ticker").orderBy(F.col("date").desc())
    df_latest_market = df_market_and_risk_data \
        .withColumn("rn", F.row_number().over(latest_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")

    # Combine all data
    df_stock_tearsheet = df_latest_market \
        .join(df_company_profiles, on="ticker", how="inner") \
        .join(df_news, on="ticker", how="left")

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
    spark = create_spark_session("silver_to_gold_chart")
    
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