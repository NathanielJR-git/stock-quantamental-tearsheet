from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F


@task.pyspark(conn_id="spark_default")
def transform_to_stock_tearsheet(spark: SparkSession, sc: SparkContext):
    """
    
    """
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts stock tearsheet silver to gold transformation")



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