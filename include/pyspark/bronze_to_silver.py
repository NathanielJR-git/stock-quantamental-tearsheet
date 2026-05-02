from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F, col, lower


@task.pyspark(conn_id="spark_default")
def transform_company_profiles(spark: SparkSession, sc: SparkContext):
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts company profiles bronze to silver transformation")

    # Setup schema 
    company_profiles_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("company_name", StringType(), False),
        StructField("sector", StringType(), False),
        StructField("industry", StringType(), False),
    ])

    # Read company profiles JSON file from S3
    df_bronze = spark.read \
        .schema(company_profiles_schema) \
        .json(configuration.BRONZE_COMPANY_PROFILES_PATH)

    # Transform to silver format
    df_silver = df_bronze.select(
        col("ticker"),
        col("company_name"),
        lower(col("sector")).alias("sector"),
        lower(col("industry")).alias("industry")
    )

    # Save to company profile S3 silver path as Parquet
    df_silver.write \
        .mode("overwrite") \
        .parquet(configuration.SILVER_COMPANY_PROFILES_PATH)

    print(f"Done processing company profile bronze to silver transformation")


@task.pyspark(conn_id="spark_default")
def transform_market_data(spark: SparkSession, sc: SparkContext):
    ...


@task.pyspark(conn_id="spark_default")
def transform_market_metrics(spark: SparkSession, sc: SparkContext):
    ...


@task.pyspark(conn_id="spark_default")
def transform_news_data(spark: SparkSession, sc: SparkContext):
    ...


@task.pyspark(conn_id="spark_default")
def transform_risk_free_rate(spark: SparkSession, sc: SparkContext):
    ...