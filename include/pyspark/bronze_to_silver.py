from airflow.sdk import task
from include.configuration import configuration
from include.llm.pick_and_rate_news import extract_top_news_udf
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    ArrayType
)
from pyspark.sql.functions import (
    col, lower, to_timestamp, row_number,
    to_json,from_json, struct, collect_list,
    explode
)
from pyspark.sql.window import Window


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
def transform_news_data(spark: SparkSession, sc: SparkContext):
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts news data bronze to silver transformation")
    
    # Read gnews JSON files from S3 bronze news data
    df_news = spark.read.json(configuration.BRONZE_NEWS_PATH)

    # Cast date to timestamp and deduplicate  data
    df_news_cleaned = df_news \
        .withColumn("publication_date", to_timestamp(col("publication_date"), "EEE, dd MMM yyyy HH:mm:ss z")) \
        .dropDuplicates()
    
    window_spec = Window.partitionBy("ticker").orderBy(col("publication_date").desc())
    df_news_top_20 = df_news_cleaned \
        .withColumn("news_rank", row_number().over(window_spec)) \
        .filter(col("news_rank") <= 20) \
        .drop("news_rank")

    # Call LLM UDF to extract sentiment and to news
    df_llm_ready = df_news_top_20 \
        .groupBy("ticker") \
        .agg(
            to_json(collect_list(
                struct(
                    col("publication_date"),
                    col("title"),
                    col("summary")
                )
            )).alias("news_context_json")
        )
    
    # Construct schema for LLM outputs
    llm_output_schema = StructType([
        StructField("extracted_news", ArrayType(
            StructType([
                StructField("title", StringType(), True),
                StructField("sentiment_score", FloatType(), True),
                StructField("summary", StringType(), True)
            ])
        ), True)
    ])

    # Get raw LLM responses
    df_raw_llm = df_llm_ready.withColumn(
        "llm_raw_response",
        extract_top_news_udf(col("news_context_json"))
    )

    # Parse raw LLM JSON responses 
    df_parsed = df_raw_llm.withColumn(
        "extracted_data",
        from_json(col("llm_raw_response"), llm_output_schema)
    )

    # Select news item (title, summary, and )
    df_news_insights = df_parsed \
        .select(
            col("ticker"),
            col("extracted_data.extracted_news").alias("key_news")
        ) \
        .withColumn("news_item", explode("key_news")) \
        .select("ticker", "news_item.*") \
        .filter(col("news_item").isNotNull())

    # Save to news S3 silver path as Parquet
    df_news_insights.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.SILVER_NEWS_PATH)

    print(f"Done processing news data bronze to silver transformation")


@task.pyspark(conn_id="spark_default")
def transform_market_and_risk_data(spark: SparkSession, sc: SparkContext):
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts market data bronze to silver transformation")


    print(f"Done processing market data bronze to silver transformation")