import os
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
    explode, to_date, concat_ws, last, avg,
    lag
)
from pyspark.sql.window import Window


@task.pyspark(conn_id="spark_default")
def transform_company_profiles(spark: SparkSession, sc: SparkContext):
    """
    Convert company profiles file format from JSON to Parquet
    """
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
    df_silver_profiles = df_bronze.select(
        col("ticker"),
        col("company_name"),
        lower(col("sector")).alias("sector"),
        lower(col("industry")).alias("industry")
    )

    # Save to company profile S3 silver path as Parquet
    df_silver_profiles.write \
        .mode("overwrite") \
        .parquet(configuration.SILVER_COMPANY_PROFILES_PATH)

    print(f"Done processing company profile bronze to silver transformation")


GROQ_KEY = os.getenv("GROQ_API_KEY")

@task.pyspark(
    conn_id="spark_default",
    conf={
        "spark.executorEnv.GROQ_API_KEY": GROQ_KEY
    }
)
def transform_news_data(spark: SparkSession, sc: SparkContext):
    """
    Call Groq API for the latest 20 news form each stocks
    to get the top 3 most important news and each sentiment scores
    using Pandas UDF and Groq API's Llama 3.3 versatile model
    """
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
    df_silver_news = df_parsed \
        .select(
            col("ticker"),
            col("extracted_data.extracted_news").alias("key_news")
        ) \
        .withColumn("news_item", explode("key_news")) \
        .select("ticker", "news_item.*") \
        .filter(col("news_item").isNotNull())

    # Save to news S3 silver path as Parquet
    df_silver_news.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.SILVER_NEWS_PATH)

    print(f"Done processing news data bronze to silver transformation")


@task.pyspark(conn_id="spark_default")
def transform_market_and_risk_data(spark: SparkSession, sc: SparkContext):
    """
    Reads market data, market metrics dan risk-free rate data
    (historical and daily data), clean them and create new
    financial metrics and ratios features
    """
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts market data and risk bronze to silver transformation")

    # Read historical and daily (routine) market data, then merge them
    df_market_data_historical = spark.read \
        .option("header", "true") \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/historical_data.csv") \
        .withColumnRenamed("Date", "date") \
        .withColumn("date", to_date("date")) \
        .withColumnRenamed("Close", "date") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("Volume", "volume") \

    df_market_data_routine = spark.read \
        .option("header", "true") \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/year=*/") \
        .drop("year", "month", "day") \
        .withColumnRenamed("Date", "date") \
        .withColumn("date", to_date("date")) \
        .withColumnRenamed("Close", "date") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("Volume", "volume") \
    
    df_market_data = df_market_data_historical \
        .unionByName(df_market_data_routine, allowMissingColumns=True) \
        .dropDuplicates(["ticker", "date"])
    
    # Market data feature engineering windows
    sma_20_window  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-19, 0)
    sma_50_window  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-49, 0)
    sma_100_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-99, 0)
    sma_200_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-199, 0)
    lag_window = Window.partitionBy("ticker").orderBy("date")

    # Simple Moving Averages (SMA)
    df_market_data_sma = df_market_data \
        .withColumn("sma_20", avg("close").over(sma_20_window)) \
        .withColumn("sma_50", avg("close").over(sma_50_window)) \
        .withColumn("sma_100", avg("close").over(sma_100_window)) \
        .withColumn("sma_200", avg("close").over(sma_200_window)) \
    
    # Periodic returns (1 day, 1 week, 1 month, 3 months, 6 months, 12 months)
    df_market_data_returns = df_market_data_sma \
        .withColumn("price_1d_ago", lag(col("close"), 1).over(lag_window)) \
        .withColumn("price_1w_ago", lag(col("close"), 5).over(lag_window)) \
        .withColumn("price_1m_ago", lag(col("close"), 21).over(lag_window)) \
        .withColumn("price_3m_ago", lag(col("close"), 63).over(lag_window)) \
        .withColumn("price_6m_ago", lag(col("close"), 126).over(lag_window)) \
        .withColumn("price_12m_ago", lag(col("close"), 252).over(lag_window)) \
        .withColumn("1d_return", (col("close") - col("price_1d_ago")) / col("price_1d_ago")) \
        .withColumn("1w_return", (col("close") - col("price_1w_ago")) / col("price_1w_ago")) \
        .withColumn("1m_return", (col("close") - col("price_1m_ago")) / col("price_1m_ago")) \
        .withColumn("3m_return", (col("close") - col("price_3m_ago")) / col("price_3m_ago")) \
        .withColumn("6m_return", (col("close") - col("price_6m_ago")) / col("price_6m_ago")) \
        .withColumn("12m_return", (col("close") - col("price_12m_ago")) / col("price_12m_ago")) \
        .drop("price_1d_ago", "price_1w_ago", "price_1m_ago", "price_3m_ago", "price_6m_ago", "price_12m_ago")
    
    # Sharpe Ratio
    ...
    
    # Value at Risk (VaR)
    ...

    # Beta 3Y
    ...

    df_market_data_final = ...

    # Read market metrics data
    df_market_metrics = spark.read.json(configuration.BRONZE_MARKET_METRICS_PATH) \
        .withColumns("date", to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-MM-dd")
        ).drop("year", "month", "day").dropDuplicates(["ticker", "date"])

    # Read risk-free rate data
    df_rff = spark.read.json(configuration.BRONZE_RFF_PATH) \
        .withColumns("date", to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-MM-dd")
        ).drop("year", "month", "day").dropDuplicates(["date"])

    # Combine all data and front fill
    df_market_combined = df_market_data_final \
        .join(df_market_metrics, on=["ticker", "date"], how="left") \
        .join(df_rff, on="date", how="left")

    cols_to_ffill = [
        "ev_ebitda",
        "book_value",
        "earnings",
        "dividend_yield",
        "payout_ratio",
        "target_mean_price",
        "recommendation_mean",
        "market_cap",
        "shares_outstanding",
        "free_float",
        "fifty_two_week_low",
        "fifty_two_week_high",
        "risk-free-rate"
    ]

    ffill_window = Window.partitionBy("ticker") \
        .orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    ffill_expression = {
        c: last(col(c), True).over(ffill_window) for c in cols_to_ffill
    }

    df_market_and_risk = df_market_combined \
        .withColumn(ffill_expression)
    
    # PBV, EPS, PER, free float decimal
    df_silver_market_and_risk = df_market_and_risk \
        .withColumn("PBV", col("close") / ("book_value")) \
        .withColumn("EPS", col("earnings") / ("shares_outstanding")) \
        .withColumn("PBV", col("close") / ("EPS")) \
        .withColumn("free_float", col("free_float") / col("shares_oustanding"))

    # Write combined market and risk data to S3 silver path as Parquet
    df_silver_market_and_risk.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH)

    print(f"Done processing market and risk data bronze to silver transformation")