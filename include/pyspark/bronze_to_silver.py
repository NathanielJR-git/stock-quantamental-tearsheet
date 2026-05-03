import os
import math
from airflow.sdk import task
from include.configuration import configuration
from include.llm.pick_and_rate_news import extract_top_news_udf
from include.pyspark.utils import apply_s3_config
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, ArrayType
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F


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
        F.col("ticker"),
        F.col("company_name"),
        F.lower(F.col("sector")).alias("sector"),
        F.lower(F.col("industry")).alias("industry")
    )

    # Save to company profile S3 silver path as Parquet
    df_silver_profiles.write \
        .mode("overwrite") \
        .parquet(configuration.SILVER_COMPANY_PROFILES_PATH)

    print(f"Done processing company profile bronze to silver transformation")


@task.pyspark(conn_id="spark_default")
def transform_news_data(spark: SparkSession, sc: SparkContext):
    """
    Call Groq API for the latest 20 news form each stocks
    to get the top 3 most important news and each sentiment scores
    using Pandas UDF and Groq API's Llama 3.3 versatile model
    """
    # Inject Groq API Key to environment for UDF execution
    groq_key = os.getenv("GROQ_API_KEY")
    if groq_key:
        os.environ["GROQ_API_KEY"] = groq_key
        sc.setLocalProperty("spark.executorEnv.GROQ_API_KEY", groq_key)
    
    # Apply Hadoop S3 connection configurations
    apply_s3_config(sc)
    print("Starts news data bronze to silver transformation")
    
    # Read gnews JSON files from S3 bronze news data
    df_news = spark.read.json(configuration.BRONZE_NEWS_PATH)

    # Cast date to timestamp and deduplicate  data
    df_news_cleaned = df_news \
        .withColumn("publication_date", F.to_timestamp(F.col("publication_date"), "EEE, dd MMM yyyy HH:mm:ss z")) \
        .dropDuplicates()
    
    window_spec = Window.partitionBy("ticker").orderBy(F.col("publication_date").desc())
    df_news_top_20 = df_news_cleaned \
        .withColumn("news_rank", F.row_number().over(window_spec)) \
        .filter(F.col("news_rank") <= 20) \
        .drop("news_rank")

    # Call LLM UDF to extract sentiment and to news
    df_llm_ready = df_news_top_20 \
        .groupBy("ticker") \
        .agg(
            F.to_json(F.collect_list(
                F.struct(
                    F.col("publication_date"),
                    F.col("title"),
                    F.col("summary")
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
        extract_top_news_udf(F.col("news_context_json"))
    )

    # Parse raw LLM JSON responses 
    df_parsed = df_raw_llm.withColumn(
        "extracted_data",
        F.from_json(F.col("llm_raw_response"), llm_output_schema)
    )

    # Select news item (title, summary, and )
    df_extracted_news = df_parsed \
        .select(
            F.col("ticker"),
            F.col("extracted_data.extracted_news").alias("key_news")
        ) \
        .withColumn("news_item", F.explode("key_news")) \
        .filter(F.col("news_item").isNotNull()) \
        .select("ticker", "news_item.*")
    
    # Join df extracted news with top 20 news dataframe to extract publication date
    df_silver_news = df_extracted_news.join(
        df_news_top_20.select("ticker", "title", "publication_date"),
        on=["ticker", "title"],
        how="left"
    ).withColumn("date", F.to_date("publication_date"))

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

    # Read historical market data
    df_market_data_historical = spark.read \
        .option("header", "true") \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/historical_data.csv") \
        .withColumn("date", F.to_date("Date")) \
        .withColumn("close", F.col("Close").cast("float")) \
        .withColumn("high", F.col("High").cast("float")) \
        .withColumn("low", F.col("Low").cast("float")) \
        .withColumn("open", F.col("Open").cast("float")) \
        .withColumn("volume", F.col("Volume").cast("long")) \
        .drop("Date", "Close", "High", "Low", "Open", "Volume")

    # Read daily (routine) market data
    df_market_data_routine = spark.read \
        .option("header", "true") \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/year=*/") \
        .withColumn("date", F.to_date("Date")) \
        .withColumn("close", F.col("Close").cast("float")) \
        .withColumn("high", F.col("High").cast("float")) \
        .withColumn("low", F.col("Low").cast("float")) \
        .withColumn("open", F.col("Open").cast("float")) \
        .withColumn("volume", F.col("Volume").cast("integer")) \
        .drop("Date", "Close", "High", "Low", "Open", "Volume", "year", "month", "day")
    
    # Merge historical and daily market data
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
        .withColumn("sma_20", F.avg("close").over(sma_20_window)) \
        .withColumn("sma_50", F.avg("close").over(sma_50_window)) \
        .withColumn("sma_100", F.avg("close").over(sma_100_window)) \
        .withColumn("sma_200", F.avg("close").over(sma_200_window)) \
    
    # Periodic returns (1 day, 1 week, 1 month, 3 months, 6 months, 12 months)
    df_market_data_returns = df_market_data_sma \
        .withColumn("price_1d_ago", F.lag(F.col("close"), 1).over(lag_window)) \
        .withColumn("price_1w_ago", F.lag(F.col("close"), 5).over(lag_window)) \
        .withColumn("price_1m_ago", F.lag(F.col("close"), 21).over(lag_window)) \
        .withColumn("price_3m_ago", F.lag(F.col("close"), 63).over(lag_window)) \
        .withColumn("price_6m_ago", F.lag(F.col("close"), 126).over(lag_window)) \
        .withColumn("price_12m_ago", F.lag(F.col("close"), 252).over(lag_window)) \
        .withColumn("1d_return", (F.col("close") - F.col("price_1d_ago")) / F.col("price_1d_ago")) \
        .withColumn("1w_return", (F.col("close") - F.col("price_1w_ago")) / F.col("price_1w_ago")) \
        .withColumn("1m_return", (F.col("close") - F.col("price_1m_ago")) / F.col("price_1m_ago")) \
        .withColumn("3m_return", (F.col("close") - F.col("price_3m_ago")) / F.col("price_3m_ago")) \
        .withColumn("6m_return", (F.col("close") - F.col("price_6m_ago")) / F.col("price_6m_ago")) \
        .withColumn("12m_return", (F.col("close") - F.col("price_12m_ago")) / F.col("price_12m_ago")) \
        .drop("price_1d_ago", "price_1w_ago", "price_1m_ago", "price_3m_ago", "price_6m_ago", "price_12m_ago")
    
    # Sharpe Ratio
    risk_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-251, 0)

    df_market_data_risk = df_market_data_returns \
        .withColumn("stddev_1d_return", F.stddev("1d_return").over(risk_window)) \
        .withColumn("mean_1d_return", F.avg("1d_return").over(risk_window)) \
        .withColumn(
            "sharpe_ratio_252d",
            F.when(
                F.col("stddev_1d_return") > 0, 
                (F.col("mean_1d_return") / F.col("stddev_1d_return")) * math.sqrt(252)
            ).otherwise(None)
        )

    # Value at Risk (VaR) 95%
    df_market_data_final = df_market_data_risk \
        .withColumn("returns_array", F.collect_list("1d_return").over(risk_window)) \
        .withColumn("sorted_returns", F.array_sort("returns_array")) \
        .withColumn("array_length", F.size("sorted_returns")) \
        .withColumn(
            # Calculate 5% based off array length
            "var_index", 
            F.expr("cast(ceil(array_length * 0.05) as int)") 
        ) \
        .withColumn(
            # Extract return values
            "var_95_252d", 
            F.expr("element_at(sorted_returns, var_index)")
        ) \
        .drop(
            "stddev_1d_return", "mean_1d_return", 
            "returns_array", "sorted_returns", "array_length", "var_index"
        )

    # TODO: Beta 3Y
    ...

    # Read market metrics data
    df_market_metrics = spark.read.json(configuration.BRONZE_MARKET_METRICS_PATH) \
        .withColumn("date", F.make_date(F.col("year"), F.col("month"), F.col("day"))) \
        .withColumns({
            "ev_ebitda": F.col("ev_ebitda").cast("double"),
            "book_value": F.col("book_value").cast("double"),
            "dividend_yield": F.col("dividend_yield").cast("double"),
            "payout_ratio": F.col("payout_ratio").cast("double"),
            "target_mean_price": F.col("target_mean_price").cast("double"),
            "recommendation_mean": F.col("recommendation_mean").cast("double"),
            "fifty_two_week_low": F.col("fifty_two_week_low").cast("double"),
            "fifty_two_week_high": F.col("fifty_two_week_high").cast("double"),
            "earnings": F.col("earnings").cast("long"),
            "market_cap": F.col("market_cap").cast("long"),
            "shares_outstanding": F.col("shares_outstanding").cast("long"),
            "free_float": F.col("free_float").cast("long")
        }) \
        .drop("year", "month", "day") \
        .dropDuplicates(["ticker", "date"])

    # Read risk-free rate data
    df_rff = spark.read.json(configuration.BRONZE_RFF_PATH) \
        .withColumn("date", F.make_date(F.col("year"), F.col("month"), F.col("day"))) \
        .withColumn("risk-free-rate", F.col("risk-free-rate").cast("float")) \
        .drop("year", "month", "day") \
        .dropDuplicates(["date"])

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
        c: F.last(F.col(c), True).over(ffill_window) for c in cols_to_ffill
    }

    df_market_and_risk = df_market_combined \
        .withColumns(ffill_expression)
    
    # PBV, EPS, PER, free float decimal
    df_silver_market_and_risk = df_market_and_risk \
        .withColumn("PBV", F.col("close") / ("book_value")) \
        .withColumn("EPS", F.col("earnings") / ("shares_outstanding")) \
        .withColumn("PER", F.col("close") / ("EPS")) \
        .withColumn("free_float", F.col("free_float") / F.col("shares_outstanding"))

    # Write combined market and risk data to S3 silver path as Parquet
    df_silver_market_and_risk.write \
        .mode("overwrite") \
        .partitionBy("ticker", "date") \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH)

    print(f"Done processing market and risk data bronze to silver transformation")