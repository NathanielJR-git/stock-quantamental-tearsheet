import math
from airflow.sdk import task
from include.configuration import configuration
from include.pyspark.utils import apply_s3_config, create_spark_session
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F


@task
def transform_company_profiles():
    """
    Convert company profiles file format from JSON to Parquet
    """
    spark = create_spark_session("bronze_to_silver")
    sc = spark.sparkContext
    apply_s3_config(sc)
    print("Starts company profiles bronze to silver transformation")

    # Company profiles schema explicit definition
    company_profiles_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("company_name", StringType(), False),
        StructField("sector", StringType(), False),
        StructField("industry", StringType(), False),
    ])

    # Read company profiles data from bronze storage
    df_bronze = spark.read \
        .schema(company_profiles_schema) \
        .json(configuration.BRONZE_COMPANY_PROFILES_PATH)

    # Clean data
    df_silver_profiles = df_bronze.select(
        F.col("ticker"),
        F.col("company_name"),
        F.lower(F.col("sector")).alias("sector"),
        F.lower(F.col("industry")).alias("industry")
    )

    # Save to S3 silver storage
    df_silver_profiles.write \
        .mode("overwrite") \
        .parquet(configuration.SILVER_COMPANY_PROFILES_PATH)

    spark.stop()
    print(f"Done processing company profile bronze to silver transformation")


@task
def transform_news_data():
    """
    Silver news transformation. LLM sentiment path is temporarily disabled —
    we keep the three most recent news items per ticker with neutral sentiment.
    """
    spark = create_spark_session("bronze_to_silver_news")
    sc = spark.sparkContext
    apply_s3_config(sc)
    print("Starts news data bronze to silver transformation")

    # Read news data from bronze storage
    df_news = spark.read.json(configuration.BRONZE_NEWS_PATH)

    # Clean news data
    df_news_cleaned = df_news \
        .withColumn(
            "publication_date",
            F.to_timestamp(F.col("publication_date"), "EEE, dd MMM yyyy HH:mm:ss z")
        ) \
        .dropDuplicates()

    # Select 3 of the latest news data from each ticker (temporary)
    window_spec = Window.partitionBy("ticker").orderBy(F.col("publication_date").desc())
    df_news_top_3 = df_news_cleaned \
        .withColumn("news_rank", F.row_number().over(window_spec)) \
        .filter(F.col("news_rank") <= 3) \
        .drop("news_rank")

    # TEMPORARY: skip Groq LLM — top 3 by publication date, sentiment score = 0.
    # TODO: restore LLM path — aggregate rows into JSON per ticker, call Groq on the
    # driver with call_groq_api, parse with from_json, explode, join back for dates.
    df_silver_news = df_news_top_3.select(
        F.col("ticker"),
        F.col("title"),
        F.lit(0.0).alias("sentiment_score"),
        F.col("summary"),
        F.to_date(F.col("publication_date")).alias("news_date"),
    )

    # Save to S3 silver storage
    df_silver_news.write \
        .mode("overwrite") \
        .partitionBy("ticker") \
        .parquet(configuration.SILVER_NEWS_PATH)

    spark.stop()
    print(f"Done processing news data bronze to silver transformation")


@task
def transform_market_and_risk_data():
    """
    Reads market data, market metrics and risk-free rate data, enriches each
    trading-day row with the most recent weekly snapshot (via ffill + bfill),
    then computes all financial features including a risk-free-rate-adjusted
    Sharpe ratio.
    """
    spark = create_spark_session("bronze_to_silver_market_risk")
    sc = spark.sparkContext
    apply_s3_config(sc)
    print("Starts market data and risk bronze to silver transformation")

    # Raw market data
    df_market_data_historical = spark.read \
        .option("header", "true") \
        .option("basePath", configuration.BRONZE_MARKET_DATA_PATH) \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/historical_data.csv") \
        .select(
            F.col("ticker"),
            F.to_date(F.col("Date")).alias("date"),
            F.col("Close").cast("float").alias("close"),
            F.col("High").cast("float").alias("high"),
            F.col("Low").cast("float").alias("low"),
            F.col("Open").cast("float").alias("open"),
            F.col("Volume").cast("long").alias("volume"),
        )

    df_market_data_routine = spark.read \
        .option("header", "true") \
        .option("basePath", configuration.BRONZE_MARKET_DATA_PATH) \
        .csv(f"{configuration.BRONZE_MARKET_DATA_PATH}/ticker=*/year=*/") \
        .select(
            F.col("ticker"),
            F.to_date(F.col("Date")).alias("date"),
            F.col("Close").cast("float").alias("close"),
            F.col("High").cast("float").alias("high"),
            F.col("Low").cast("float").alias("low"),
            F.col("Open").cast("float").alias("open"),
            F.col("Volume").cast("integer").alias("volume"),
        )

    df_market_data = df_market_data_historical \
        .unionByName(df_market_data_routine, allowMissingColumns=True) \
        .dropDuplicates(["ticker", "date"])

    # Market metrics
    df_market_metrics = spark.read.json(configuration.BRONZE_MARKET_METRICS_PATH) \
        .withColumn("raw_date", F.make_date(F.col("year"), F.col("month"), F.col("day"))) \
        .withColumn("date",
            F.when(F.dayofweek("raw_date") == 1, F.date_add("raw_date", 1))
             .when(F.dayofweek("raw_date") == 7, F.date_add("raw_date", 2))
             .otherwise(F.col("raw_date"))
        ) \
        .withColumns({
            "ev_ebitda":           F.col("ev_ebitda").cast("double"),
            "book_value":          F.col("book_value").cast("double"),
            "dividend_yield":      F.col("dividend_yield").cast("double"),
            "payout_ratio":        F.col("payout_ratio").cast("double"),
            "target_mean_price":   F.col("target_mean_price").cast("double"),
            "recommendation_mean": F.col("recommendation_mean").cast("double"),
            "fifty_two_week_low":  F.col("fifty_two_week_low").cast("double"),
            "fifty_two_week_high": F.col("fifty_two_week_high").cast("double"),
            "earnings":            F.col("earnings").cast("long"),
            "market_cap":          F.col("market_cap").cast("long"),
            "shares_outstanding":  F.col("shares_outstanding").cast("long"),
            "free_float":          F.col("free_float").cast("long"),
        }) \
        .drop("raw_date", "year", "month", "day") \
        .dropDuplicates(["ticker", "date"])

    # Risk-free rate
    df_rff = spark.read.json(configuration.BRONZE_RFF_PATH) \
        .withColumn("raw_date", F.make_date(F.col("year"), F.col("month"), F.col("day"))) \
        .withColumn("date",
            F.when(F.dayofweek("raw_date") == 1, F.date_add("raw_date", 1))
             .when(F.dayofweek("raw_date") == 7, F.date_add("raw_date", 2))
             .otherwise(F.col("raw_date"))
        ) \
        .withColumn("risk_free_rate", F.col("risk_free_rate").cast("float")) \
        .drop("raw_date", "year", "month", "day") \
        .dropDuplicates(["date"])

    # Merge and fill (metrics and risk-free rate data are ingested weekly)
    df_market_combined = df_market_data \
        .join(df_market_metrics, on=["ticker", "date"], how="left") \
        .join(df_rff, on="date", how="left")

    cols_to_fill = [
        "ev_ebitda", "book_value", "earnings", "dividend_yield", "payout_ratio",
        "target_mean_price", "recommendation_mean", "market_cap", "shares_outstanding",
        "free_float", "fifty_two_week_low", "fifty_two_week_high", "risk_free_rate",
    ]

    ffill_window = Window.partitionBy("ticker").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    bfill_window = Window.partitionBy("ticker").orderBy("date") \
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)

    df_ffilled = df_market_combined.withColumns({
        c: F.last(F.col(c), True).over(ffill_window) for c in cols_to_fill
    })
    df_enriched = df_ffilled.withColumns({
        c: F.coalesce(F.col(c), F.first(F.col(c), True).over(bfill_window))
        for c in cols_to_fill
    })

    # Feature Engineering
    sma_20_window  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-19,  0)
    sma_50_window  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-49,  0)
    sma_100_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-99,  0)
    sma_200_window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-199, 0)
    lag_window     = Window.partitionBy("ticker").orderBy("date")
    risk_window    = Window.partitionBy("ticker").orderBy("date").rowsBetween(-251, 0)

    # Simple Moving Averages
    df_sma = df_enriched \
        .withColumn("sma_20",  F.avg("close").over(sma_20_window)) \
        .withColumn("sma_50",  F.avg("close").over(sma_50_window)) \
        .withColumn("sma_100", F.avg("close").over(sma_100_window)) \
        .withColumn("sma_200", F.avg("close").over(sma_200_window))

    # Periodic returns
    df_returns = df_sma \
        .withColumn("price_1d_ago",  F.lag("close",   1).over(lag_window)) \
        .withColumn("price_1w_ago",  F.lag("close",   5).over(lag_window)) \
        .withColumn("price_1m_ago",  F.lag("close",  21).over(lag_window)) \
        .withColumn("price_3m_ago",  F.lag("close",  63).over(lag_window)) \
        .withColumn("price_6m_ago",  F.lag("close", 126).over(lag_window)) \
        .withColumn("price_12m_ago", F.lag("close", 252).over(lag_window)) \
        .withColumn("1d_return",  (F.col("close") - F.col("price_1d_ago"))  / F.col("price_1d_ago")) \
        .withColumn("1w_return",  (F.col("close") - F.col("price_1w_ago"))  / F.col("price_1w_ago")) \
        .withColumn("1m_return",  (F.col("close") - F.col("price_1m_ago"))  / F.col("price_1m_ago")) \
        .withColumn("3m_return",  (F.col("close") - F.col("price_3m_ago"))  / F.col("price_3m_ago")) \
        .withColumn("6m_return",  (F.col("close") - F.col("price_6m_ago"))  / F.col("price_6m_ago")) \
        .withColumn("12m_return", (F.col("close") - F.col("price_12m_ago")) / F.col("price_12m_ago")) \
        .drop("price_1d_ago", "price_1w_ago", "price_1m_ago",
              "price_3m_ago", "price_6m_ago", "price_12m_ago")

    # Sharpe ratio
    df_risk = df_returns \
        .withColumn("stddev_1d_return", F.stddev("1d_return").over(risk_window)) \
        .withColumn("mean_1d_return",   F.avg("1d_return").over(risk_window)) \
        .withColumn(
            "sharpe_ratio_252d",
            F.when(
                F.col("stddev_1d_return") > 0,
                (
                    (F.col("mean_1d_return") - F.coalesce(F.col("risk_free_rate"), F.lit(0.0)) / 252)
                    / F.col("stddev_1d_return")
                ) * math.sqrt(252)
            ).otherwise(None)
        )

    # Value at Risk 95%
    df_market_data_final = df_risk \
        .withColumn("returns_array", F.collect_list("1d_return").over(risk_window)) \
        .withColumn("sorted_returns", F.array_sort("returns_array")) \
        .withColumn("array_length",   F.size("sorted_returns")) \
        .withColumn("var_index",      F.expr("greatest(cast(ceil(array_length * 0.05) as int), 1)")) \
        .withColumn("var_95_252d",    F.expr("element_at(sorted_returns, var_index)")) \
        .drop("stddev_1d_return", "mean_1d_return",
              "returns_array", "sorted_returns", "array_length", "var_index")

    # TODO: Beta 3Y
    ...

    # Derived financial ratios
    df_silver_market_and_risk = df_market_data_final \
        .withColumn("PBV",
            F.col("close") / F.when(
                F.col("book_value").isNotNull() & (F.col("book_value") != 0),
                F.col("book_value"))) \
        .withColumn("EPS",
            F.col("earnings").cast("double") / F.when(
                F.col("shares_outstanding").isNotNull() & (F.col("shares_outstanding") != 0),
                F.col("shares_outstanding").cast("double"))) \
        .withColumn("PER",
            F.col("close") / F.when(
                F.col("EPS").isNotNull() & (F.col("EPS") != 0),
                F.col("EPS"))) \
        .withColumn("free_float",
            F.col("free_float").cast("double") / F.when(
                F.col("shares_outstanding").isNotNull() & (F.col("shares_outstanding") != 0),
                F.col("shares_outstanding").cast("double")))

    # Save 
    df_silver_market_and_risk.write \
        .mode("overwrite") \
        .partitionBy("ticker") \
        .parquet(configuration.SILVER_MARKET_AND_RISK_PATH)

    spark.stop()
    print(f"Done processing market and risk data bronze to silver transformation")
