class Configuration:
    def __init__(self):
        # Selected stock tickers (IDX)
        self.TICKERS = [
            "BBCA.JK", 
            "BBRI.JK", 
            "BMRI.JK", 
            "PTRO.JK", 
            "RAJA.JK", 
            "WIFI.JK", 
            "ADMR.JK", 
            "ARCI.JK", 
            "BULL.JK", 
            "MBSS.JK", 
            "DEWA.JK", 
            "BUMI.JK"
        ]

        # Company profiles static data for initial loading
        self.STATIC_COMPANY_PROFILES = [
            {"ticker": "BBCA.JK", "company_name": "PT Bank Central Asia Tbk", "sector": "Financials", "industry": "Banks - Regional"},
            {"ticker": "BBRI.JK", "company_name": "PT Bank Rakyat Indonesia (Persero) Tbk", "sector": "Financials", "industry": "Banks - Regional"},
            {"ticker": "BMRI.JK", "company_name": "PT Bank Mandiri (Persero) Tbk", "sector": "Financials", "industry": "Banks - Regional"},
            {"ticker": "PTRO.JK", "company_name": "PT Petrosea Tbk", "sector": "Energy", "industry": "Oil & Gas Equipment & Services"},
            {"ticker": "RAJA.JK", "company_name": "PT Rukun Raharja Tbk", "sector": "Energy", "industry": "Oil & Gas Midstream"},
            {"ticker": "WIFI.JK", "company_name": "PT Solusi Sinergi Digital Tbk", "sector": "Technology", "industry": "Information Technology Services"},
            {"ticker": "ADMR.JK", "company_name": "PT Adaro Minerals Indonesia Tbk", "sector": "Basic Materials", "industry": "Coking Coal"},
            {"ticker": "ARCI.JK", "company_name": "PT Archi Indonesia Tbk", "sector": "Basic Materials", "industry": "Gold"},
            {"ticker": "BULL.JK", "company_name": "PT Buana Lintas Lautan Tbk", "sector": "Industrials", "industry": "Marine Shipping"},
            {"ticker": "MBSS.JK", "company_name": "PT Mitrabahtera Segara Sejati Tbk", "sector": "Industrials", "industry": "Marine Shipping"},
            {"ticker": "DEWA.JK", "company_name": "PT Darma Henwa Tbk", "sector": "Energy", "industry": "Thermal Coal"},
            {"ticker": "BUMI.JK", "company_name": "PT Bumi Resources Tbk", "sector": "Energy", "industry": "Thermal Coal"}
        ]

        # S3 bucket name
        self.BUCKET_NAME = "nathaniel-tearsheet-datalake-413539128030-ap-southeast-3-an"
        
        # Investing.com URL for fetching risk-free rate data
        self.RISK_FREE_RATE_URL = "https://tradingeconomics.com/indonesia/government-bond-yield"
        
        # Airflow DAG staring date
        self.START_YEAR = 2026
        self.START_MONTH = 4
        self.START_DAY = 29

        # Company profile S3 paths
        self.BRONZE_COMPANY_PROFILES_PATH = f"s3a://{self.BUCKET_NAME}/bronze/company_profiles/profiles.json"
        self.SILVER_COMPANY_PROFILES_PATH = f"s3a://{self.BUCKET_NAME}/silver/company_profiles/"

        # News S3 paths
        self.BRONZE_NEWS_PATH = f"s3a://{self.BUCKET_NAME}/bronze/news_data/"
        self.SILVER_NEWS_PATH = f"s3a://{self.BUCKET_NAME}/silver/news_data/"

        # Market dan risk data S3 paths
        self.BRONZE_MARKET_DATA_PATH = f"s3a://{self.BUCKET_NAME}/bronze/market_data/"
        self.BRONZE_MARKET_METRICS_PATH = f"s3a://{self.BUCKET_NAME}/bronze/market_metrics/"
        self.BRONZE_RFF_PATH = f"s3a://{self.BUCKET_NAME}/bronze/risk_free_rate/"
        self.SILVER_MARKET_AND_RISK_PATH = f"s3a://{self.BUCKET_NAME}/silver/market_and_risk/"

        # Gold S3 paths
        self.GOLD_MARKET_DATA_PATH = f"s3a://{self.BUCKET_NAME}/gold/market_data/"
        self.GOLD_STOCK_TEARSHEET = f"s3a://{self.BUCKET_NAME}/gold/stock_tearsheet/"

configuration = Configuration()