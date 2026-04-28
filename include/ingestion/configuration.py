class Configuration:
    def __init__(self):
        self.TICKERS = [
            "BBCA.JK",
            "BMRI.JK",
            "BBRI.JK"
        ]

        self.BUCKET_NAME = ""
        
        self.RISK_FREE_RATE_URL = "https://tradingeconomics.com/indonesia/government-bond-yield"

configuration = Configuration()