import pandas as pd

__all__ = ["df_constant_tickers"]

constant_tickers = [
    {"symbol": "ETH-USD", "name": "Ethereum USD", "country": None, "indices": None},
    {"symbol": "BTC-USD", "name": "Bitcoin USD", "country": None, "indices": None},
    {"symbol": "^GSPC", "name": "S&P 500 USD", "country": None, "indices":  None},
    {"symbol": "FEZ", "name": "SPDR EURO STOXX 50 ETF USD", "country": None, "indices": None},
    {"symbol": "GC=F", "name": "Gold USD", "country": None, "indices": None},
    {"symbol": "SI=F", "name": "Silver USD", "country": None, "indices": None},
    {"symbol": "EURUSD=X", "name": "EUR USD", "country": None, "indices": None},
    {"symbol": "NOKUSD=X", "name": "Norwegian Crowns USD", "country": None, "indices": None},
    {"symbol": "SEKUSD=X", "name": "Scwedish Crowns USD", "country": None, "indices": None}
]

df_constant_tickers = pd.DataFrame(constant_tickers)
