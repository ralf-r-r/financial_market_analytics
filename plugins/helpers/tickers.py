import pandas as pd

__all__ = ["df_constant_tickers"]

constant_tickers = [
    {"ticker": "ETH-USD", "name": "Ethereum USD", "market_index": None},
    {"ticker": "BTC-USD", "name": "Bitcoin USD", "market_index": None},
    {"ticker": "^GSPC", "name": "S&P 500 USD", "market_index": None},
    {"ticker": "FEZ", "name": "SPDR EURO STOXX 50 ETF USD", "market_index": None},
    {"ticker": "GC=F", "name": "Gold USD", "market_index": None},
    {"ticker": "SI=F", "name": "Silver USD", "market_index": None},
    {"ticker": "EURUSD=X", "name": "EUR USD", "market_index": None},
    {"ticker": "NOKUSD=X", "name": "Norwegian Crowns USD", "market_index": None},
    {"ticker": "SEKUSD=X", "name": "Scwedish Crowns USD", "market_index": None}
]

df_constant_tickers = pd.DataFrame(constant_tickers)
