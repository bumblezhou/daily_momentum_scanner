import yfinance as yf
import pandas as pd
import numpy as np
import requests

PROXIES = {
    "http": "http://127.0.0.1:8118",
    "https": "http://127.0.0.1:8118",
}


yf.set_config(proxy="http://127.0.0.1:8118")

# =========================
# 财务过滤
# =========================
def fundamental_filter(symbol):
    info = yf.Ticker(symbol).info

    eps = info.get("trailingEps")
    roe = info.get("returnOnEquity")

    if eps is None or roe is None:
        return False

    if eps <= 0:
        return False

    if roe < 0.15:
        return False

    return True

fundamental_filter("AAPL")