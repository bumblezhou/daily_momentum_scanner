# import requests

# symbol = "TSM"
# exchange = "NYSE"
# url = f"https://scanner.tradingview.com/america/scan"

# payload = {
#     "symbols": {
#         "tickers": [f"{exchange}:{symbol}"],
#         "query": {"types": []}
#     },
#     "columns": [
#         "RSI",  # 相对强弱指数
#         "Recommend.All",  # 综合买卖建议
#         "MACD.macd",
#         "ADX",
#         "Stoch.K",
#         "CCI20"
#     ]
# }

# response = requests.post(url, json=payload)
# data = response.json()

# # 提取 RSI 值
# rsi_value = data["data"][0]["d"][0]
# print("RSI(14):", rsi_value)


import yfinance as yf
import pandas as pd
import numpy as np

def calc_rs_rating(ticker="TSM", benchmark="SPY", period="1y"):
    # 下载过去一年的数据
    df_stock = yf.download(ticker, period=period)
    df_bench = yf.download(benchmark, period=period)
    
    # 计算年涨幅（以收盘价为准）
    stock_return = df_stock['Close'].iloc[-1] / df_stock['Close'].iloc[0] - 1
    bench_return = df_bench['Close'].iloc[-1] / df_bench['Close'].iloc[0] - 1

    # 相对强度比率（>1 表示强于大盘）
    rs_ratio = (1 + stock_return) / (1 + bench_return)

    # 映射为 IBD 风格的 1–99 分值
    # rs_rating = 50 + (rs_ratio - 1) * 200  (粗略映射)
    # 限制在 1-99 范围
    rs_rating = np.clip(int(50 + (rs_ratio - 1) * 200), 1, 99)
    
    return {
        "Stock": ticker,
        "Stock_Return": round(stock_return * 100, 2),
        "Benchmark_Return": round(bench_return * 100, 2),
        "RS_Ratio": round(rs_ratio, 3),
        "RS_Rating": rs_rating
    }

result = calc_rs_rating("TSM", "SPY")
print(result)