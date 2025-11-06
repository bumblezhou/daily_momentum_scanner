# build_universe.py
# 自动构建 UNIVERSE：从 S&P500、DJIA、Nasdaq-100 中选 市值前150 + 10日均量前150 的股票
# 已切换到 Finnhub API，避免 yfinance 限流错误

import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# ==================== 配置 ====================
REQUEST_DELAY = 0.1  # Finnhub 限流：60 calls/min，增加延迟
MAX_RETRIES = 3
PROXIES = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118',
}  # 如无需代理，设为 None
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}
FINNHUB_TOKEN = "d40ckf9r01qqo3qha4bgd40ckf9r01qqo3qha4c0"  # 从示例中提取
# ===============================================

# ==================== 代理版 read_html ====================
def read_html_with_proxy(url, proxies=None, headers=None, **kwargs):
    session = requests.Session()
    if proxies:
        session.proxies.update(proxies)
    if headers:
        session.headers.update(headers)
    response = session.get(url, timeout=30, verify=False)
    response.raise_for_status()
    return pd.read_html(response.text, **kwargs)

# ==================== 获取指数成分股 ====================
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        tables = read_html_with_proxy(url, proxies=PROXIES, headers=HEADERS)
        df = tables[0]
        return df['Symbol'].str.replace('.', '-', regex=False).tolist()
    except Exception as e:
        print(f"获取 S&P 500 失败: {e}")
        return []

def get_djia_tickers():
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    try:
        tables = read_html_with_proxy(url, proxies=PROXIES, headers=HEADERS)
        df = tables[2]
        return df['Symbol'].str.replace('.', '-', regex=False).tolist()
    except Exception as e:
        print(f"获取 DJIA 失败: {e}")
        return []

def get_nasdaq100_tickers():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    try:
        tables = read_html_with_proxy(url, proxies=PROXIES, headers=HEADERS)
        df = tables[4] if len(tables) > 4 else tables[3]
        return df['Ticker'].str.replace('.', '-', regex=False).tolist()
    except Exception as e:
        print(f"获取 Nasdaq-100 失败: {e}")
        return []

# ==================== 使用 Finnhub API 获取股票信息 ====================
def get_stock_info(ticker):
    for attempt in range(MAX_RETRIES):
        try:
            print(f"   [{ticker}] 尝试 {attempt+1}/{MAX_RETRIES}...", end="")
            
            # Finnhub API URL
            url = f"https://finnhub.io/api/v1/stock/metric?symbol={ticker}&metric=all&token={FINNHUB_TOKEN}"
            
            session = requests.Session()
            if PROXIES:
                session.proxies.update(PROXIES)
            session.headers.update(HEADERS)
            
            response = session.get(url, timeout=30, verify=False)
            response.raise_for_status()
            data = response.json()
            
            if 'metric' not in data:
                print("API 无数据")
                return None
            
            metric = data['metric']
            market_cap = metric.get('marketCapitalization', 0) or 0
            avg_volume_10d = metric.get('10DayAverageTradingVolume', 0) or 0

            if market_cap > 0 and avg_volume_10d > 0:
                print("成功")
                return {
                    'Ticker': ticker,
                    'MarketCap': market_cap,
                    'AvgVolume10D': avg_volume_10d,
                    'Name': ticker,  # Finnhub 无直接名称，简化
                    'Sector': 'Unknown'  # Finnhub 无直接 sector，简化
                }
            else:
                print("数据不足")
                return None

        except Exception as e:
            print(f"错误: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)  # Finnhub 限流，增加重试延迟
            else:
                print("跳过")
                return None

    return None

# ==================== 主程序 ====================
if __name__ == "__main__":
    print("开始构建 UNIVERSE：从 S&P500、DJIA、Nasdaq-100 中筛选高市值+高流动性股票（Finnhub API）")

    # 1. 获取三大指数成分股
    print("\n1. 获取指数成分股...")
    sp500 = get_sp500_tickers()
    djia = get_djia_tickers()
    nasdaq100 = get_nasdaq100_tickers()

    all_tickers = sorted(set(sp500 + djia + nasdaq100))
    print(f"   共 {len(all_tickers)} 只股票（去重后）")

    # 2. 获取每只股票的市值和10日均量
    print("\n2. 获取市值与10日均量（Finnhub API，可能需要几分钟）...")
    results = []
    for i, ticker in enumerate(all_tickers, 1):
        print(f"   [{i:3d}/{len(all_tickers)}] {ticker}...", end="")
        info = get_stock_info(ticker)
        if info:
            results.append(info)
        time.sleep(REQUEST_DELAY)

    if not results:
        print("无有效数据，退出")
        exit()

    df = pd.DataFrame(results)

    # 3. 排名：市值前150 + 10日均量前150
    print("\n3. 排名筛选...")
    df['MarketCap_Rank'] = df['MarketCap'].rank(ascending=False)
    df['Volume_Rank'] = df['AvgVolume10D'].rank(ascending=False)

    top_market_cap = df[df['MarketCap_Rank'] <= 150].copy()
    top_volume = df[df['Volume_Rank'] <= 150].copy()

    # 合并并去重
    universe_df = pd.concat([top_market_cap, top_volume]).drop_duplicates(subset='Ticker')
    universe_df = universe_df.sort_values(['MarketCap_Rank', 'Volume_Rank']).reset_index(drop=True)

    final_tickers = universe_df['Ticker'].tolist()

    # 4. 输出结果
    print("\n" + "="*80)
    print(f"最终 UNIVERSE：{len(final_tickers)} 只股票（市值前150 + 10日均量前150）")
    print("="*80)

    # 格式化输出 Python 列表
    print("UNIVERSE = [")
    for i in range(0, len(final_tickers), 10):
        batch = final_tickers[i:i+10]
        print("    " + ", ".join([f"'{t}'" for t in batch]) + ("," if i < len(final_tickers)-10 else ""))
    print("]")

    # 保存 CSV
    save_cols = ['Ticker', 'Name', 'MarketCap', 'AvgVolume10D', 'MarketCap_Rank', 'Volume_Rank']
    universe_df[save_cols].to_csv(f"UNIVERSE_{datetime.now():%Y%m%d}.csv", index=False)
    print(f"\n详细数据已保存：UNIVERSE_{datetime.now():%Y%m%d}.csv")

    print("\n可复制上方 UNIVERSE 列表到你的选股脚本中使用！")