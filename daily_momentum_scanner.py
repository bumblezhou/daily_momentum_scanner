# daily_momentum_scanner_custom_rs.py
# 使用 Yahoo Finance + yfinance 计算 RSI 和 自定义 RS Rating（百分位）
# 完全移除 TradingView 依赖

import yfinance as yf
import pandas as pd
import numpy as np
import time
import random
import os
from datetime import datetime
import warnings
import ssl
import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# ==================== 配置区 ====================
PROXY = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118',
}

UNIVERSE = [
    'MSFT','NVDA','INTC','ORCL','QQQ','IBM','AMZN','META',
    'AVGO','PLTR','JPM','NFLX','AAPL','SMCI','IBKR','ABNB',
    'GOOG','V','TSLA','TXN','WFC','BAC','MU','SCHD','TM',
    'MA','AMAT','COST','AMD','LLY','TSM','XOM','BRK.B','T',
    'WMT','PDD','F','BABA','QCOM','PG','JNJ','HD','ABBV','KO',
    'UNH','PM','TMUS','CSCO','BA','GE','DIS','PFE','MRK','CVX',
    'COP','PYPL','NVO','RDDT','CNC','PEP','GOOGL','LEN','DPZ',
    'AXP','ASML','ARM','CRM','ADBE','LAC','NKE','XLI','XLU',
    'GM','GS','MS'
]

TOP_N = 5
CACHE_FILE = "yahoo_custom_cache.pkl"
REQUEST_DELAY = (1.5, 3.0)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# S&P 500 作为基准
SP500_TICKER = "^GSPC"
# ===============================================

# ==================== 缓存系统 ====================
def load_cache():
    return pd.read_pickle(CACHE_FILE) if os.path.exists(CACHE_FILE) else {}

def save_cache(cache):
    pd.to_pickle(cache, CACHE_FILE)

cache = load_cache()

# ==================== TLS + 代理补丁 ====================
original_create_connection = ssl.create_default_context
def patched_create_connection(*args, **kwargs):
    ctx = original_create_connection(*args, **kwargs)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.set_ciphers('DEFAULT:@SECLEVEL=1')
    except: pass
    return ctx
ssl.create_default_context = patched_create_connection

original_get = requests.get
def patched_get(*args, **kwargs):
    kwargs.setdefault('proxies', PROXY)
    kwargs.setdefault('headers', {})
    kwargs['headers'].update(HEADERS)
    kwargs.setdefault('timeout', 20)
    kwargs['verify'] = False
    return original_get(*args, **kwargs)
requests.get = patched_get

# ==================== 辅助函数 ====================
def ensure_series(obj):
    if isinstance(obj, pd.DataFrame): return obj.iloc[:, -1]
    if isinstance(obj, pd.Series): return obj
    return pd.Series([obj])

def last_scalar_from(obj):
    if obj is None: return None
    if isinstance(obj, pd.DataFrame): val = obj.iloc[-1, -1]
    elif isinstance(obj, pd.Series): val = obj.iloc[-1]
    else: val = obj
    if isinstance(val, pd.Series): val = val.iloc[0]
    try: return float(val)
    except: return None

# ==================== Yahoo 下载 ====================
def smart_download(ticker):
    key = f"{ticker}_1y"
    if key in cache:
        df = cache[key]
        if isinstance(df, pd.DataFrame) and len(df) >= 200:
            return df
        cache.pop(key, None)

    for attempt in range(3):
        try:
            print(f"  [Yahoo] {ticker} (attempt {attempt+1})...", end="")
            df = yf.download(ticker, period="1y", interval="1d", proxy=PROXY.get('http'), progress=False)
            if isinstance(df, pd.DataFrame) and not df.empty and len(df) >= 200:
                needed = ['Open', 'High', 'Low', 'Close', 'Volume']
                exist = [c for c in needed if c in df.columns]
                df2 = df[exist].copy()
                for col in exist:
                    if isinstance(df2[col], pd.DataFrame):
                        df2[col] = df2[col].iloc[:, -1]
                cache[key] = df2
                save_cache(cache)
                print("成功")
                return df2
            else:
                print("数据不足")
        except Exception as e:
            print(f"错误: {e}")
        time.sleep(random.uniform(*REQUEST_DELAY))
    print(f"  {ticker} 失败")
    return None

# ==================== 计算 RSI(14) ====================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else None

# ==================== 计算自定义 RS Rating（百分位） ====================
def calculate_custom_rs_rating(stock_returns, sp500_return):
    """
    RS = (stock_1y_return / sp500_1y_return) → 转为百分位排名
    """
    if sp500_return <= 0 or len(stock_returns) == 0:
        return None
    ratios = stock_returns / sp500_return
    ratios = ratios.dropna()
    if len(ratios) == 0:
        return None
    # 百分位排名（1-100）
    percentile = (ratios.rank(pct=True) * 100).iloc[-1]
    return int(round(percentile))

# ==================== 获取 S&P 500 1年回报 ====================
def get_sp500_return():
    sp_cache_key = "SP500_1y"
    if sp_cache_key in cache:
        return cache[sp_cache_key]
    df = smart_download(SP500_TICKER)
    if df is None or 'Close' not in df.columns:
        return None
    close = ensure_series(df['Close'])
    if len(close) < 2:
        return None
    sp_return = close.iloc[-1] / close.iloc[0] - 1
    cache[sp_cache_key] = sp_return
    save_cache(cache)
    return sp_return

# ==================== 信号计算 ====================
def calculate_signals(ticker):
    data = smart_download(ticker)
    if data is None or (isinstance(data, pd.DataFrame) and (data.empty or len(data) < 200)):
        return None

    close = ensure_series(data['Close'])
    high = ensure_series(data['High'])
    volume = ensure_series(data.get('Volume', pd.Series([0]*len(close))))

    price = last_scalar_from(close)
    if price is None or price < 10:
        return None

    # 均线
    ma50 = last_scalar_from(close.rolling(50).mean())
    ma200 = last_scalar_from(close.rolling(200).mean())
    ma_bull = (ma50 is not None and ma200 is not None and price > ma50 > ma200)

    # 52周新高突破
    high_52w = last_scalar_from(high.rolling(252).max())
    prev_high = high.rolling(252).max().iloc[-2] if len(high.rolling(252).max()) > 1 else None
    prev_close = last_scalar_from(close.shift(1))
    breakout = (high_52w is not None and prev_high is not None and
                prev_close is not None and price > high_52w and prev_close <= prev_high)

    # 量比
    vol_20_avg = last_scalar_from(volume.rolling(20).mean())
    vol_today = last_scalar_from(volume)
    vol_ratio = vol_today / vol_20_avg if vol_20_avg and vol_20_avg > 0 else 0
    volume_ok = vol_ratio >= 1.3
    no_volume = breakout and vol_ratio < 1.3

    # 20日动量
    mom_20d = (price / float(close.iloc[-21])) - 1 if len(close) >= 21 else 0.0

    # RSI(14) 自算
    rsi_14 = calculate_rsi(close, 14)
    rsi_ok = rsi_14 is None or rsi_14 < 70

    # 自定义 RS Rating（百分位）
    sp500_return = get_sp500_return()
    stock_return = price / close.iloc[0] - 1 if len(close) > 0 else 0
    rs_rating = calculate_custom_rs_rating(close.pct_change().dropna() + 1, sp500_return + 1) if sp500_return is not None else None
    rs_strong = rs_rating is not None and rs_rating >= 80

    # 机构买入
    inst_buy = (ma50 is not None and vol_ratio > 1.5 and price > ma50)

    # 评分
    score = int(ma_bull) + int(breakout and volume_ok) + int(rsi_ok) + int(mom_20d > 0.1) + int(rs_strong) + int(inst_buy)

    # 理由
    reasons = []
    if ma_bull: reasons.append("均线多头")
    if breakout and volume_ok: reasons.append(f"放量突破(量比{vol_ratio:.1f})")
    if rsi_14 is not None: reasons.append(f"RSI {rsi_14:.0f}")
    if mom_20d > 0.1: reasons.append(f"20日动量+{mom_20d:.1%}")
    if rs_strong: reasons.append(f"RS {rs_rating}")
    if inst_buy: reasons.append("机构买入")
    if no_volume: reasons.append("Warning: 无量突破")

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'Score': score,
        'Reasons': ' | '.join(reasons),
        'RSI': round(rsi_14, 1) if rsi_14 is not None else None,
        'Vol_Ratio': round(vol_ratio, 2),
        'RS_Rating': rs_rating
    }

# ==================== 主程序 ====================
if __name__ == "__main__":
    print(f"启动 Yahoo 动量选股（自定义 RS） ({len(UNIVERSE)} 只) - {datetime.now():%Y-%m-%d %H:%M}\n")

    results = []
    for i, t in enumerate(UNIVERSE, 1):
        print(f"[{i:02d}/{len(UNIVERSE)}] {t}...", end="")
        try:
            res = calculate_signals(t)
            if res and "Warning: 无量突破" not in (res.get('Reasons') or ""):
                results.append(res)
                print("入选")
            else:
                print("未入选")
        except Exception as e:
            print(f"运行时错误: {e}")
        time.sleep(random.uniform(*REQUEST_DELAY))

    if not results:
        print("\n今日无推荐股票")
        exit()

    df = pd.DataFrame(results).sort_values('Score', ascending=False).head(TOP_N)
    print("\n" + "="*100)
    print("今日潜力股推荐 (Yahoo + 自定义 RS)")
    print("="*100)
    print(df[['Ticker','Price','Score','RS_Rating','RSI','Vol_Ratio','Reasons']].to_string(index=False))

    file = f"picks_yahoo_custom_{datetime.now():%Y%m%d}.xlsx"
    df.to_excel(file, index=False)
    print(f"\n结果保存：{file}")