# daily_momentum_scanner_custom_rs.py
# 使用 Yahoo Finance + yfinance 计算 RSI 和 自定义 RS Rating（百分位）
# 完全移除 TradingView 依赖
# 修改：支持盘后价格、VIX、7日量比、每日刷新缓存、扩大UNIVERSE

import yfinance as yf
import pandas as pd
import numpy as np
import time
import random
import os
from datetime import datetime, timedelta
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

# ================================================================================
# 最终 UNIVERSE：231 只股票（市值前150 + 10日均量前150）
# ================================================================================
UNIVERSE = [
    'NVDA', 'AAPL', 'MSFT', 'GOOG', 'GOOGL', 'AMZN', 'AVGO', 'META', 'TSLA', 'BRK-B',
    'LLY', 'JPM', 'WMT', 'ORCL', 'V', 'XOM', 'MA', 'NFLX', 'JNJ', 'PLTR',
    'AMD', 'COST', 'ABBV', 'BAC', 'HD', 'ASML', 'PG', 'GE', 'CVX', 'KO',
    'UNH', 'CSCO', 'IBM', 'WFC', 'MU', 'CAT', 'MS', 'AXP', 'CRM', 'GS',
    'RTX', 'PM', 'TMUS', 'ABT', 'SHOP', 'MCD', 'TMO', 'MRK', 'APP', 'DIS',
    'LRCX', 'LIN', 'PEP', 'UBER', 'AZN', 'ANET', 'ISRG', 'QCOM', 'C', 'PDD',
    'NOW', 'INTU', 'AMAT', 'ARM', 'INTC', 'BX', 'T', 'BLK', 'NEE', 'SCHW',
    'APH', 'VZ', 'KLAC', 'BKNG', 'AMGN', 'TJX', 'GILD', 'ETN', 'SPGI', 'ACN',
    'BA', 'DHR', 'GEV', 'BSX', 'PANW', 'TXN', 'COF', 'ADBE', 'PFE', 'SYK',
    'CRWD', 'LOW', 'UNP', 'HOOD', 'DE', 'HON', 'WELL', 'PGR', 'IBKR', 'PLD',
    'MELI', 'MDT', 'ADI', 'CEG', 'CB', 'LMT', 'COP', 'VRTX', 'HCA', 'KKR',
    'MCK', 'ADP', 'DELL', 'DASH', 'SO', 'CMCSA', 'CVS', 'PH', 'TT', 'CME',
    'DUK', 'MO', 'TRI', 'BMY', 'GD', 'SBUX', 'CDNS', 'NKE', 'NEM', 'MMC',
    'MCO', 'COIN', 'MMM', 'AMT', 'SHW', 'ICE', 'HWM', 'NOC', 'EQIX', 'WM',
    'MRVL', 'ORLY', 'JCI', 'UPS', 'EMR', 'SNPS', 'ABNB', 'BK', 'APO', 'CTAS',
    'GLW', 'MDLZ', 'USB', 'MSTR', 'WMB', 'CSX', 'PYPL', 'GM', 'CL', 'KMI',
    'FCX', 'TFC', 'WBD', 'SLB', 'O', 'WDC', 'F', 'EW', 'CARR', 'FAST',
    'EXC', 'BKR', 'CMG', 'KR', 'LVS', 'TGT', 'OXY', 'EBAY', 'DAL', 'KDP',
    'PCG', 'CTSH', 'EQT', 'FI', 'CCL', 'KMB', 'MCHP', 'VICI', 'UAL', 'HPE',
    'KVUE', 'KHC', 'TSCO', 'FITB', 'SMCI', 'HPQ', 'GIS', 'HBAN', 'DXCM', 'TTD',
    'HAL', 'RF', 'DVN', 'ON', 'NI', 'CTRA', 'KEY', 'IP', 'SW', 'AMCR',
    'CNC', 'WY', 'DD', 'DOW', 'LUV', 'KIM', 'DOC', 'VTRS', 'DECK', 'HST',
    'IVZ', 'AES', 'MRNA', 'IPG', 'BAX', 'MGM', 'NCLH', 'MOS', 'CAG', 'APA',
    'SOLS'
]
# UNIVERSE = [
#     'SMCI', 'GOOG', 'PLTR', 'AVGO', 'UNH', 'NVDA', 'AMD', 'MSFT', 'ORCL', 'TSM'
# ]
# SELF_SELECTED = [
#     'SMCI', 'GOOG', 'PLTR', 'AVGO', 'UNH', 'NVDA', 'AMD', 'MSFT', 'ORCL', 'TSM'
# ]
SELF_SELECTED = []
TOP_N = 10
CACHE_FILE = "yahoo_custom_cache.pkl"
REQUEST_DELAY = (1.5, 3.0)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# S&P 500 作为基准
SP500_TICKER = "^GSPC"
VIX_TICKER = "^VIX"
# ===============================================

# ==================== 缓存系统 ====================
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        cache = pd.read_pickle(CACHE_FILE)
        # 检查缓存日期，如果超过1天则清空
        if 'cache_date' in cache and (datetime.now().date() - cache['cache_date']).days > 1:
            cache = {}
        else:
            cache = cache.get('data', {})
        return cache
    except:
        return {}

def save_cache(cache):
    full_cache = {'data': cache, 'cache_date': datetime.now().date()}
    pd.to_pickle(full_cache, CACHE_FILE)

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
    today = datetime.now().date()
    if key in cache:
        df = cache[key]
        if (isinstance(df, pd.DataFrame) and len(df) >= 200 and 
            df.index[-1].date() >= (today - timedelta(days=3))):
            return df
        else:
            cache.pop(key, None)

    for attempt in range(3):
        try:
            print(f"  [Yahoo] {ticker} (attempt {attempt+1})...", end="")
            # 添加 prepost=True 以包含盘后价格
            df = yf.download(ticker, period="1y", interval="1d", prepost=True, proxy=PROXY.get('http'), progress=False)
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

# ==================== 获取 VIX ====================
def get_vix():
    try:
        df = yf.download(VIX_TICKER, period="5d", prepost=True, progress=False, proxy=PROXY.get('http'))
        if df is not None and not df.empty:
            return round(ensure_series(df['Close']).iloc[-1], 2)
    except:
        pass
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
def calculate_rs_rating(close, sp_close):
    """
    RS line = stock_close / sp_close, 然后取其历史百分位排名
    """
    if len(close) == 0 or len(sp_close) == 0:
        return None
    # 对齐索引
    common_idx = close.index.intersection(sp_close.index)
    if len(common_idx) < 200:
        return None
    close_aligned = close.loc[common_idx]
    sp_close_aligned = sp_close.loc[common_idx]
    rs = close_aligned / sp_close_aligned
    rs = rs.dropna()
    if len(rs) == 0:
        return None
    percentile = (rs.rank(pct=True) * 100).iloc[-1]
    return int(round(percentile))

# ==================== 信号计算 ====================
def calculate_signals(ticker, sp_close):
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
    ma10 = last_scalar_from(close.rolling(10).mean())
    ma20 = last_scalar_from(close.rolling(20).mean())
    ma50 = last_scalar_from(close.rolling(50).mean())
    ma_bull = (ma10 is not None and ma20 is not None and ma50 is not None and price > ma10 > ma20 > ma50)

    # 52周新高突破
    high52_prev = last_scalar_from(high.rolling(252).max().shift(1))
    prev_close = last_scalar_from(close.shift(1))
    breakout = (high52_prev is not None and prev_close is not None and 
                price > high52_prev and prev_close <= high52_prev)

    # 量比
    vol_20_avg = last_scalar_from(volume.rolling(20).mean())
    vol_7_avg = last_scalar_from(volume.rolling(7).mean())
    vol_today = last_scalar_from(volume)
    vol_20_ratio = vol_today / vol_20_avg if vol_20_avg and vol_20_avg > 0 else 0
    vol_7_ratio = vol_today / vol_7_avg if vol_7_avg and vol_7_avg > 0 else 0
    volume_ok = vol_20_ratio >= 1.3
    no_volume = breakout and vol_20_ratio < 1.3

    # 20日动量
    mom_20d = (price / float(close.iloc[-21])) - 1 if len(close) >= 21 else 0.0

    # RSI(14)
    rsi_14 = calculate_rsi(close, 14)

    # 自定义 RS Rating
    rs_rating = calculate_rs_rating(close, sp_close) if sp_close is not None else None
    rs_strong = rs_rating is not None and rs_rating >= 70

    # 机构买入
    inst_buy = (ma50 is not None and vol_20_ratio > 1.5 and price > ma50)

    # ==================== 计分项 ====================
    score = 0
    reasons = []

    if ma_bull:
        score += 1
        reasons.append("均线多头")

    if breakout and volume_ok:
        score += 1
        reasons.append(f"放量突破(量比{vol_20_ratio:.1f})")

    # 【新增计分】RSI 在 60~70 之间
    rsi_in_range = rsi_14 is not None and 60 <= rsi_14 <= 70
    if rsi_in_range:
        score += 1
        reasons.append(f"RSI 强势区间 {rsi_14:.0f}")

    if mom_20d > 0.1:
        score += 1
        reasons.append(f"20日动量+{mom_20d:.1%}")

    if rs_strong:
        score += 1
        reasons.append(f"RS {rs_rating}")

    if inst_buy:
        score += 1
        reasons.append("机构买入")

    # 【新增计分】当天成交量 > 700万
    vol_today_million = vol_today / 1_000_000 if vol_today else 0
    if vol_today_million > 7:
        score += 1
        reasons.append(f"高量 {vol_today_million:.1f}M")

    # 警告
    if no_volume:
        reasons.append("Warning: 无量突破")
    if vol_7_ratio > 1.5:
        reasons.append(f"高7日量({vol_7_ratio:.1f})")

    # 7日均量（单位：百万）
    vol_7_avg_million = vol_7_avg / 1_000_000 if vol_7_avg else 0

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'Volume': int(vol_today) if vol_today else 0,
        'Volume_M': round(vol_today_million, 2),
        'Vol_7_Avg': int(vol_7_avg) if vol_7_avg else 0,
        'Vol_7_Avg_M': round(vol_7_avg_million, 2),
        'Score': score,
        'Reasons': ' | '.join(reasons),
        'RSI': round(rsi_14, 1) if rsi_14 is not None else None,
        'Vol_20_Ratio': round(vol_20_ratio, 2),
        'Vol_7_Ratio': round(vol_7_ratio, 2),
        'RS_Rating': rs_rating
    }

# ==================== 主程序 ====================
if __name__ == "__main__":
    # 【新增】强制删除缓存文件，保证每次运行都获取最新数据
    if os.path.exists(CACHE_FILE):
        try:
            os.remove(CACHE_FILE)
            print(f"已删除旧缓存文件：{CACHE_FILE}，本次将重新下载最新数据。")
        except Exception as e:
            print(f"删除缓存失败：{e}")

    # 重新初始化缓存（为空）
    cache = {}

    print(f"启动 Yahoo 动量选股（自定义 RS） ({len(UNIVERSE)} 只) - {datetime.now():%Y-%m-%d %H:%M}\n")

    # 获取 S&P 500 数据和 VIX
    sp_df = smart_download(SP500_TICKER)
    if sp_df is None:
        print("无法获取 S&P 500 数据，退出")
        exit()
    sp_close = ensure_series(sp_df['Close'])
    vix = get_vix()
    print(f"VIX (恐慌指数): {vix if vix else 'N/A'}\n")

    results = []
    for i, t in enumerate(UNIVERSE, 1):
        print(f"[{i:02d}/{len(UNIVERSE)}] {t}...", end="")
        try:
            res = calculate_signals(t, sp_close)
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

    # 1. 原始数据
    df = pd.DataFrame(results)

    # 2. 取 Top‑N
    top_df = df.sort_values('Score', ascending=False).head(TOP_N)

    # 3. SELF_SELECTED
    self_selected_df = df[df['Ticker'].isin(SELF_SELECTED)]

    # 4. 合并去重
    final_df = pd.concat([top_df, self_selected_df]).drop_duplicates(subset='Ticker').reset_index(drop=True)
    final_df = final_df.sort_values('Score', ascending=False).reset_index(drop=True)

    print("\n" + "="*120)
    print("今日潜力股推荐 (Volume>7M & RSI 60~70 计分项)")
    print("="*120)
    display_cols = [
        'Ticker', 'Price', 'Volume_M', 'Vol_7_Avg_M',
        'Score', 'RS_Rating', 'RSI', 'Vol_20_Ratio', 'Vol_7_Ratio', 'Reasons'
    ]
    print(final_df[display_cols].to_string(index=False))

    # ==================== 中文指标解释 ====================
    print("\n" + "="*120)
    print("指标解释")
    print("="*120)
    print("RS_Rating   : 相对强度评级（1-100），衡量该股票过去一年相对大盘（标普500）的表现强弱。")
    print("             数值越高越强，≥80 表示在前20%最强股票中。")
    print("RSI         : 相对强弱指数（14日），反映短期价格动能。")
    print("             <30 超卖，>70 超买，<70 为宜（避免追高）。")
    print("VIX         : 恐慌指数（芝加哥期权交易所波动率指数），反映市场对未来30天波动的预期。")
    print("             <15 低恐慌，>30 高恐慌，>50 极度恐慌。")
    print("Vol_20_Ratio: 今日成交量 / 过去20日平均成交量。")
    print("             >1.3 表示放量，>1.5 更强，显示资金活跃。")
    print("Vol_7_Ratio : 今日成交量 / 过去7日平均成交量。")
    print("             用于捕捉短期资金异动，>1.5 表明近期量能突然放大。")
    print("="*120)

    # 保存 Excel（保留原始 Volume 整数）
    file = f"picks_yahoo_score_vol_rsi_{datetime.now():%Y%m%d}.xlsx"
    save_df = final_df.drop(columns=['Volume_M', 'Vol_7_Avg_M'])
    save_df.to_excel(file, index=False)
    print(f"\n结果保存：{file}")