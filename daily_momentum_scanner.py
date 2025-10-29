# daily_momentum_scanner_google_http_proxy.py
# Google Finance -> 已改为 Yahoo Finance (yfinance) + HTTP 代理
# 处理 pandas Series/DataFrame 布尔歧义，增强容错

import yfinance as yf
import pandas as pd
import requests
import time
import random
import os
from datetime import datetime
import warnings
import ssl
import urllib3
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
CACHE_FILE = "google_http_cache.pkl"
REQUEST_DELAY = (1.5, 3.0)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

EXCHANGE_MAP = {
    'JPM':'NYSE','WFC':'NYSE','BAC':'NYSE','TM':'NYSE','XOM':'NYSE','T':'NYSE',
    'PG':'NYSE','JNJ':'NYSE','HD':'NYSE','KO':'NYSE','UNH':'NYSE','PM':'NYSE',
    'BA':'NYSE','GE':'NYSE','DIS':'NYSE','PFE':'NYSE','MRK':'NYSE','CVX':'NYSE',
    'COP':'NYSE','NVO':'NYSE','PEP':'NYSE','AXP':'NYSE','XLI':'NYSE','XLU':'NYSE',
    'GM':'NYSE','GS':'NYSE','MS':'NYSE',
    'default':'NASDAQ'
}
# ===============================================

# ==================== 缓存 ====================
def load_cache():
    return pd.read_pickle(CACHE_FILE) if os.path.exists(CACHE_FILE) else {}

def save_cache(cache):
    pd.to_pickle(cache, CACHE_FILE)

cache = load_cache()

# ==================== 强制降级 TLS + 忽略证书（保留原有做法） ====================
original_create_connection = ssl.create_default_context

def patched_create_connection(*args, **kwargs):
    ctx = original_create_connection(*args, **kwargs)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.set_ciphers('DEFAULT:@SECLEVEL=1')  # 降级 TLS（可选，失败则忽略）
    except Exception:
        pass
    return ctx

ssl.create_default_context = patched_create_connection

# ==================== 猴子补丁 requests.get()（让 requests 默认走代理并忽略证书警告） ====================
original_get = requests.get

def patched_get(*args, **kwargs):
    kwargs['proxies'] = kwargs.get('proxies', PROXY)
    kwargs['headers'] = kwargs.get('headers', {})
    kwargs['headers'].update(HEADERS)
    kwargs['timeout'] = kwargs.get('timeout', 20)
    kwargs['verify'] = False
    return original_get(*args, **kwargs)

requests.get = patched_get

# ==================== 辅助函数：把可能是 DataFrame/Series 的列标准化为 Series ====================
def ensure_series(obj):
    """
    接受 pd.Series 或 pd.DataFrame 或其他（数值），返回 pd.Series。
    如果是 DataFrame，返回最后一列（通常单 ticker 时就是该列）。
    """
    if isinstance(obj, pd.DataFrame):
        # 选最后一列作为代表（yfinance 在某些情况下会返回多列）
        return obj.iloc[:, -1]
    if isinstance(obj, pd.Series):
        return obj
    # 如果是标量
    return pd.Series([obj])

def last_scalar_from(obj):
    """
    从 Series 或 DataFrame 的最后一项取出标量 float。
    如果最后一项仍是 Series（重复索引等异常情况），继续取第一个元素。
    """
    if obj is None:
        return None
    if isinstance(obj, pd.DataFrame):
        # 取最后一行最后一列
        val = obj.iloc[-1, -1]
    elif isinstance(obj, pd.Series):
        val = obj.iloc[-1]
    else:
        val = obj
    # 如果 val 还是 pd.Series（极少见），再取第一个元素
    if isinstance(val, pd.Series):
        val = val.iloc[0]
    try:
        return float(val)
    except Exception:
        return None

# ==================== Yahoo 下载（替代 Google） ====================
def smart_download(ticker):
    key = f"{ticker}_1y"
    if key in cache:
        print(f"  [缓存命中] {ticker}")
        # 从缓存中返回 DataFrame（确保是 DataFrame）
        df = cache[key]
        if isinstance(df, pd.DataFrame):
            return df
        # 若缓存损坏，删除并重新下载
        cache.pop(key, None)

    for attempt in range(3):
        try:
            print(f"  [Yahoo Finance] {ticker} (attempt {attempt+1})...", end="")
            # yfinance 支持 proxy 参数为字符串
            df = yf.download(ticker, period="1y", interval="1d", proxy=PROXY.get('http'))
            # yfinance 有时会返回空 DataFrame 或只有少量行
            if isinstance(df, pd.DataFrame) and not df.empty and len(df) >= 200:
                # 只保留我们需要的列，容错：部分 tickers 可能没有 Volume 等列
                needed = ['Open', 'High', 'Low', 'Close', 'Volume']
                exist = [c for c in needed if c in df.columns]
                df2 = df[exist].copy()
                # 如果 Close 等是 DataFrame（多列），转成单列（取最后一列）
                for col in exist:
                    if isinstance(df2[col], pd.DataFrame):
                        df2[col] = df2[col].iloc[:, -1]
                cache[key] = df2
                save_cache(cache)
                print("成功")
                return df2
            else:
                print(" 数据不足或为空")
        except Exception as e:
            print(f"错误: {e}")

        time.sleep(random.uniform(*REQUEST_DELAY))

    print(f"  {ticker} 失败")
    return None

# ==================== Finviz RS（保持原实现，容错增强） ====================
def get_rs_rating(ticker):
    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        for df in pd.read_html(r.text):
            # 更稳健地查找 RS Rating
            df_str = df.astype(str).apply(lambda col: col.str.strip()).to_string()
            if 'RS Rating' in df_str:
                # 找到包含 RS Rating 的行
                mask = df.iloc[:, 0].astype(str).str.contains('RS Rating', na=False)
                if mask.any():
                    # 第二列通常是数值
                    val = df.loc[mask, df.columns[1]].iat[0]
                    try:
                        return int(str(val).strip())
                    except:
                        return None
        return None
    except Exception:
        return None

# ==================== 信号计算（修正所有布尔/类型问题） ====================
def calculate_signals(ticker):
    data = smart_download(ticker)
    if data is None or (isinstance(data, pd.DataFrame) and data.empty) or (hasattr(data, "__len__") and len(data) < 200):
        return None

    # 把各列标准化为 Series
    close = ensure_series(data.get('Close') if isinstance(data, dict) else data['Close'])
    high = ensure_series(data['High'])
    volume = ensure_series(data['Volume']) if 'Volume' in data.columns else pd.Series([0]*len(close), index=close.index)

    # 获取 price（最后一个有效标量）
    price = last_scalar_from(close)
    if price is None:
        return None
    if price < 10:
        return None

    # 计算均线，确保为标量
    ma50 = last_scalar_from(close.rolling(50).mean())
    ma200 = last_scalar_from(close.rolling(200).mean())
    ma_bull = False
    if ma50 is not None and ma200 is not None:
        ma_bull = (price > ma50) and (ma50 > ma200)

    # 52 周高点 (约 252 个交易日)
    high_52w = last_scalar_from(high.rolling(252).max())
    prev_high = None
    # 取上一个最大值（倒数第二个位置的 rolling max 结果）
    try:
        prev_high = float(high.rolling(252).max().iloc[-2])
    except Exception:
        prev_high = None

    breakout = False
    if high_52w is not None and prev_high is not None:
        # close.iloc[-2] 取上一个收盘价（可能为 Series，需要处理）
        prev_close_val = last_scalar_from(close.shift(1))
        if prev_close_val is not None:
            breakout = (price > high_52w) and (prev_close_val <= prev_high)

    # 量比 = 当日成交量 / 20 日平均量
    vol_20_avg = last_scalar_from(volume.rolling(20).mean())
    vol_today = last_scalar_from(volume)
    vol_ratio = None
    if vol_20_avg and vol_20_avg > 0 and vol_today is not None:
        vol_ratio = vol_today / vol_20_avg
    else:
        vol_ratio = 0.0

    volume_ok = vol_ratio >= 1.3
    no_volume = breakout and vol_ratio < 1.3

    # RSI 计算（14 日）
    delta = ensure_series(close).diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi_val = None
    try:
        # 避免除以零
        last_gain = last_scalar_from(gain)
        last_loss = last_scalar_from(loss)
        if last_gain is None or last_loss is None:
            rsi_val = None
        elif last_loss == 0 and last_gain == 0:
            rsi_val = 50.0
        else:
            # 使用 gain/loss 计算
            # 处理 last_loss == 0 情况
            if last_loss == 0:
                rsi_val = 100.0
            else:
                rs = last_gain / last_loss
                rsi_val = 100 - (100 / (1 + rs))
    except Exception:
        rsi_val = None

    rsi_ok = True if rsi_val is None else (rsi_val < 70)

    # 20 日动量（用 21 日前的收盘）
    mom_20d = 0.0
    try:
        if len(close) >= 21:
            close_21 = last_scalar_from(close.shift(21) * 0 + close.shift(21))  # safe access
            # simpler: take close.iloc[-21]
            prev21 = last_scalar_from(close.iloc[:-21+len(close)].iloc[-1]) if False else None  # placeholder not used
            # simpler direct:
            mom_20d = (price / float(close.iloc[-21])) - 1 if len(close) >= 21 else 0.0
    except Exception:
        # fallback: try using .iloc safely
        try:
            mom_20d = (price / float(close.iloc[-21])) - 1 if len(close) >= 21 else 0.0
        except Exception:
            mom_20d = 0.0

    # RS Rating
    rs_rating = get_rs_rating(ticker)
    rs_strong = rs_rating >= 80 if rs_rating else False

    # 机构买入条件
    inst_buy = False
    if ma50 is not None and vol_ratio is not None:
        inst_buy = (price > ma50) and (vol_ratio > 1.5)

    # Score 计算（布尔项转 0/1）
    score = int(ma_bull) + int(breakout and volume_ok) + int(rsi_ok) + int(mom_20d > 0.1) + int(rs_strong) + int(inst_buy)

    # 构造 reasons 列表
    reasons = []
    if ma_bull:
        reasons.append("均线多头")
    if breakout and volume_ok:
        reasons.append(f"放量突破(量比{vol_ratio:.1f})")
    if rsi_val is not None:
        reasons.append(f"RSI {rsi_val:.0f}")
    if mom_20d > 0.1:
        reasons.append(f"20日动量+{mom_20d:.1%}")
    if rs_strong:
        reasons.append(f"RS {rs_rating}")
    if inst_buy:
        reasons.append("机构买入")
    if no_volume:
        reasons.append("Warning: 无量突破")

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'Score': score,
        'Reasons': ' | '.join(reasons),
        'RSI': round(rsi_val, 1) if rsi_val is not None else None,
        'Vol_Ratio': round(vol_ratio, 2) if vol_ratio is not None else None,
        'RS_Rating': rs_rating
    }

# ==================== 主程序 ====================
if __name__ == "__main__":
    print(f"启动 Yahoo+HTTP代理 动量选股 ({len(UNIVERSE)} 只) - {datetime.now():%Y-%m-%d %H:%M}\n")

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
    print("\n" + "="*90)
    print("今日潜力股推荐 (Yahoo + HTTP代理)")
    print("="*90)
    print(df[['Ticker','Price','Score','Reasons']].to_string(index=False))

    file = f"picks_yahoo_http_{datetime.now():%Y%m%d}.xlsx"
    df.to_excel(file, index=False)
    print(f"\n结果保存：{file}")
