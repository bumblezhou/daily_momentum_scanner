import requests
import pandas as pd
import duckdb
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import urllib3
import warnings
import pytz
from datetime import date, datetime, timedelta
import pandas_market_calendars as mcal

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# ===================== 配置 =====================
FINNHUB_TOKEN = "d40ckf9r01qqo3qha4bgd40ckf9r01qqo3qha4c0"

DUCKDB_PATH = "stock_data.duckdb"

PROXIES = {
    "http": "http://127.0.0.1:8118",
    "https": "http://127.0.0.1:8118",
}

MAX_WORKERS = 8          # yfinance 并发线程
YF_BATCH_SIZE = 20       # 每批 ticker 数
# ===============================================

yf.set_config(proxy="http://127.0.0.1:8118")

# ===================== DuckDB 初始化 =====================
def init_db():
    con = duckdb.connect(DUCKDB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_ticker (
            symbol TEXT PRIMARY KEY,
            description TEXT,
            mic TEXT,
            currency TEXT,
            type TEXT,
            sector TEXT,
            industry TEXT,
            updated_at TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_price (
            stock_code TEXT,
            trade_date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (stock_code, trade_date)
        )
    """)
    con.close()


# ===================== 1. Finnhub 下载所有 US Tickers =====================
def fetch_us_tickers():
    print("📥 下载 Finnhub US 股票列表...")
    r = requests.get(
        f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_TOKEN}",
        proxies=PROXIES,
        timeout=60,
        verify=False
    )
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data)
    print(f"获取 {len(df)} 个 ticker")
    return df


def upsert_stock_tickers(df):
    init_db()

    con = duckdb.connect(DUCKDB_PATH)

    df = df[[
        "symbol", "description", "mic", "currency", "type"
    ]].copy()
    df["updated_at"] = datetime.now()

    con.execute("""
        INSERT INTO stock_ticker
        SELECT * FROM df
        ON CONFLICT(symbol) DO UPDATE SET
            description = EXCLUDED.description,
            mic = EXCLUDED.mic,
            currency = EXCLUDED.currency,
            type = EXCLUDED.type,
            updated_at = EXCLUDED.updated_at
    """)

    con.close()
    print("✅ stock_ticker 表已更新")


# ===================== 2. yfinance 下载近一年行情 =====================
def download_price_batch(tickers):
    try:
        data = yf.download(
            tickers=tickers,
            period="3y",
            interval="1d",
            group_by="ticker",
            auto_adjust=True, # 复权价格
            threads=False,
            # proxy=PROXIES["http"]
        )
        return data
    except Exception as e:
        print(f"❌ 批量下载失败: {e}")
        return None


def save_price_to_duckdb(data):
    if data is None or data.empty:
        return

    con = duckdb.connect(DUCKDB_PATH)
    rows = []

    if isinstance(data.columns, pd.MultiIndex):
        # 多 ticker
        for ticker in data.columns.levels[0]:
            df = data[ticker].dropna()
            for date, row in df.iterrows():
                rows.append((
                    yahoo_to_finnhub(ticker),
                    date.date(),
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    int(row["Volume"])
                ))
    else:
        # 单 ticker
        for date, row in data.iterrows():
            rows.append((
                yahoo_to_finnhub(data.name),
                date.date(),
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"])
            ))

    if rows:
        con.executemany("""
        INSERT INTO stock_price (
            stock_code,
            trade_date,
            open,
            high,
            low,
            close,
            volume
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, trade_date) DO NOTHING
        """, rows)

    con.close()


def fetch_all_prices():
    con = duckdb.connect(DUCKDB_PATH)
    raw_tickers = con.execute("""
        SELECT symbol FROM stock_ticker
        WHERE type = 'Common Stock' AND mic IN (
            'XNYS',
            'XNGS',
            'XNAS',
            'XASE',
            'ARCX',
            'BATS',
            'IEXG'
        );
    """).fetchall()
    con.close()

    tickers = [finnhub_to_yahoo(t[0]) for t in raw_tickers]
    print(f"📊 准备下载 {len(tickers)} 只股票的行情")

    for i in range(0, len(tickers), YF_BATCH_SIZE):
        batch = tickers[i:i + YF_BATCH_SIZE]
        print(f"   下载 {i} - {i + len(batch)}")
        data = download_price_batch(batch)
        save_price_to_duckdb(data)
        time.sleep(1)


# 获取 US 市场节假日 & 最近 N 个交易日
def get_recent_trading_days_smart(n=10):
    """
    使用真实的纽交所(NYSE)日历获取最近交易日
    """
    nyse = mcal.get_calendar('NYSE')
    tz_ny = pytz.timezone('America/New_York')
    now_ny = datetime.now(tz_ny)
    
    # 设定查询范围：从 30 天前到今天
    # 考虑到上海中午运行美股还没开盘/刚收盘，终点设为美东今天
    end_date = now_ny.date()
    start_date = end_date - timedelta(days=30)
    
    # 获取纽交所实际开盘的日期表（自动排除周末和美股法定节假日）
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    
    # 获取已完成交易的日期列表（排除掉还没收盘的今天，除非已经在美东17:00后）
    valid_days = schedule.index.date
    if now_ny.hour < 17:
        # 如果美东还没到下午5点，当天的K线可能还没封装好，取到昨天为止
        valid_days = [d for d in valid_days if d < now_ny.date()]
        
    return [d.strftime('%Y-%m-%d') for d in valid_days[-n:]]


# 找出「最近交易日有缺失行情」的 ticker
def get_tickers_missing_recent_data(target_date):
    """
    返回尚未更新到最近一个交易日的 ticker 列表
    """
    con = duckdb.connect(DUCKDB_PATH)

    query = f"""
        SELECT t.symbol
        FROM stock_ticker t
        LEFT JOIN (
            SELECT stock_code, MAX(trade_date) AS last_date
            FROM stock_price
            GROUP BY stock_code
        ) p ON p.stock_code = t.symbol
        WHERE t.type = 'Common Stock'
          AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
          AND COALESCE(t.yf_price_available, TRUE) = TRUE
          AND (p.last_date IS NULL OR p.last_date < CAST('{target_date}' AS DATE))
    """

    tickers = [r[0] for r in con.execute(query).fetchall()]
    con.close()
    return tickers


def mark_yf_unavailable(symbols):
    if not symbols:
        return

    con = duckdb.connect(DUCKDB_PATH)
    con.executemany(
        """
        UPDATE stock_ticker
        SET yf_price_available = FALSE
        WHERE symbol = ?
        """,
        [(yahoo_to_finnhub(s),) for s in symbols]
    )
    con.close()


# symbol ↔ yahoo_symbol 映射函数
def finnhub_to_yahoo(symbol: str) -> str:
    """
    Finnhub / Exchange symbol -> Yahoo Finance symbol
    BRK.A -> BRK-A
    """
    return symbol.replace(".", "-")


def yahoo_to_finnhub(symbol: str) -> str:
    """
    Yahoo Finance symbol -> Finnhub / Exchange symbol
    BRK-A -> BRK.A
    """
    return symbol.replace("-", ".")


# 用 yfinance 批量补齐最近 10 个交易日行情（20 支一批）
def update_recent_prices(watchlist: list = []):
    print(f"🕒 当前上海时间: {datetime.now():%Y-%m-%d %H:%M}")
    
    # 1. 自动根据 NYSE 日历获取最近 10 个有效交易日
    # 这里面已经自动排除了周末、圣诞节、感恩节等
    trading_days = get_recent_trading_days_smart(10)
    print(f"📅 纽交所最近有效交易日：{trading_days}")
    
    target_date = trading_days[-1]
    print(f"🎯 目标同步日期: {target_date}")

    # 2. 检查数据库缺失
    raw_tickers = get_tickers_missing_recent_data(target_date)
    if watchlist:
        # 合并自选列表
        raw_tickers = list(set(raw_tickers) | set(watchlist))

    if not raw_tickers:
        print(f"✅ 数据库已是最新（美东 {target_date} 已对齐），跳过更新")
        return

    yahoo_map = {t: finnhub_to_yahoo(t) for t in raw_tickers}
    yahoo_tickers = list(yahoo_map.values())

    print(f"需要更新 {len(yahoo_tickers)} 只股票: {yahoo_tickers}")

    for i in range(0, len(yahoo_tickers), YF_BATCH_SIZE):
        batch = yahoo_tickers[i:i + YF_BATCH_SIZE]
        print(f"   更新 {i} - {i + len(batch)}")

        failed = []

        try:
            data = yf.download(
                tickers=batch,
                period="20d",
                interval="1d",
                group_by="ticker",
                threads=False,
                auto_adjust=True, # 复权价格
                # proxy=PROXIES["http"]
            )

            save_price_to_duckdb(data)

            # 🔍 判断哪些 ticker 没拿到数据
            if isinstance(data.columns, pd.MultiIndex):
                for yf_symbol in batch:
                    if yf_symbol not in data.columns.levels[0]:
                        failed.append(yf_symbol)
                        continue

                    df = data[yf_symbol]

                    # 核心判断：Close 是否全部 NaN
                    if df.empty or df["Close"].dropna().empty:
                        failed.append(yf_symbol)
            else:
                # 单 ticker 情况
                if data.empty or data["Close"].dropna().empty:
                    failed.extend(batch)

        except Exception as e:
            print(f"❌ 批次失败: {batch}, {e}")
            failed.extend(batch)

        if failed:
            # 反查原始 symbol
            reverse_map = {v: k for k, v in yahoo_map.items()}
            failed_symbols = [reverse_map[s] for s in failed if s in reverse_map]

            print(f"⚠️ 标记以下 ticker 为 yf 不可用: {failed_symbols}")
            mark_yf_unavailable(failed_symbols)

        time.sleep(1)

    print("🎉 全部完成")


# 创建基本面数据表结构
def init_fundamental_table(con):
    """初始化基本面数据表"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_fundamentals (
            stock_code VARCHAR PRIMARY KEY,           -- 股票代码，主键
            update_date DATE,                         -- 本次基本面数据更新日期
            quarterly_eps_growth DOUBLE,              -- C: 当前季度 EPS 同比增长率（earningsQuarterlyGrowth）
            annual_eps_growth DOUBLE,                 -- A: 年度 EPS 同比增长率（earningsGrowth）
            revenue_growth DOUBLE,                    -- 营收同比增长率（辅助指标）
            roe DOUBLE,                               -- ROE（净资产收益率）
            shares_outstanding BIGINT,                -- S: 流通股本（sharesOutstanding）
            inst_ownership DOUBLE,                    -- I: 机构持仓比例（heldPercentInstitutions）
            fcf_quality DOUBLE,                       -- 自由现金流质量（fcf / ocf）
            canslim_score INTEGER,                    -- CAN SLIM 综合得分（代码中计算）
            market_cap BIGINT                         -- 市值（marketCap）
        );
    """)


# 编写“增量更新”脚本（扩展为 CAN SLIM）
def update_fundamentals(ticker_list, force_update=False):
    """
    定期更新基本面数据，包括 CAN SLIM 特定指标
    force_update: 是否强制更新所有股票，否则只更新过期数据
    """

    con = duckdb.connect(DUCKDB_PATH)

    init_fundamental_table(con)

    # 1. 找出需要更新的 Tickers
    if force_update:
        need_update = ticker_list
    else:
        # 找出库里没有的，或者更新时间超过 7 天的
        existing = con.execute("""
            SELECT stock_code FROM stock_fundamentals 
            WHERE update_date >= CURRENT_DATE
        """).df()['stock_code'].tolist()
        need_update = [t for t in ticker_list if t not in existing]

    if not need_update:
        print("✅ 所有基本面数据均在有效期内，无需更新。")
        return

    print(f"🚀 开始更新 {len(need_update)} 只股票的基本面...")

    for symbol in need_update:
        try:
            fundamentals_sql = f"""
                SELECT stock_code FROM stock_fundamentals WHERE update_date >= CURRENT_DATE AND stock_code = '{symbol}'
            """
            fundamentals_sql_df = con.execute(fundamentals_sql).df()
            if not fundamentals_sql_df.empty:
                print(f"  [跳过] {symbol} 基本面数据在有效期内")
                continue
            
            t = yf.Ticker(finnhub_to_yahoo(symbol))
            info = t.info

            # --- 金律字段提取 ---
            market_cap = info.get('marketCap', 0) or 0

            # 更新 sector 和 industry
            sector = info.get("sector")
            industry = info.get("industry")
            con.execute("""
                UPDATE stock_ticker
                SET sector = ?, industry = ?, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = ?
            """, (sector, industry, symbol))
            
            # 提取 CAN SLIM 指标
            quarterly_eps_growth = info.get("earningsQuarterlyGrowth")  # C
            annual_eps_growth = info.get("earningsGrowth")  # A (年度)
            rev_growth = info.get("revenueGrowth")  # 辅助
            roe = info.get("returnOnEquity")
            shares_outstanding = info.get("sharesOutstanding")  # S
            inst_own = info.get("heldPercentInstitutions")  # I
            fcf = info.get("freeCashflow")
            ocf = info.get("operatingCashflow")
            fcf_quality = (fcf / ocf) if (fcf and ocf and ocf > 0) else None

            # 计算 CAN SLIM 分数 (简化：每个组件达标加1分)
            score = 0
            if quarterly_eps_growth and quarterly_eps_growth > 0.25: score += 1  # C >25%
            if annual_eps_growth and annual_eps_growth > 0.25: score += 1  # A >25%
            if rev_growth and rev_growth > 0.15: score += 1  # 营收辅助
            if shares_outstanding and shares_outstanding < 100000000: score += 1  # S: 低股本 <1亿股 (可调)
            if inst_own and inst_own > 0.5: score += 1  # I: 机构 >50%
            # N/L/M 在技术筛选中处理

            # 使用 UPSERT 逻辑
            con.execute("""
                INSERT OR REPLACE INTO stock_fundamentals 
                VALUES (?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, quarterly_eps_growth, annual_eps_growth,  rev_growth, roe, shares_outstanding, inst_own, fcf_quality, score, market_cap
            ))

            print(f"  [OK] {symbol} (CAN SLIM Score: {score})")
            time.sleep(0.5)  # 频率控制

        except Exception as e:
            print(f"  [ERR] {symbol} 更新失败: {e}")
            continue
    
    con.close()

def get_latest_date_in_db():
    con = duckdb.connect(DUCKDB_PATH)
    latest_date_in_db = con.execute("SELECT MAX(trade_date) FROM stock_price").fetchone()[0]
    con.close()
    return latest_date_in_db


# ==================== 新增：回撤深度与波动模拟函数 ====================
def simulate_pullback_range(stock_code, current_vix=18.0):
    """
    基于 ATR、历史回撤及 VIX 动态调节因子模拟入场区间与硬止损
    :param stock_code: 股票代码
    :param current_vix: 当前市场 VIX 指数，默认 18.0 (基准均值)
    """
    con = duckdb.connect(DUCKDB_PATH)
    
    # 从数据库获取最近 20 个交易日数据
    sql = f"""
        SELECT trade_date, open, high, low, close 
        FROM stock_price 
        WHERE stock_code = '{stock_code}' 
        ORDER BY trade_date DESC 
        LIMIT 20
    """
    try:
        df = con.execute(sql).df().sort_values('trade_date')
        con.close()
        if len(df) < 15:
            return None
    except Exception as e:
        print(f"提取 {stock_code} 波动数据失败: {e}")
        return None

    # --- A. 计算 15日 ATR (真实波幅) ---
    high_low = df['high'] - df['low']
    high_prev_close = (df['high'] - df['close'].shift(1)).abs()
    low_prev_close = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    atr_15 = tr.tail(15).mean()

    # --- B. 计算 VIX 调节因子 (关键优化) ---
    # 理论依据：VIX 越高，市场非理性波动越大，需要更宽的止损垫
    # 基准 VIX 设为 18，每高出 1 点，波动空间放大 5%
    vix_factor = 1.0
    if current_vix > 18:
        vix_factor = 1 + (current_vix - 18) * 0.05
        vix_factor = min(vix_factor, 1.8)  # 最高限制在 1.8 倍，防止止损过深

    current_price = df['close'].iloc[-1]
    
    # --- C. 计算动态水位 ---
    # 理想入场位：价格回调 0.6 倍 ATR (经 VIX 修正)
    pullback_dist = atr_15 * 0.6 * vix_factor
    entry_low = current_price - pullback_dist
    entry_high = current_price * 0.99  # 至少等待 1% 的回调以避免追涨

    # 硬止损位：1.5 倍 ATR (经 VIX 修正)，防扫单
    stop_dist = atr_15 * 1.5 * vix_factor
    hard_stop = current_price - stop_dist

    return {
        'ideal_entry': f"{entry_low:.2f} - {entry_high:.2f}",
        'hard_stop': round(hard_stop, 2),
        'atr_15': round(atr_15, 2),
        'vix_adj': round(vix_factor, 2)
    }


def filter_dip_stocks_from_db(target_date_str: str):
    """
    实现严格突破回踩策略：
    1. A为支撑日，B为突破日（A在B前40天内）。
    2. A与B之间所有收盘价 < A的收盘价。
    3. 当前日(C/D/E)在B之后，且满足回踩支撑位A的条件。
    """
    con = duckdb.connect(DUCKDB_PATH)
    
    # 参数定义
    VOLATILITY_LIMIT = 0.05         # 条件2.3：波动性限制
    SUPPORT_TOLERANCE = 0.995       # 条件2.1：最低价容差因子
    MIN_PRICE = 5.0                 # 金律①
    MIN_DOLLAR_VOLUME = 2000000     # 金律②
    
    sql = f"""
    WITH Ordered AS (
        SELECT
            stock_code,
            trade_date,
            open, high, low, close, volume,
            ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date) AS rn
        FROM stock_price
    ),

    A_Candidate AS (
        SELECT
            o1.stock_code,
            o1.trade_date AS support_date,
            o1.close AS support_price,
            o1.rn AS a_rn
        FROM Ordered o1
        WHERE NOT EXISTS (
            SELECT 1 FROM Ordered o2
            WHERE o2.stock_code = o1.stock_code
            AND o2.rn BETWEEN o1.rn + 1 AND o1.rn + 40
            AND o2.close >= o1.close
        )
    ),

    B_Day AS (
        SELECT
            a.stock_code,
            a.support_date,
            a.support_price,
            o.trade_date AS breakthrough_date,
            o.close AS breakthrough_price
        FROM A_Candidate a
        JOIN Ordered o
        ON o.stock_code = a.stock_code
        AND o.rn = a.a_rn + 41
        AND o.close > a.support_price
    ),

    C_Dip AS (
        SELECT
            b.stock_code,
            b.support_date,
            b.support_price,
            b.breakthrough_date,
            b.breakthrough_price,
            o.trade_date AS dip_date,
            o.close AS dip_price,
            o.high,
            o.low,
            o.volume,
            (o.high - o.low) / NULLIF(LAG(o.close) OVER (PARTITION BY o.stock_code ORDER BY o.trade_date), 0) AS volatility,
            AVG(o.close * o.volume) OVER (
                PARTITION BY o.stock_code
                ORDER BY o.trade_date
                ROWS 50 PRECEDING
            ) AS avg_dollar_volume
        FROM B_Day b
        JOIN Ordered o
        ON o.stock_code = b.stock_code
        AND o.trade_date > b.breakthrough_date
    )

    SELECT
        stock_code,
        support_date,
        support_price,
        breakthrough_date,
        breakthrough_price,
        dip_date,
        dip_price,
        high,
        low,
        volume,
        volatility,
        avg_dollar_volume
    FROM C_Dip
    WHERE
        high >= support_price
        AND low * 0.995 <= support_price
        AND dip_price > support_price
        /* AND volatility < {VOLATILITY_LIMIT} */
        AND dip_price >= {MIN_PRICE}
        AND avg_dollar_volume IS NOT NULL
        AND avg_dollar_volume >= {MIN_DOLLAR_VOLUME}
        AND dip_date = '{target_date_str}'
    """
    
    result_df = con.execute(sql).df()
    con.close()
    return result_df


# ==================== 计算全市场 RS Rank ====================
def calculate_rs_rank_for_candidates(candidates_df, target_date_str):
    """基于全市场表现计算 RS Rank"""
    if candidates_df.empty: return candidates_df
    con = duckdb.connect(DUCKDB_PATH)
    sql = f"""
    WITH Performance AS (
        SELECT stock_code,
               (MAX(close) - MIN(close)) / NULLIF(MIN(close), 0) as yearly_return
        FROM stock_price
        WHERE trade_date >= CAST('{target_date_str}' AS DATE) - INTERVAL '1 year'
        GROUP BY stock_code
    )
    SELECT stock_code, ROUND(PERCENT_RANK() OVER (ORDER BY yearly_return) * 100, 1) as rs_rank
    FROM Performance
    """
    rs_df = con.execute(sql).df()
    con.close()
    return pd.merge(candidates_df, rs_df, on='stock_code', how='left')

# ==================== 给候选股票数据关联上基本面数据 ====================
def link_fundamental_data(candidates_df):
    """给候选股票数据关联上基本面数据"""
    # 关联基本面数据并应用 金律③ 和 市值区间
    MIN_MARKET_CAP = 300_000_000    # 3亿美元
    MAX_MARKET_CAP = 5_000_000_000  # 建议放宽到50亿美元以覆盖更多类似A股的高质股
    MIN_INST_OWN = 0.3             # 金律③：机构持仓 > 30%
    con = duckdb.connect(DUCKDB_PATH)
    found_sql = f"""
        SELECT
            stock_code,
            quarterly_eps_growth,
            annual_eps_growth,
            revenue_growth,
            roe,
            shares_outstanding,
            inst_ownership,
            fcf_quality,
            canslim_score,
            market_cap
        FROM stock_fundamentals
        WHERE market_cap BETWEEN {MIN_MARKET_CAP} AND {MAX_MARKET_CAP}
          AND inst_ownership >= {MIN_INST_OWN}
    """
    fundamentals_df = con.execute(found_sql).df()
    final_df = pd.merge(candidates_df, fundamentals_df, on='stock_code', how='left')
    con.close()
    return final_df


# ===================== 配置 =====================
# 填写你当前持仓或重点观察的股票
CURRENT_SELECTED_TICKERS = ["CDE"]
# CURRENT_SELECTED_TICKERS = []
# ===============================================

def main():
    # 1️⃣ State 1: A, Finnhub ticker
    # 首次执行时解开注释执行，以后每天轮动不用再执行
    # ticker_df = fetch_us_tickers()
    # upsert_stock_tickers(ticker_df)

    # 2️⃣ State 1: B, yfinance 批量加载所有1800左右流动股的价格
    # 首次执行时解开注释执行，以后每天轮动不用再执行
    # fetch_all_prices()

    # 3️⃣ State 1: C, 每天只需更新最新的股票价格数据即可
    print(f"🚀 Stage 1: 更新最新的股票价格数据")
    update_recent_prices(CURRENT_SELECTED_TICKERS)
    
    latest_date_in_db = get_latest_date_in_db()
    target_date_str = latest_date_in_db.strftime('%Y-%m-%d')

    # 4️⃣ 技术面初步筛选
    print(f"🚀 Step 1: 正在筛选 {target_date_str} 符合突破回踩形态的股票...")
    stage2_df = filter_dip_stocks_from_db(target_date_str)
    
    if stage2_df.empty:
        print("❌ 未发现符合条件的股票。")
        return

    # 5️⃣ 针对候选股更新基本面 (仅更新这几只，速度极快)
    print(f"🚀 Step 2: 更新 {len(stage2_df)} 只候选股的基本面及子项...")
    update_fundamentals(stage2_df['stock_code'].tolist(), force_update=False)
    
    # 6️⃣ 计算 RS Rank 
    print("🚀 Step 3: 计算全市场 RS Rank...")
    # 计算 RS Rank
    candidates_df = calculate_rs_rank_for_candidates(stage2_df, target_date_str)
    
    # 7️⃣ 关联基本面分数
    print("🚀 Step 4: 关联基本面分数...")
    final_df = link_fundamental_data(candidates_df)

    # 8️⃣ 波动模拟 (VIX 调节)
    print("\n🔍 正在获取市场 VIX 数据以调节波动区间...")
    try:
        vix_df = yf.download("^VIX", period="1d", progress=False, proxy=PROXIES["http"])
        # 获取最新 VIX 收盘价，若失败则取默认值 18.0
        current_vix = vix_df['Close'].iloc[-1] if not vix_df.empty else 18.0
        if isinstance(current_vix, pd.Series): current_vix = current_vix.iloc[0]
        print(f"当前 VIX 指数: {current_vix:.2f} (调节系数: {max(1.0, 1+(current_vix-18)*0.05):.2f}x)")
    except Exception as e:
        print(f"VIX 获取失败，使用基准值: {e}")
        current_vix = 18.0

    # 注入回撤模拟数据
    print("🛠️ 正在计算个股波动容错区间...")
    pullback_list = []
    for ticker in final_df['stock_code']:
        p_data = simulate_pullback_range(ticker, current_vix=current_vix)
        pullback_list.append(p_data if p_data else {})
    
    # 合并模拟结果
    pullback_df = pd.DataFrame(pullback_list)
    final_with_sim = pd.concat([final_df.reset_index(drop=True), pullback_df], axis=1)

    # 计算建议止盈位（以支撑位为基准的 3:1 盈亏比，或简单的 20% 目标）
    final_with_sim['target_profit'] = (final_with_sim['support_price'] * 1.20).round(2)

    # 2. 统一保留两位小数
    # 自动识别 DataFrame 中存在的浮点数列并取2位小数
    final_with_sim = final_with_sim.round(2)

    # 3. 定义必须非空的字段并剔除包含 NaN 的行
    # 包含的字段：quarterly_eps_growth, annual_eps_growth, revenue_growth, roe, shares_outstanding, inst_ownership, canslim_score
    critical_fundamental_cols = [
        'quarterly_eps_growth', 'annual_eps_growth', 'revenue_growth', 
        'roe', 'shares_outstanding', 'inst_ownership', "fcf_quality", 'canslim_score'
    ]
    # 4. 剔除基本面分数为 NaN 的股票
    # 这会过滤掉那些 yfinance 无法获取财务数据或不符合初步基本面条件的股票
    before_count = len(final_with_sim)
    final_with_sim = final_with_sim.dropna(subset=critical_fundamental_cols)
    after_count = len(final_with_sim)
    print(f"🧹 已剔除基本面数据不全的股票: {before_count - after_count} 只")

    # 5. 确保日期格式美化（可选，防止 Excel 里显示长字符串）
    if 'trade_date' in final_with_sim.columns:
        final_with_sim['trade_date'] = pd.to_datetime(final_with_sim['trade_date']).dt.strftime('%Y-%m-%d')
    if 'support_date' in final_with_sim.columns:
        final_with_sim['support_date'] = pd.to_datetime(final_with_sim['support_date']).dt.strftime('%Y-%m-%d')

    # 9️⃣ 打印输出
    display_cols = [
        "stock_code",
        "support_date", "support_price",
        "breakthrough_date", "breakthrough_price",
        "dip_date", "dip_price",
        "rs_rank",
        "hard_stop", "target_profit", "ideal_entry",
        "canslim_score",
        "quarterly_eps_growth", "annual_eps_growth",
        "revenue_growth", "roe",
        "shares_outstanding", "inst_ownership", "fcf_quality"
    ]
    
    # 过滤掉不存在的列以防报错
    actual_cols = [c for c in display_cols if c in final_with_sim.columns]
    print(final_with_sim[actual_cols].to_string(index=False))

    # 保存结果
    if not final_with_sim.empty:
        file_name_xlsx = f"fourty_days_breakthrough_with_dip_{datetime.now():%Y%m%d}.xlsx"
        try:
            final_with_sim[actual_cols].to_excel(file_name_xlsx, index=False, engine='openpyxl')
            print(f"\n📊 详细策略报告已生成 Excel: {file_name_xlsx}")
        except Exception as e:
            print(f"❌ Excel 生成失败 (请检查是否安装 openpyxl): {e}")
            # 备选保存为 CSV
            file_name_csv = file_name_xlsx.replace(".xlsx", ".csv")
            final_with_sim.to_csv(file_name_csv, index=False, encoding="utf-8-sig")
            print(f"\n📊 详细策略报告已生成: {file_name_csv}")
    else:
        print("⚠️ 经过基本面严格筛选后，没有符合条件的股票。")

if __name__ == "__main__":
    main()
