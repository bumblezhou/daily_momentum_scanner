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

    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_volume_trend (
            stock_code VARCHAR,
            trade_date DATE,
            obv DOUBLE,
            obv_ma20 DOUBLE,
            obv_slope_20 DOUBLE,
            obv_high_60 BOOLEAN,
            ad DOUBLE,
            ad_slope_20 DOUBLE,
            vol20 DOUBLE,
            vol_rs DOUBLE
        )
    """);
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
        SELECT
            t.symbol
        FROM stock_ticker t
        WHERE
            t.type = 'Common Stock'
            AND t.mic IN ('XNYS', 'XNGS', 'XNAS', 'XASE', 'ARCX', 'BATS', 'IEXG')
            AND COALESCE(t.yf_price_available, TRUE) = TRUE;
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
def get_tickers_missing_recent_data(trading_days):
    """
    返回尚未更新到最近一个交易日的 ticker 列表
    """
    latest_trading_day = trading_days[-1]

    con = duckdb.connect(DUCKDB_PATH)

    query = f"""
        SELECT t.symbol
        FROM stock_ticker t
        LEFT JOIN (
            SELECT
                stock_code,
                MAX(trade_date) AS last_trade_date
            FROM stock_price
            GROUP BY stock_code
        ) p
        ON p.stock_code = t.symbol
        WHERE
            t.type = 'Common Stock'
            AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
            AND COALESCE(t.yf_price_available, TRUE) = TRUE
            AND (
                p.last_trade_date IS NULL
                OR p.last_trade_date < DATE '{latest_trading_day}'
            )
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
    raw_tickers = get_tickers_missing_recent_data(trading_days)
    if watchlist:
        # 合并自选列表
        raw_tickers = list(set(raw_tickers) | set(watchlist))

    if not raw_tickers:
        print(f"✅ 数据库已是最新（美东 {target_date} 已对齐），跳过更新")
        return

    yahoo_map = {t: finnhub_to_yahoo(t) for t in raw_tickers}
    yahoo_tickers = list(yahoo_map.values())

    print(f"需要更新 {len(yahoo_tickers)} 只股票")

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


# ============================================================
# Stage 2：SwingTrend 技术筛选（全部在 DuckDB 内完成）
# ============================================================

def build_stage2_swingtrend_old(con, target_date: date, monitor_list: list = []) -> pd.DataFrame:
    # 将列表转换为 SQL 字符串格式 ('AAPL', 'TSLA')
    monitor_str = ", ".join([f"'{t}'" for t in monitor_list]) if monitor_list else "''"

    sql = f"""
    /* ======================================================
       Stage 2 – SwingTrend 技术筛选
       所有参数均可根据注释位置自行调整
       ====================================================== */

    WITH base AS (
        SELECT
            p.stock_code,
            p.trade_date,
            p.close,
            p.high,
            p.low,
            p.volume,
            t.sector,
            /* ===== 均线参数（可调） ===== */
            AVG(p.close) OVER w10  AS ma10,    -- 短线持仓用
            AVG(p.close) OVER w20  AS ma20,    -- 新增：用于止损和VCP
            AVG(p.close) OVER w50  AS ma50,
            AVG(p.close) OVER w150 AS ma150,
            AVG(p.close) OVER w200 AS ma200,
            /* ===== 52 周高低点窗口（252 日） ===== */
            MAX(p.high) OVER w252 AS high_52w,  -- 修正：实战中多用 high
            MIN(p.low) OVER w252 AS low_52w,    -- 修正：实战中多用 low

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price p
        LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
        WINDOW
            w10  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 9 PRECEDING),
            w20  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 19 PRECEDING),
            w50  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 49 PRECEDING),
            w150 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 149 PRECEDING),
            w200 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 199 PRECEDING),
            w252 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 251 PRECEDING),
            w_all AS (PARTITION BY p.stock_code)
    ),

    /* ===== RS Rank 计算（Minervini 权重） ===== */
    returns AS (
        SELECT
            stock_code,
            trade_date,

            /* 对上市不足一年的股票，自动使用可得周期并年化 */
            POWER(
                close / NULLIF(
                    LAG(close, LEAST(trading_days - 1, 252))
                    OVER (PARTITION BY stock_code ORDER BY trade_date),
                0),
                252.0 / NULLIF(LEAST(trading_days - 1, 252), 0)
            ) - 1 AS r1y,

            close / NULLIF(LAG(close,126) OVER w, close) - 1 AS r6m,
            close / NULLIF(LAG(close,63)  OVER w, close) - 1 AS r3m,
            close / NULLIF(LAG(close,21)  OVER w, close) - 1 AS r1m
        FROM base
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    rs_scores AS (
        SELECT
            stock_code,
            trade_date,

            /* 🔥🔥 核心修正：使用 COALESCE 防止 NULL 传染
               如果数据不足导致 r6m 为空，则视为 0，保证 rs_score 能算出来
            */
            (
                0.4 * COALESCE(r1y, 0) + 
                0.3 * COALESCE(r6m, 0) + 
                0.2 * COALESCE(r3m, 0) + 
                0.1 * COALESCE(r1m, 0)
            ) AS rs_score,

            /* 计算排名 */
            PERCENT_RANK() OVER (
                PARTITION BY trade_date
                ORDER BY (
                    0.4 * COALESCE(r1y, 0) + 
                    0.3 * COALESCE(r6m, 0) + 
                    0.2 * COALESCE(r3m, 0) + 
                    0.1 * COALESCE(r1m, 0)
                )
            ) * 100 AS rs_rank
        FROM returns
    ),

    rs_averages AS (  -- 新 CTE: 计算 rs_20 使用预计算的 rs_score（无嵌套）
        SELECT
            *,
            /* 新增：RS 变化率 - 过去20日RS均值 */
            AVG(rs_score) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 19 PRECEDING
            ) AS rs_20
        FROM rs_scores
    ),

    rs_ranked AS (  -- 最终 CTE: 计算 lagged 值使用预计算的 rs_20（无嵌套）
        SELECT
            *,
            /* 10日前 RS_20 */
            LAG(rs_20, 10) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
            ) AS rs_20_10days_ago
        FROM rs_averages
    ),

    /* ===== ATR（VCP 波动收缩） ===== */
    atr_raw AS (
        SELECT
            stock_code,
            trade_date,
            GREATEST(
                high - low,
                ABS(high - LAG(close) OVER w),
                ABS(low  - LAG(close) OVER w)
            ) AS tr
        FROM stock_price
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    atr_10day_avg AS (
        SELECT
            stock_code,
            trade_date,
            AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 9 PRECEDING) AS atr10_recent
        FROM atr_raw
    ),

    atr_stats AS (
        SELECT
            a.stock_code,
            a.trade_date,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 4 PRECEDING)  AS atr5,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 19 PRECEDING) AS atr20,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 14 PRECEDING) AS atr15,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 59 PRECEDING) AS atr60,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 9 PRECEDING)  AS atr10,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 49 PRECEDING) AS atr50,
            (avg10.atr10_recent - LAG(avg10.atr10_recent, 10) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date)) / 10 AS atr_slope
        FROM atr_raw a
        JOIN atr_10day_avg avg10 USING (stock_code, trade_date)
    ),

    /* ===== Pivot（最近 40 日高点） ===== */
    /* 修正点：重命名 CTE 为 pivot_data 避免关键字冲突 */
    pivot_data AS (
        SELECT
            stock_code,
            trade_date,
            /* 修正点：取昨日起算的过去20日最高价，作为今天的压力位 */
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS pivot_price
        FROM stock_price
    ),

    /* ===== 成交量确认 ===== */
    volume_check AS (
        SELECT
            stock_code,
            trade_date,
            volume,
            AVG(volume) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 19 PRECEDING  -- 修改为20日均量
            ) AS vol20,
            AVG(volume) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 49 PRECEDING
            ) AS vol50
        FROM stock_price
    ),

    /* ===== 前5日最高价 ===== */
    prev_high AS (
        SELECT
            stock_code,
            trade_date,
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS high_5d
        FROM stock_price
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        b.sector,
        r.rs_rank,
        b.ma10, b.ma20, b.ma50, b.ma150, b.ma200,
        b.high_52w, b.low_52w,
        a.atr5, a.atr20, a.atr15, a.atr60, a.atr10, a.atr50,
        a.atr_slope,
        r.rs_20, r.rs_20_10days_ago,
        p.pivot_price,
        v.volume, v.vol20, v.vol50,
        ph.high_5d

    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN atr_stats a USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    JOIN volume_check v USING (stock_code, trade_date)
    JOIN prev_high ph USING (stock_code, trade_date)

    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            (
                /* ===== 1. 基础结构：即使去掉了均线，也要保证不是垃圾股 ===== */
                /* 均线排列参数标准：保守：close > ma50 > ma150 > ma200， 标准：close > ma150 AND ma50 > ma150 > ma200， 激进：close > ma50 AND ma50 > ma200 */
                b.close > b.ma150
                AND b.ma50  > b.ma150
                AND b.ma150 > b.ma200
                /* 距离 52 周低点：保守：close >= 1.5 * low_52w， 标准： close >= 1.25 * low_52w， 激进： close >= 1.15 * low_52w */
                AND b.close >= 1.25 * b.low_52w   -- 距 52 周低点至少 +25%
                AND b.close >= 0.75 * b.high_52w  -- 距 52 周高点不超过 -25%

                /* 均线斜率参数标准：保守： ma150 > LAG(ma150, 20)， 标准： ma150 >= LAG(ma150, 20)， 激进： 不要求 */

                /* ===== 2. RS 强度：保留，这是核心，但稍微放宽排名 ===== */
                /* RS Rank（全市场）参数标准：保守：rs_rank >= 80，标准：rs_rank >= 70，激进：rs_rank >= 60，严禁低于 55（55 以下长期统计期望≈0）*/
                AND ((r.rs_rank >= 75) OR (b.sector = 'Technology' AND r.rs_rank >= 65))  -- 保证强者恒强，允许科技、医疗、消费周期股稍微低一点
                /* 注释掉苛刻的RS加速要求，允许RS走平。收紧到 0.95，不能明显走弱 */
                /* RS 持续性参数标准： 保守：rs_20 > rs_20_10days_ago， 标准：rs_20 > rs_20_10days_ago * 0.95， 激进：不强制，但不允许明显下行 */
                AND r.rs_20 > r.rs_20_10days_ago * 0.95

                /* ===== 3. VCP 形态：放宽波动收缩阈值 ===== */
                /* 将 0.8 放宽到 0.95，只要近期没有剧烈波动即可。 */
                /* ATR 收缩强度参数标准： 保守：atr5 / atr20 < 0.85， 标准：atr5 / atr20 < 0.95， 激进：atr5 / atr20 < 1.0 */
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 0.95 
                
                /* 注释掉 slope < 0，有时候震荡期 slope 是平的 */
                -- AND a.atr_slope < 0
                
                /* 保留：短期比长期稳定 */
                /* 长短波动对比参数对比： 保守： atr5 < atr20 AND atr15 < atr60， 标准： atr15 < atr60， 激进： atr15 <= atr60 * 1.05 */
                AND a.atr15 < a.atr60

                /* ===== 4. 关键修改：移除“当天必须爆发”的条件 ===== */
                /* 我们要找的是“准备好”的股票，而不是“已经涨完”的股票 */
                /* AND (
                    b.close > ph.high_5d
                    OR b.close > p.pivot_price
                )
                AND v.volume >= 1.2 * v.vol20 
                */
                
                /* 替代方案：只要成交量不要已经枯竭到死寂即可，或者完全不限 */
                /* 成交量参数标准： 保守： vol20 > 1_000_000， 标准： vol20 > 300_000， 激进： vol20 > 150_000 */
                AND v.vol20 > 500000

                /* 市值参数标准： 保守： market_cap > 5e9， 标准： market_cap > 1e9， 激进： market_cap > 不强制 */
                AND EXISTS (
                    SELECT 1 FROM stock_fundamentals f
                    WHERE f.stock_code = b.stock_code AND f.market_cap >= 1e9
                )
            )
            OR
            b.stock_code IN ({monitor_str})
        )

    """

    df = con.execute(sql).df()
    return df


def build_stage2_swingtrend(con, target_date: date, monitor_list: list = [], market_regime: str = "多头") -> pd.DataFrame:
    if market_regime == "多头":
        market_filter_sql = """
            /* =====================================================
            多头市场（Bull Regime / 进攻模式）
            核心目标：
            - 接受“不完美结构”
            - 优先捕捉趋势扩散，而非极致收缩
            - RS 权重 > 均线完美度
            ===================================================== */
            (
                /* ===== 1. 基础结构（放宽） =====
                要求仍然站在中长期趋势之上，但不苛求完美排列
                close > ma50 > ma200 即可
                */
                b.close > b.ma50
                AND b.ma50 > b.ma200

                /* ===== 2. 52 周结构（放宽） =====
                允许更早期的 Stage 2
                */
                AND b.close >= 1.15 * b.low_52w     -- 距 52 周低点 ≥ +15%
                AND b.close >= 0.70 * b.high_52w    -- 距 52 周高点 ≥ -30%

                /* ===== 3. RS 强度（收紧） =====
                多头市中，强者更强是核心假设
                */
                AND ((r.rs_rank >= 75) OR (b.sector = 'Technology' AND r.rs_rank >= 65))

                /* RS 不能明显走弱，允许横盘 */
                AND r.rs_20 >= r.rs_20_10days_ago * 0.90

                /* ===== 4. 波动结构（允许轻微扩散） =====
                不强制典型 VCP，只要不是失控即可
                */
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 1.00
                AND a.atr15 <= a.atr60 * 1.05

                /* ===== 5. 成交量底线 =====
                只排除流动性明显不足的股票
                */
                AND v.vol20 > 300000

                /* ===== 6. 市值过滤 =====
                多头市允许中等市值参与趋势扩散
                */
                AND EXISTS (
                    SELECT 1 FROM stock_fundamentals f
                    WHERE f.stock_code = b.stock_code
                    AND f.market_cap >= 1e9
                )
            )
        """
    else:
        market_filter_sql = """
            /* =====================================================
            非多头市场（Neutral / Defensive Regime）
            核心目标：
            - 只做结构完整 + 高质量股票
            - 优先防守，其次等待收敛后的确定性
            ===================================================== */
            (
                /* ===== 1. 均线结构（严格） =====
                必须是标准 Stage 2 形态
                */
                b.close > b.ma150
                AND b.ma50  > b.ma150
                AND b.ma150 > b.ma200

                /* ===== 2. 52 周结构（收紧） =====
                必须远离底部，接近新高
                */
                AND b.close >= 1.30 * b.low_52w     -- 距 52 周低点 ≥ +30%
                AND b.close >= 0.80 * b.high_52w    -- 距 52 周高点 ≥ -20%

                /* ===== 3. RS 强度（收紧） =====
                非多头市只做真正的领涨股
                */
                AND ((r.rs_rank >= 75) OR (b.sector = 'Technology' AND r.rs_rank >= 65))

                /* RS 必须维持上行 */
                AND r.rs_20 > r.rs_20_10days_ago

                /* ===== 4. 波动结构（必须收缩） =====
                典型 VCP / Base 形态
                */
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 0.90
                AND a.atr15 < a.atr60

                /* ===== 5. 成交量要求（提高） =====
                防止震荡市中被流动性杀伤
                */
                AND v.vol20 > 700000

                /* ===== 6. 市值过滤（提高） =====
                只做抗风险能力更强的中大盘股
                */
                AND EXISTS (
                    SELECT 1 FROM stock_fundamentals f
                    WHERE f.stock_code = b.stock_code
                    AND f.market_cap >= 3e9
                )
            )
        """
    # 将列表转换为 SQL 字符串格式 ('AAPL', 'TSLA')
    monitor_str = ", ".join([f"'{t}'" for t in monitor_list]) if monitor_list else "''"

    sql = f"""
    /* ======================================================
       Stage 2 – SwingTrend 技术筛选
       所有参数均可根据注释位置自行调整
       ====================================================== */

    WITH base AS (
        SELECT
            p.stock_code,
            p.trade_date,
            p.close,
            p.high,
            p.low,
            p.volume,
            t.sector,
            /* ===== 均线参数（可调） ===== */
            AVG(p.close) OVER w10  AS ma10,    -- 短线持仓用
            AVG(p.close) OVER w20  AS ma20,    -- 新增：用于止损和VCP
            AVG(p.close) OVER w50  AS ma50,
            AVG(p.close) OVER w150 AS ma150,
            AVG(p.close) OVER w200 AS ma200,
            /* ===== 52 周高低点窗口（252 日） ===== */
            MAX(p.high) OVER w252 AS high_52w,  -- 修正：实战中多用 high
            MIN(p.low) OVER w252 AS low_52w,    -- 修正：实战中多用 low

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price p
        LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
        WINDOW
            w10  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 9 PRECEDING),
            w20  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 19 PRECEDING),
            w50  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 49 PRECEDING),
            w150 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 149 PRECEDING),
            w200 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 199 PRECEDING),
            w252 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 251 PRECEDING),
            w_all AS (PARTITION BY p.stock_code)
    ),

    /* ===== RS Rank 计算（Minervini 权重） ===== */
    returns AS (
        SELECT
            stock_code,
            trade_date,

            /* 对上市不足一年的股票，自动使用可得周期并年化 */
            POWER(
                close / NULLIF(
                    LAG(close, LEAST(trading_days - 1, 252))
                    OVER (PARTITION BY stock_code ORDER BY trade_date),
                0),
                252.0 / NULLIF(LEAST(trading_days - 1, 252), 0)
            ) - 1 AS r1y,

            close / NULLIF(LAG(close,126) OVER w, close) - 1 AS r6m,
            close / NULLIF(LAG(close,63)  OVER w, close) - 1 AS r3m,
            close / NULLIF(LAG(close,21)  OVER w, close) - 1 AS r1m
        FROM base
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    rs_scores AS (
        SELECT
            stock_code,
            trade_date,

            /* 🔥🔥 核心修正：使用 COALESCE 防止 NULL 传染
               如果数据不足导致 r6m 为空，则视为 0，保证 rs_score 能算出来
            */
            (
                0.4 * COALESCE(r1y, 0) + 
                0.3 * COALESCE(r6m, 0) + 
                0.2 * COALESCE(r3m, 0) + 
                0.1 * COALESCE(r1m, 0)
            ) AS rs_score,

            /* 计算排名 */
            PERCENT_RANK() OVER (
                PARTITION BY trade_date
                ORDER BY (
                    0.4 * COALESCE(r1y, 0) + 
                    0.3 * COALESCE(r6m, 0) + 
                    0.2 * COALESCE(r3m, 0) + 
                    0.1 * COALESCE(r1m, 0)
                )
            ) * 100 AS rs_rank
        FROM returns
    ),

    rs_averages AS (  -- 新 CTE: 计算 rs_20 使用预计算的 rs_score（无嵌套）
        SELECT
            *,
            /* 新增：RS 变化率 - 过去20日RS均值 */
            AVG(rs_score) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 19 PRECEDING
            ) AS rs_20
        FROM rs_scores
    ),

    rs_ranked AS (  -- 最终 CTE: 计算 lagged 值使用预计算的 rs_20（无嵌套）
        SELECT
            *,
            /* 10日前 RS_20 */
            LAG(rs_20, 10) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
            ) AS rs_20_10days_ago
        FROM rs_averages
    ),

    /* ===== ATR（VCP 波动收缩） ===== */
    atr_raw AS (
        SELECT
            stock_code,
            trade_date,
            GREATEST(
                high - low,
                ABS(high - LAG(close) OVER w),
                ABS(low  - LAG(close) OVER w)
            ) AS tr
        FROM stock_price
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    atr_10day_avg AS (
        SELECT
            stock_code,
            trade_date,
            AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 9 PRECEDING) AS atr10_recent
        FROM atr_raw
    ),

    atr_stats AS (
        SELECT
            a.stock_code,
            a.trade_date,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 4 PRECEDING)  AS atr5,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 19 PRECEDING) AS atr20,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 14 PRECEDING) AS atr15,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 59 PRECEDING) AS atr60,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 9 PRECEDING)  AS atr10,
            AVG(tr) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date ROWS 49 PRECEDING) AS atr50,
            (avg10.atr10_recent - LAG(avg10.atr10_recent, 10) OVER (PARTITION BY a.stock_code ORDER BY a.trade_date)) / 10 AS atr_slope
        FROM atr_raw a
        JOIN atr_10day_avg avg10 USING (stock_code, trade_date)
    ),

    /* ===== Pivot（最近 40 日高点） ===== */
    /* 修正点：重命名 CTE 为 pivot_data 避免关键字冲突 */
    pivot_data AS (
        SELECT
            stock_code,
            trade_date,
            /* 修正点：取昨日起算的过去20日最高价，作为今天的压力位 */
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS pivot_price
        FROM stock_price
    ),

    /* ===== 成交量确认 ===== */
    volume_check AS (
        SELECT
            stock_code,
            trade_date,
            volume,
            AVG(volume) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 19 PRECEDING  -- 修改为20日均量
            ) AS vol20,
            AVG(volume) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 49 PRECEDING
            ) AS vol50
        FROM stock_price
    ),

    /* ===== 前5日最高价 ===== */
    prev_high AS (
        SELECT
            stock_code,
            trade_date,
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS high_5d
        FROM stock_price
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        b.sector,
        r.rs_rank,
        b.ma10, b.ma20, b.ma50, b.ma150, b.ma200,
        b.high_52w, b.low_52w,
        a.atr5, a.atr20, a.atr15, a.atr60, a.atr10, a.atr50,
        a.atr_slope,
        r.rs_20, r.rs_20_10days_ago,
        p.pivot_price,
        v.volume, v.vol20, v.vol50,
        ph.high_5d

    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN atr_stats a USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    JOIN volume_check v USING (stock_code, trade_date)
    JOIN prev_high ph USING (stock_code, trade_date)

    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            (
                {market_filter_sql}
            )
            OR
            b.stock_code IN ({monitor_str})
        )

    """

    df = con.execute(sql).df()
    return df


def build_stage3_fundamental_fast(con, stage2_df: pd.DataFrame) -> pd.DataFrame:
    """
    从本地 DuckDB 直接获取基本面评分 (极速版)
    """
    if stage2_df.empty:
        return pd.DataFrame()

    # 将 Stage 2 的结果注册为临时表，方便与基本面表 JOIN
    con.register("tmp_stage2", stage2_df)

    # 💡 核心修正：只选择基本面相关的列 + 关联主键
    sql = """
        SELECT 
            f.stock_code,
            f.canslim_score,
            f.quarterly_eps_growth,
            f.annual_eps_growth,
            f.roe,
            f.revenue_growth,
            f.fcf_quality,
            f.shares_outstanding,
            f.inst_ownership,
            f.market_cap
        FROM stock_fundamentals f
        WHERE f.stock_code IN (SELECT stock_code FROM tmp_stage2) AND f.fcf_quality IS NOT NULL AND f.roe IS NOT NULL
    """
    
    result_df = con.execute(sql).df()
    return result_df


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
def update_fundamentals(con, ticker_list, force_update=False):
    """
    定期更新基本面数据，包括 CAN SLIM 特定指标
    force_update: 是否强制更新所有股票，否则只更新过期数据
    """

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

            # 标准行业分类参考：
            # "Technology"
            # "Healthcare"
            # "Financial Services"
            # "Energy"
            # "Basic Materials"
            # "Industrials"
            # "Consumer Cyclical"
            # "Consumer Defensive"
            # "Communication Services"
            # "Utilities"
            # "Real Estate"
            
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

        except Exception as e:
            print(f"  [ERR] {symbol} 更新失败: {e}")
            continue


def get_latest_date_in_db():
    con = duckdb.connect(DUCKDB_PATH)
    latest_date_in_db = con.execute("SELECT MAX(trade_date) FROM stock_price").fetchone()[0]
    con.close()
    return latest_date_in_db


# ==================== 新增：回撤深度与波动模拟函数（修复版）===================
def simulate_pullback_range(con, stock_code, current_vix=18.0):
    """
    基于 ATR、历史回撤及 VIX 动态调节因子模拟入场区间与硬止损
    :param stock_code: 股票代码
    :param current_vix: 当前市场 VIX 指数，默认 18.0 (基准均值)
    """
    
    # 直接在 SQL 中计算所需的 ma20 和 pivot_price（因为原始表没有这些列）
    sql = f"""
        SELECT 
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            AVG(close) OVER (ORDER BY trade_date ROWS 19 PRECEDING) AS ma20,
            MAX(high) OVER (ORDER BY trade_date ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING) AS pivot_price
        FROM stock_price 
        WHERE stock_code = '{stock_code}' 
        ORDER BY trade_date DESC 
        LIMIT 30  -- 多取几条确保窗口计算完整
    """
    try:
        df = con.execute(sql).df().sort_values('trade_date')  # 按时间升序方便计算 ATR
        if len(df) < 20:
            print(f"⚠️ {stock_code} 数据不足20条，无法计算波动区间")
            return {}
    except Exception as e:
        print(f"提取 {stock_code} 波动数据失败: {e}")
        return {}

    # --- A. 计算 15日 ATR (真实波幅) ---
    high_low = df['high'] - df['low']
    high_prev_close = (df['high'] - df['close'].shift(1)).abs()
    low_prev_close = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    atr_15 = tr.tail(15).mean()

    # --- B. 计算 VIX 调节因子 ---
    vix_factor = 1.0
    if current_vix > 18:
        vix_factor = 1 + (current_vix - 18) * 0.05
        vix_factor = min(vix_factor, 1.8)  # 最高不超过 1.8 倍

    current_price = df['close'].iloc[-1]
    pivot_price = df['pivot_price'].iloc[-1]
    ma20 = df['ma20'].iloc[-1]

    # --- C. 计算动态水位 ---
    pullback_dist = atr_15 * 0.6 * vix_factor
    entry_low = current_price - pullback_dist
    entry_high = current_price * 0.99

    hard_stop = current_price - atr_15 * 1.5 * vix_factor

    # 失败模式止损：取 pivot-7%、ma20、hard_stop 中的最小值
    stop_pivot = pivot_price * 0.93 if pivot_price > 0 else float('inf')
    stop_ma20 = ma20 if ma20 > 0 else float('inf')
    failure_stop = min(stop_pivot, stop_ma20, hard_stop)

    return {
        'ideal_entry': f"{entry_low:.2f} - {entry_high:.2f}",
        'hard_stop': round(hard_stop, 2),
        'failure_stop': round(failure_stop, 2),
        'atr_15': round(atr_15, 2),
        'vix_adj': round(vix_factor, 2)
    }


def check_market_regime(con) -> dict:
    """
    检查市场整体形态（Market Regime）
    规则：
    - SPY > MA200
    - QQQ > MA50

    返回：
    {
        "is_bull": bool,
        "spy_close": float,
        "spy_ma200": float,
        "qqq_close": float,
        "qqq_ma50": float
    }
    """

    spy_sql = """
        SELECT close, ma200
        FROM (
            SELECT 
                close,
                AVG(close) OVER (ORDER BY trade_date ROWS 199 PRECEDING) AS ma200
            FROM stock_price
            WHERE stock_code = 'SPY'
            ORDER BY trade_date DESC
            LIMIT 1
        )
    """

    qqq_sql = """
        SELECT close, ma50
        FROM (
            SELECT 
                close,
                AVG(close) OVER (ORDER BY trade_date ROWS 49 PRECEDING) AS ma50
            FROM stock_price
            WHERE stock_code = 'QQQ'
            ORDER BY trade_date DESC
            LIMIT 1
        )
    """

    spy_df = con.execute(spy_sql).df()
    qqq_df = con.execute(qqq_sql).df()

    if spy_df.empty or qqq_df.empty:
        return {
            "is_bull": False,
            "reason": "SPY 或 QQQ 数据缺失"
        }

    spy_close = spy_df['close'].iloc[0]
    spy_ma200 = spy_df['ma200'].iloc[0]

    qqq_close = qqq_df['close'].iloc[0]
    qqq_ma50 = qqq_df['ma50'].iloc[0]

    is_bull = (spy_close > spy_ma200) and (qqq_close > qqq_ma50)

    return {
        "is_bull": is_bull,
        "spy_close": spy_close,
        "spy_ma200": spy_ma200,
        "qqq_close": qqq_close,
        "qqq_ma50": qqq_ma50
    }


def build_stage2_with_volume_trend(con, stage2_df: pd.DataFrame) -> pd.DataFrame:
    """
    在不修改 Stage2 原有逻辑的前提下，
    为结果外挂 OBV / AD 量价趋势特征
    """
    if stage2_df.empty:
        return stage2_df

    con.register("tmp_stage2", stage2_df)

    sql = """
    SELECT
        s.*,
        v.obv,
        v.obv_ma20,
        v.obv_slope_20,
        v.ad,
        v.ad_slope_20,
        v.vol20,
        v.vol_rs
    FROM tmp_stage2 s
    LEFT JOIN stock_volume_trend v
        ON s.stock_code = v.stock_code
        AND s.trade_date = v.trade_date
    """

    return con.execute(sql).df()


def update_volume_trend_features(con, latest_trading_day: str):
    con.execute(f"""
        DELETE FROM stock_volume_trend
        WHERE trade_date = DATE '{latest_trading_day}'
    """)

    sql = f"""
    INSERT INTO stock_volume_trend
    WITH base AS (
        SELECT
            s.stock_code,
            s.trade_date,
            s.volume,
            s.close,
            s.high,
            s.low,

            /* ===== OBV delta ===== */
            CASE
                WHEN s.close > LAG(s.close) OVER w THEN s.volume
                WHEN s.close < LAG(s.close) OVER w THEN -s.volume
                ELSE 0
            END AS obv_delta,

            /* ===== AD delta ===== */
            ((s.close - s.low) - (s.high - s.close))
                / NULLIF(s.high - s.low, 0)
                * s.volume AS ad_delta

        FROM stock_price s
        LEFT JOIN stock_ticker t ON s.stock_code = t.symbol
        WHERE
            t.type = 'Common Stock'
            AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
            AND COALESCE(t.yf_price_available, TRUE) = TRUE
        WINDOW w AS (PARTITION BY s.stock_code ORDER BY s.trade_date)
    ),

    obv_ad AS (
        SELECT
            *,
            /* ===== OBV ===== */
            SUM(obv_delta) OVER w AS obv,

            /* ===== AD ===== */
            SUM(ad_delta) OVER w AS ad
        FROM base
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    trends AS (          -- 新CTE：在全历史上计算窗口
        SELECT
            stock_code,
            trade_date,
            obv,
            AVG(obv) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING) AS obv_ma20,
            (obv - LAG(obv, 20) OVER (PARTITION BY stock_code ORDER BY trade_date)) / 20 AS obv_slope_20,
            obv >= MAX(obv) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 59 PRECEDING) AS obv_high_60,
            ad,
            (ad - LAG(ad, 20) OVER (PARTITION BY stock_code ORDER BY trade_date)) / 20 AS ad_slope_20,
            AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING) AS vol20,
            volume / NULLIF(AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING), 0) AS vol_rs
        FROM obv_ad
    )

    SELECT * FROM trends
    WHERE trade_date = DATE '{latest_trading_day}'
    WINDOW
        w AS (PARTITION BY stock_code ORDER BY trade_date),
        w20 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING),
        w60 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 59 PRECEDING)
    """
    con.execute(sql)


# Define a function to classify based on slopes
def classify_obv_ad_enhanced(
    obv_slope,
    ad_slope,
    vol20=None,
    vix=None,
    eps_ratio=0.05
):
    """
    增强版 OBV + AD 量价结构解释器
    - obv_slope: On-Balance Volume 斜率 （数值型）
    - ad_slope: Accumulation/Distribution 斜率 （数值型）
    - vol20: 20日均量（可选，用于后续扩展）
    - vix: 当前 VIX 指数（可选，用于动态调整阈值）
    - eps_ratio: 噪声阈值比例，默认 5% (可调)
    返回分类标签字符串
    """

    if pd.isna(obv_slope) or pd.isna(ad_slope):
        return "未分类"

    # === 动态噪声阈值 ===
    # VIX 高 → 放宽中性区
    if vix is not None and vix > 18:
        eps_ratio = 0.10

    eps_obv = abs(obv_slope) * eps_ratio
    eps_ad  = abs(ad_slope) * eps_ratio

    def trend(v, eps):
        if v > eps:
            return '↑'
        elif v < -eps:
            return '↓'
        else:
            return '→'

    obv_trend = trend(obv_slope, eps_obv)
    ad_trend  = trend(ad_slope, eps_ad)

    # ===== 强趋势 =====
    if obv_trend == '↑' and ad_trend == '↑':
        return "明确吸筹(最强)"
    if obv_trend == '↓' and ad_trend == '↓':
        return "趋势结束(最弱)"

    # ===== 资金分歧 =====
    if obv_trend == '↑' and ad_trend == '↓':
        return "资金分歧(内强外弱)"
    if obv_trend == '↓' and ad_trend == '↑':
        return "价格拉升但量未确认"

    # ===== 震荡 / 过渡 =====
    if obv_trend == '↑' and ad_trend == '→':
        return "洗盘(震荡)"
    if obv_trend == '→' and ad_trend == '↓':
        return "派发前兆(见顶)"
    if obv_trend == '→' and ad_trend == '→':
        return "量价均衡(整理期)"

    return "未分类"


# =========================
# V3 新增：量价交易资格判定
# =========================

OBV_AD_BLOCKLIST = {
    "趋势结束(最弱)",
    "价格拉升但量未确认"
}

OBV_AD_WATCHLIST = {
    "资金分歧(内强外弱)"
}

def obv_ad_trade_gate(obv_ad_interpretation: str):
    """
    返回交易资格：
    - allow_trade: bool
    - trade_state: str
    """
    if obv_ad_interpretation in OBV_AD_BLOCKLIST:
        return False, "禁止交易"

    if obv_ad_interpretation in OBV_AD_WATCHLIST:
        return True, "仅跟踪"

    return True, "允许建仓"


# =========================
# V3：准实盘综合评分模型
# =========================
OBV_SCORE_MAP = {
    "明确吸筹(最强)": 1.0,
    "洗盘(震荡)": 0.8,
    "量价均衡(整理期)": 0.7,
    "资金分歧(内强外弱)": 0.4,
    "趋势结束(最弱)": 0.0,
    "价格拉升但量未确认": 0.0
}


def compute_trade_score(row):
    """
    准实盘评分：
    - 技术结构权重 60%
    - CANSLIM 权重 40%
    """

    if not row['allow_trade']:
        return 0.0

    obv_score = OBV_SCORE_MAP.get(row['obv_ad_interpretation'], 0.5)
    rs_score = min(row.get('rs_rank', 50) / 100.0, 1.0)
    canslim_score = min(row.get('canslim_score', 0) / 5.0, 1.0)
    technical_score = (obv_score * 0.6 + rs_score * 0.4)
    final_score = technical_score * 0.6 + canslim_score * 0.4
    return round(final_score * 100, 2)

# ===================== 配置 =====================
# 填写你当前持仓或重点观察的股票
CURRENT_SELECTED_TICKERS = ["GOOG", "TLSA", "NVDA", "AMD", "ORCL", "CDE"]
# CURRENT_SELECTED_TICKERS = []
# ===============================================

# ===================== 主流程 =====================
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
    # 新增：确保SPY和QQQ数据更新，用于Market Regime Filter
    update_recent_prices(CURRENT_SELECTED_TICKERS + ["SPY", "QQQ"])

    # 连接数据库
    con = duckdb.connect(DUCKDB_PATH)

    # 先更新所有基本面数据（包含监控名单）
    update_fundamentals(con, get_tickers_missing_recent_data(get_recent_trading_days_smart(10)) + CURRENT_SELECTED_TICKERS + ["SPY", "QQQ"], force_update=True)

    # 🚀 修复点：自动获取库中最新的交易日期
    latest_date_in_db = get_latest_date_in_db()
    if not latest_date_in_db:
        print("❌ 数据库中没有价格数据，请先运行 fetch_all_prices()")
        return

    # 新增：建议3 - Market Regime Filter
    # SPY > MA200 AND QQQ > MA50，否则不交易
    print("🔍 检查市场 Regime...")
    regime = check_market_regime(con)
    market_regime = "多头" if regime.get("is_bull", False) else "非多头"
    print(f"市场形态判定: {market_regime}")

    # 4️⃣ Stage 2: SwingTrend 技术筛选
    print(f"🚀 Stage 2: SwingTrend 技术筛选 (包含监控名单: {CURRENT_SELECTED_TICKERS})")
    stage2 = build_stage2_swingtrend(con, latest_date_in_db, monitor_list=CURRENT_SELECTED_TICKERS, market_regime=market_regime)
    print(f"Stage 2 股票数量: {len(stage2)}")
    
    # 更新量价趋势特征表
    update_volume_trend_features(con, latest_date_in_db)

    # 挂载量价趋势特征
    stage2 = build_stage2_with_volume_trend(con, stage2)

    if stage2.empty:
        print("❌ 今日无符合技术面筛选的股票，程序结束。")
        return # 或者保存一个空结果

    # 5️⃣ Stage 3: 基本面分析
    print("📊 Stage 3: 基本面分析")
    stage3 = build_stage3_fundamental_fast(con, stage2)

    # 合并结果
    final = stage2.merge(stage3, on="stock_code", how="left")
    # 填充缺失的基本面分数为 0，防止 query 报错
    final["canslim_score"] = final["canslim_score"].fillna(0)

    # 标记来源（可选：方便你在结果中区分哪些是买入的，哪些是新选出的）
    final["is_current_hold"] = final["stock_code"].apply(lambda x: "✅" if x in CURRENT_SELECTED_TICKERS else "❌")

    # 过滤与排序
    # 💡 注意：如果你放宽了条件，这里的 canslim_score >= 3 可能又会把结果过滤成 0
    # 建议先打印看看
    print(f"合并后带评分的股票总数: {len(final)}")
    
    # 暂时降低过滤门槛以确保有输出
    final_filtered = (
        final
        .query("canslim_score > 0")
        .sort_values(["canslim_score", "rs_rank", "is_current_hold"], ascending=False)
    )

    # 6️⃣ 波动模拟 (VIX 调节)
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
    for ticker in final_filtered['stock_code']:
        p_data = simulate_pullback_range(con, ticker, current_vix=current_vix)
        pullback_list.append(p_data if p_data else {})
    
    # 关闭连接
    con.close()
    
    # 合并模拟结果
    pullback_df = pd.DataFrame(pullback_list)
    final_with_sim = pd.concat([final_filtered.reset_index(drop=True), pullback_df], axis=1)

    # 计算建议止盈位（以支撑位为基准的 3:1 盈亏比，或简单的 20% 目标）
    final_with_sim['target_profit'] = (final_with_sim['close'] * 1.20).round(2)

    # 量价趋势特征解读
    final_with_sim['obv_ad_interpretation'] = final_with_sim.apply(
        lambda row: classify_obv_ad_enhanced(
            row.get('obv_slope_20'),
            row.get('ad_slope_20'),
            vol20=row.get('vol20'),
            vix=current_vix if 'current_vix' in globals() else None
        ),
        axis=1
    )

    # =========================
    # V3：应用量价交易 Gate
    # =========================
    gate_result = final_with_sim['obv_ad_interpretation'].apply(obv_ad_trade_gate)
    final_with_sim['allow_trade'] = gate_result.apply(lambda x: x[0])
    final_with_sim['trade_state'] = gate_result.apply(lambda x: x[1])

    # =========================
    # V3：生成最终交易评分
    # =========================
    final_with_sim['trade_score'] = final_with_sim.apply(compute_trade_score, axis=1)
    final_with_sim = final_with_sim.sort_values(
        by=['trade_state', 'trade_score'],
        ascending=[True, False]
    )

    # 确保日期格式美化（可选，防止 Excel 里显示长字符串）
    if 'trade_date' in final_with_sim.columns:
        final_with_sim['trade_date'] = pd.to_datetime(final_with_sim['trade_date']).dt.strftime('%Y-%m-%d')
    # 自动识别 DataFrame 中存在的浮点数列并取2位小数
    final_with_sim = final_with_sim.round(2)

    # 7️⃣ 最终打印输出
    print("\n✅ 最终买入候选及波动模拟 (含 VIX 调节)")
    print("-" * 150)
    display_cols = [
        "is_current_hold", "stock_code", "close", 
        "ideal_entry", "hard_stop", "failure_stop", "rs_rank",
        "hard_stop", "target_profit", "canslim_score",
        "quarterly_eps_growth", "annual_eps_growth",
        "revenue_growth", "roe", "shares_outstanding", 
        "inst_ownership", "fcf_quality", "market_cap", 'sector', 
        'trade_state', 'allow_trade', 'trade_score',
        'obv_ad_interpretation', 'obv_slope_20', 'ad_slope_20'
    ]
    print(final_with_sim[display_cols].to_string(index=False))

    # 保存结果
    if not final_with_sim.empty:
        file_name_xlsx = f"swing_strategy_vix_sim_{datetime.now():%Y%m%d}.xlsx"
        try:
            final_with_sim[display_cols].to_excel(file_name_xlsx, index=False, engine='openpyxl')
            print(f"\n📊 详细策略报告已生成 Excel: {file_name_xlsx}")
        except Exception as e:
            print(f"❌ Excel 生成失败 (请检查是否安装 openpyxl): {e}")
            # 备选保存为 CSV
            file_name_csv = file_name_xlsx.replace(".xlsx", ".csv")
            final_with_sim[display_cols].to_csv(file_name_csv, index=False, encoding="utf-8-sig")
            print(f"\n📊 详细策略报告已生成: {file_name_csv}")
    else:
        print("⚠️ 经过基本面严格筛选后，没有符合条件的股票。")

if __name__ == "__main__":
    main()