import random
import requests
import pandas as pd
import duckdb
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import urllib3
import warnings
import pytz
from datetime import date, datetime, timedelta
import pandas_market_calendars as mcal
import threading
import queue
import numpy as np
from enum import Enum
from openpyxl import load_workbook

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
INSERT_BATCH_SIZE = 100
QUEUE_MAX_SIZE = 5000
# ===============================================

price_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
fundamental_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
STOP_SIGNAL = object()


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
            obv_slope_5 DOUBLE,
            obv_high_60 BOOLEAN,
            ad DOUBLE,
            ad_slope_20 DOUBLE,
            ad_slope_5 DOUBLE,
            vol20 DOUBLE,
            vol_rs DOUBLE,
            vol50 DOUBLE,
            vol_rs_vcp DOUBLE,
            price_tightness DOUBLE,
            vol_spike_ratio DOUBLE,
            daily_change_pct DOUBLE
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
def get_tickers_missing_recent_price(trading_days):
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


def get_tickers_missing_recent_fundamentals(trading_days):
    """
    返回尚未更新到最近一个交易日的 ticker 列表
    """
    latest_trading_day = trading_days[-1]

    con = duckdb.connect(DUCKDB_PATH)

    query = f"""
        SELECT t.symbol
        FROM stock_ticker t
        LEFT JOIN  (
            SELECT
                stock_code,
                MAX(update_date) AS last_update_date
            FROM stock_fundamentals
            GROUP BY stock_code
        ) f
        ON f.stock_code = t.symbol
        WHERE
            t.type = 'Common Stock'
            AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
            AND COALESCE(t.yf_price_available, TRUE) = TRUE
            AND (
                f.last_update_date IS NULL
                OR f.last_update_date < DATE '{latest_trading_day}'
            )
    """

    tickers = [r[0] for r in con.execute(query).fetchall()]
    con.close()
    return tickers


def mark_yf_unavailable(symbols):
    if not symbols: return
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # 直接拿 original_symbol 去匹配数据库 symbol 列
        con.executemany(
            "UPDATE stock_ticker SET yf_price_available = FALSE WHERE symbol = ?", 
            [(s,) for s in symbols]
        )
        print(f"🛠️ 数据库已更新：已永久屏蔽这 {len(symbols)} 只股票。")
    except Exception as e:
        print(f"❌ 数据库写入失败: {e}")
    finally:
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


def price_consumer():
    con = duckdb.connect(DUCKDB_PATH)
    buffer = []
    SQL_INSERT = """
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
    """
    while True:
        item = price_queue.get()

        if item is STOP_SIGNAL:
            break

        buffer.append(item) # 逐行获取

        if len(buffer) >= INSERT_BATCH_SIZE:
            con.executemany(SQL_INSERT, buffer)
            buffer.clear()

    if buffer:
        con.executemany(SQL_INSERT, buffer)

    con.close()


def update_recent_prices(watchlist: list = []):
    print(f"🕒 当前上海时间: {datetime.now():%Y-%m-%d %H:%M}")

    trading_days = get_recent_trading_days_smart(10)
    print(f"📅 纽交所最近有效交易日：{trading_days}")

    target_date = trading_days[-1]
    print(f"🎯 目标同步日期: {target_date}")

    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()

    raw_tickers = get_tickers_missing_recent_price(trading_days)
    if not raw_tickers and not watchlist:
        print(f"✅ 数据库已是最新，跳过更新")
        return

    yahoo_map = {t: finnhub_to_yahoo(t) for t in raw_tickers}
    yahoo_tickers = list(yahoo_map.values())
    print(f"需要更新 {len(yahoo_tickers)} 只股票")

    # 🔥 核心改变：记录哪些真正写进了数据库
    actual_success_yahoo_tickers = set()

    consumer_thread = threading.Thread(target=price_consumer, daemon=True)
    consumer_thread.start()

    for i in range(0, len(yahoo_tickers), YF_BATCH_SIZE):
        batch = yahoo_tickers[i:i + YF_BATCH_SIZE]
        print(f"   更新 {i} - {i + len(batch)}")

        try:
            data = yf.download(tickers=batch, period="20d", group_by='ticker', threads=True, auto_adjust=True)

            for yf_symbol in batch:
                # 1. 提取 DataFrame
                if len(batch) == 1:
                    df = data
                else:
                    if yf_symbol not in data.columns.levels[0]:
                        print(f"   🔎 {yf_symbol}: yf 返回结果中完全不存在该列")
                        continue
                    df = data[yf_symbol]

                # 2. 检查是否有数据
                df_clean = df.dropna(subset=['Close'])
                if df_clean.empty:
                    print(f"   🔎 {yf_symbol}: Close 列全是空值 (NaN)")
                    continue

                # 3. 严格日期判定
                last_val = df_clean.index.max()
                last_dt = last_val.date() if hasattr(last_val, 'date') else last_val
                
                # --- 核心诊断打印 ---
                # print(f"   🔎 {yf_symbol}: 最新日期={last_dt}, 目标日期={target_date_obj}")

                if last_dt < target_date_obj: # 注意：直接对比目标同步日期
                    print(f"   🔎 {yf_symbol}: 日期不合要求 ({last_dt} < {target_date_obj})，不计入成功")
                    continue

                # 4. 只有日期完全对上的，才算成功
                actual_success_yahoo_tickers.add(yf_symbol)

                # ✅ 只有通过了上面所有关卡，才认为成功，并入库
                actual_success_yahoo_tickers.add(yf_symbol)

                finnhub_symbol = yahoo_to_finnhub(yf_symbol)
                for trade_date, row in df_clean.iterrows():
                    price_queue.put((
                        finnhub_symbol, trade_date.date(),
                        float(row["Open"]), float(row["High"]), float(row["Low"]),
                        float(row["Close"]), int(row["Volume"])
                    ))
        except Exception as e:
            print(f"❌ 批次异常: {e}")

    price_queue.put(STOP_SIGNAL)
    consumer_thread.join()

    # ==========================================================
    # 终极识别：总表 减去 真正入库成功的表 = 顽固失效的表
    # ==========================================================
    failed_tickers = list(set(yahoo_tickers) - actual_success_yahoo_tickers)
    
    if failed_tickers:
        reverse_map = {v: k for k, v in yahoo_map.items()}
        to_blacklist = [reverse_map[s] for s in failed_tickers if s in reverse_map]
        
        print(f"⚠️ 识别完成！以下 {len(to_blacklist)} 只股票因无数据或过期被判定为失效:")
        print(f"🚫 列表: {to_blacklist}")
        
        # 执行标记入库
        mark_yf_unavailable(to_blacklist)
    else:
        print("✅ 本次所有股票均已更新到最新日期。")

    print("🎉 全部完成")


# ============================================================
# Stage 2：SwingTrend 技术筛选（全部在 DuckDB 内完成）
# ============================================================
def build_stage2_swingtrend(target_date: date, monitor_list: list = [], market_regime: str = "多头") -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH)
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
                AND b.close >= 1.25 * b.low_52w     -- 距 52 周低点 ≥ +25%
                AND b.close >= 0.55 * b.high_52w    -- 距 52 周高点 ≥ -45%

                /* ===== 3. RS 强度（收紧） =====
                多头市中，强者更强是核心假设
                */
                AND ((r.rs_rank >= 75) OR (b.sector = 'Technology' AND r.rs_rank >= 65))

                /* RS 不能明显走弱，允许横盘 */
                AND r.rs_20 >= r.rs_20_10days_ago * 0.90

                /* ===== 4. 波动结构 =====
                典型 VCP, 确保收缩, 而不是平坦或扩散
                */
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 0.95
                AND a.atr_slope < 0
                AND a.atr15 <= a.atr60 * 1.00

                /* ===== 5. 成交量底线 =====
                只排除流动性明显不足的股票
                */
                AND vt.vol20 > 300000

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
                AND vt.vol20 > 700000

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
            /* ===== 5 天高低点窗口 ===== */
            -- 计算过去 5 个交易日（含今天）的最高价
            MAX(p.high) OVER w5 AS high_5d,
            -- 计算过去 5 个交易日（含今天）的最低价
            MIN(p.low) OVER w5 AS low_5d,

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price p
        LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
        WINDOW
            w5  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 4 PRECEDING),
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

    latest_fundamentals AS (
        SELECT *
        FROM (
            SELECT
                sf.*,
                ROW_NUMBER() OVER (
                    PARTITION BY sf.stock_code
                    ORDER BY sf.update_date DESC
                ) AS rn
            FROM stock_fundamentals sf
            WHERE sf.update_date IS NOT NULL
        )
        WHERE rn = 1
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        b.volume,
        b.sector,
        r.rs_rank,
        b.ma10, b.ma20, b.ma50, b.ma150, b.ma200,
        b.high_52w, b.low_52w, b.high_5d, b.low_5d,
        a.atr5, a.atr20, a.atr15, a.atr60, a.atr10, a.atr50,
        a.atr_slope,
        r.rs_20, r.rs_20_10days_ago,
        p.pivot_price,
        vt.obv,
        vt.obv_ma20,
        vt.obv_slope_20,
        vt.obv_slope_5,
        vt.ad,
        vt.ad_slope_20,
        vt.ad_slope_5,
        vt.vol20,
        vt.vol_rs,
        vt.vol50,
        vt.vol_rs_vcp,
        vt.price_tightness,
        vt.vol_spike_ratio,  -- 量比
        vt.daily_change_pct, -- 当日价格变动波幅(%)
        f.canslim_score,
        f.quarterly_eps_growth,
        f.annual_eps_growth,
        f.forward_eps_growth,
        f.roe,
        f.revenue_growth,
        f.fcf_quality,
        f.shares_outstanding,
        f.inst_ownership,
        f.market_cap,
        f.opt_pc_ratio,
        f.opt_avg_iv,
        f.opt_uoa_detected
    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN atr_stats a USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    LEFT JOIN stock_volume_trend vt
        ON b.stock_code = vt.stock_code
        AND b.trade_date = vt.trade_date
    INNER JOIN latest_fundamentals f
        ON b.stock_code = f.stock_code
    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            (
                {market_filter_sql}
            )
            OR
            b.stock_code IN ({monitor_str})
        )
        AND f.roe IS NOT NULL
        AND f.fcf_quality IS NOT NULL
        AND f.market_cap >= 1e9

    """

    df = con.execute(sql).df()
    con.close()
    return df


def build_stage2_swingtrend_balanced(target_date, monitor_list: list = [], market_regime: str = "多头"):
    """
    平衡版：介于标准版和应急版之间
    
    核心改动：
    1. 均线排列：close > ma50 AND ma50 > ma200（移除ma150要求）
    2. 52周位置：close >= 1.25 * low_52w AND >= 0.55 * high_52w
    3. RS排名：≥65（前35%）
    4. VCP收缩：atr5/atr20 < 1.10（略微放宽）
    5. 成交量：10万股
    
    预期筛选：50-150支股票
    """
    con = duckdb.connect(DUCKDB_PATH)

    monitor_str = ", ".join([f"'{t}'" for t in monitor_list]) if monitor_list else "''"
    
    if market_regime == "多头":
        market_filter_sql = """
            (
                /* ===== 1. 均线排列（简化版）===== */
                b.close > b.ma50
                AND b.ma50 > b.ma200
                AND b.ma50 IS NOT NULL
                AND b.ma200 IS NOT NULL
                
                /* ===== 2. 52周位置（适中）===== */
                AND b.close >= 1.25 * b.low_52w     -- 距52周低点 ≥ +25%
                AND b.close >= 0.55 * b.high_52w    -- 距52周高点 ≥ -45%
                
                /* ===== 3. RS强度（适中）===== */
                AND r.rs_rank >= 65  -- 前35%
                AND r.rs_20 >= r.rs_20_10days_ago * 0.88  -- RS不能明显走弱
                
                /* ===== 4. VCP收缩（核心修改：大市值放宽）===== */
                AND (
                    /* 大市值（≥1000亿美元）放宽到 < 1.05 */
                    (f.market_cap >= 100000000000 AND (a.atr5 / NULLIF(a.atr20, 0)) < 1.05)
                    OR
                    /* 小市值保持原要求 < 1.10 */
                    (f.market_cap < 100000000000 AND (a.atr5 / NULLIF(a.atr20, 0)) < 1.10)
                )
                AND a.atr15 <= a.atr60 * 1.05
                
                /* ===== 5. 成交量（标准）===== */
                AND vt.vol20 > 100000
            )
        """
    else:
        # 非多头市场保持严格
        market_filter_sql = """
            (
                b.close > b.ma150
                AND b.ma50 > b.ma150
                AND b.ma150 > b.ma200
                AND b.close >= 1.30 * b.low_52w
                AND b.close >= 0.80 * b.high_52w
                AND r.rs_rank >= 70
                AND r.rs_20 > r.rs_20_10days_ago
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 0.95
                AND a.atr15 < a.atr60
                AND vt.vol20 > 300000
            )
        """
    
    # SQL主体（与标准版相同，只替换market_filter_sql）
    sql = f"""
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
            /* ===== 5 天高低点窗口 ===== */
            -- 计算过去 5 个交易日（含今天）的最高价
            MAX(p.high) OVER w5 AS high_5d,
            -- 计算过去 5 个交易日（含今天）的最低价
            MIN(p.low) OVER w5 AS low_5d,

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price p
        LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
        WINDOW
            w5  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 4 PRECEDING),
            w10  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 9 PRECEDING),
            w20  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 19 PRECEDING),
            w50  AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 49 PRECEDING),
            w150 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 149 PRECEDING),
            w200 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 199 PRECEDING),
            w252 AS (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 251 PRECEDING),
            w_all AS (PARTITION BY p.stock_code)
    ),

    returns AS (
        SELECT
            stock_code,
            trade_date,
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
            (
                0.4 * COALESCE(r1y, 0) + 
                0.3 * COALESCE(r6m, 0) + 
                0.2 * COALESCE(r3m, 0) + 
                0.1 * COALESCE(r1m, 0)
            ) AS rs_score,
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

    rs_averages AS (
        SELECT
            *,
            AVG(rs_score) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 19 PRECEDING
            ) AS rs_20
        FROM rs_scores
    ),

    rs_ranked AS (
        SELECT
            *,
            LAG(rs_20, 10) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
            ) AS rs_20_10days_ago
        FROM rs_averages
    ),

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

    pivot_data AS (
        SELECT
            stock_code,
            trade_date,
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS pivot_price
        FROM stock_price
    ),

    latest_fundamentals AS (
        SELECT *
        FROM (
            SELECT
                sf.*,
                ROW_NUMBER() OVER (
                    PARTITION BY sf.stock_code
                    ORDER BY sf.update_date DESC
                ) AS rn
            FROM stock_fundamentals sf
            WHERE sf.update_date IS NOT NULL
        )
        WHERE rn = 1
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        b.volume,
        b.sector,
        r.rs_rank,
        b.ma10, b.ma20, b.ma50, b.ma150, b.ma200,
        b.high_52w, b.low_52w, b.high_5d, b.low_5d,
        a.atr5, a.atr20, a.atr15, a.atr60, a.atr10, a.atr50,
        a.atr_slope,
        r.rs_20, r.rs_20_10days_ago,
        p.pivot_price,
        vt.obv,
        vt.obv_ma20,
        vt.obv_slope_20,
        vt.obv_slope_5,
        vt.ad,
        vt.ad_slope_20,
        vt.ad_slope_5,
        vt.vol20,
        vt.vol_rs,
        vt.vol50,
        vt.vol_rs_vcp,
        vt.price_tightness,
        vt.vol_spike_ratio,  -- 量比
        vt.daily_change_pct, -- 当日价格变动波幅(%)
        f.canslim_score,
        f.quarterly_eps_growth,
        f.annual_eps_growth,
        f.forward_eps_growth,
        f.roe,
        f.revenue_growth,
        f.fcf_quality,
        f.shares_outstanding,
        f.inst_ownership,
        f.market_cap,
        f.opt_pc_ratio,
        f.opt_avg_iv,
        f.opt_uoa_detected
    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN atr_stats a USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    LEFT JOIN stock_volume_trend vt
        ON b.stock_code = vt.stock_code
        AND b.trade_date = vt.trade_date
    INNER JOIN latest_fundamentals f
        ON b.stock_code = f.stock_code
    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            {market_filter_sql}
            OR
            b.stock_code IN ({monitor_str})
        )
        AND f.roe IS NOT NULL
        AND f.fcf_quality IS NOT NULL
        AND f.market_cap >= 1e9
    """
    
    df = con.execute(sql).df()
    con.close()
    return df


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
            market_cap BIGINT,                        -- 市值（marketCap）
            forward_eps_growth DOUBLE,                -- L: 前瞻每股收益增长率（计算得出）
            opt_pc_ratio DOUBLE DEFAULT 0,            -- 期权看涨(call)看跌(put)成交量比
            opt_avg_iv DOUBLE DEFAULT 0,              -- 期权平均隐含波动率
            opt_uoa_detected BOOLEAN DEFAULT FALSE,   -- 是否检测到异常期权活动
            opt_uoa_score DOUBLE DEFAULT 0,           -- 异常期权强度（0~1）,下注规模
            opt_uoa_call_bias DOUBLE DEFAULT 0,       -- 异常期权方向偏好：>0 偏多，<0 偏空
            opt_uoa_avg_dte DOUBLE DEFAULT NULL,      -- 异常期权平均到期天数或下注周期：判断是事件还是趋势
            opt_uoa_type VARCHAR DEFAULT NULL,        -- 异常期权行为分类（institutional / event / noise）
            next_earnings_date DATE DEFAULT NULL      -- 下一次财报日期
        );
    """)


def extract_option_sentiment_from_yf(ticker: yf.Ticker) -> dict:
    """
    从 yfinance 提取期权情绪指标（现实可行版本）

    返回：
    - opt_pc_ratio       : Put / Call 成交量比
    - opt_avg_iv         : ATM 附近期权平均隐含波动率
    - opt_uoa_detected   : 是否检测到异常期权活动（弱 UOA proxy）
    """

    try:
        # =========================
        # 0. 检查是否有期权数据
        # =========================
        expirations = ticker.options
        if not expirations:
            return {
                "opt_pc_ratio": None,
                "opt_avg_iv": None,
                "opt_uoa_detected": False,
                "underlying_price": None
            }

        # =========================
        # 1. 获取最近到期日
        # =========================
        exp = expirations[0]
        opt_chain = ticker.option_chain(exp)

        calls = opt_chain.calls.copy()
        puts = opt_chain.puts.copy()

        if calls.empty or puts.empty:
            return {
                "opt_pc_ratio": None,
                "opt_avg_iv": None,
                "opt_uoa_detected": False,
                "underlying_price": None
            }

        # =========================
        # 2. 获取标的价格（稳定顺序）
        # =========================
        underlying_price = (
            ticker.fast_info.get("lastPrice")
            or ticker.fast_info.get("regularMarketPrice")
            or ticker.info.get("regularMarketPrice")
        )

        # =========================
        # A. Put / Call Ratio（成交量）
        # =========================
        call_vol = calls["volume"].fillna(0).sum()
        put_vol = puts["volume"].fillna(0).sum()
        pc_ratio = round(put_vol / call_vol, 3) if call_vol > 0 else None

        # =========================
        # B. ATM 附近平均 IV
        # =========================
        if underlying_price:
            lower = underlying_price * 0.85
            upper = underlying_price * 1.15

            mask_c = (calls["strike"] >= lower) & (calls["strike"] <= upper)
            mask_p = (puts["strike"]  >= lower) & (puts["strike"]  <= upper)

            relevant_iv = pd.concat(
                [
                    calls.loc[mask_c, "impliedVolatility"],
                    puts.loc[mask_p,  "impliedVolatility"]
                ],
                ignore_index=True
            )
        else:
            relevant_iv = pd.concat(
                [calls["impliedVolatility"], puts["impliedVolatility"]],
                ignore_index=True
            )

        # 清洗 IV
        relevant_iv = (
            relevant_iv
            .replace(0, np.nan)
            .dropna()
        )

        # 过滤极端异常值（流动性导致的假 IV）
        relevant_iv = relevant_iv[relevant_iv.between(0.05, 1.5)]

        avg_iv = round(float(relevant_iv.mean()), 4) if not relevant_iv.empty else None

        # =========================
        # C. 弱 UOA（异常期权活动）检测
        # =========================
        # 定义：成交量显著大于持仓量，且达到最小规模
        UOA_MIN_VOLUME = 100

        def detect_uoa(df: pd.DataFrame) -> bool:
            if df.empty:
                return False

            if "volume" not in df.columns or "openInterest" not in df.columns:
                return False

            vol = df["volume"].fillna(0)
            oi = df["openInterest"].fillna(0)

            uoa_mask = (vol >= UOA_MIN_VOLUME) & (vol > oi * 2)
            return bool(uoa_mask.any())

        uoa_flag = detect_uoa(calls) or detect_uoa(puts)

        # =========================
        # 返回
        # =========================
        return {
            "opt_pc_ratio": pc_ratio,
            "opt_avg_iv": avg_iv,
            "opt_uoa_detected": uoa_flag,
            "underlying_price": round(float(underlying_price), 2) if underlying_price else None
        }

    except Exception as e:
        return {
            "opt_pc_ratio": None,
            "opt_avg_iv": None,
            "opt_uoa_detected": False,
            "underlying_price": None,
            "error": str(e)
        }


def extract_uoa_structured_from_yf(t: yf.Ticker) -> dict:
    """
    从 yfinance 构造【机构级】UOA 结构字段

    返回字段说明：
    - opt_uoa_score:   异常强度 (0~1)
    - opt_uoa_call_bias: 方向性 (-1~1)，>0 偏多
    - opt_uoa_avg_dte: 平均剩余期限（天）
    - opt_uoa_type:   行为类型: none / event / institutional / long_term
    """

    empty_uoa = {
        "opt_uoa_score": 0.0,
        "opt_uoa_call_bias": 0.0,
        "opt_uoa_avg_dte": None,
        "opt_uoa_type": "none",
    }

    try:
        expirations = t.options
        if not expirations:
            return empty_uoa

        # 只看最近到期（机构短期行为最集中）
        exp = expirations[0]
        opt_chain = t.option_chain(exp)

        calls = opt_chain.calls.copy()
        puts  = opt_chain.puts.copy()

        if calls.empty and puts.empty:
            return empty_uoa

        calls["type"] = "CALL"
        puts["type"]  = "PUT"

        df = pd.concat([calls, puts], ignore_index=True)

        # ===== 基础防御 =====
        df = df[
            (df["volume"].fillna(0) > 0) &
            (df["openInterest"].fillna(0) >= 0)
        ]

        if df.empty:
            return empty_uoa

        # ===== 派生指标 1：Volume / OI =====
        df["vol_oi_ratio"] = (
            df["volume"] /
            df["openInterest"].replace(0, np.nan)
        )

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["vol_oi_ratio"])

        if df.empty:
            return empty_uoa

        # ===== 派生指标 2：DTE（用 expiration 算）=====
        exp_date = pd.to_datetime(exp)
        today = pd.Timestamp.today().normalize()

        df["dte"] = (exp_date - today).days
        df = df[df["dte"] > 0]

        if df.empty:
            return empty_uoa

        # ===== UOA 判定（机构级）=====
        # 条件：
        # 1. 成交量 >= 100
        # 2. volume >= openInterest * 3
        uoa_df = df[
            (df["volume"] >= 100) &
            (df["vol_oi_ratio"] >= 3)
        ]

        if uoa_df.empty:
            return empty_uoa

        # ===== 强度评分（归一化）=====
        uoa_score = min(
            uoa_df["vol_oi_ratio"].mean() / 10.0,
            1.0
        )

        # ===== 方向性 =====
        call_vol = uoa_df[uoa_df["type"] == "CALL"]["volume"].sum()
        put_vol  = uoa_df[uoa_df["type"] == "PUT"]["volume"].sum()

        call_bias = (call_vol - put_vol) / max(call_vol + put_vol, 1)

        # ===== 平均 DTE =====
        avg_dte = uoa_df["dte"].mean()

        # ===== 行为分类（非常关键）=====
        if avg_dte <= 7:
            uoa_type = "event"               # 财报 / FDA / 判决
        elif avg_dte <= 30:
            uoa_type = "institutional"       # 典型机构 swing
        else:
            uoa_type = "long_term"            # 长周期布局

        return {
            "opt_uoa_score": round(float(uoa_score), 3),
            "opt_uoa_call_bias": round(float(call_bias), 3),
            "opt_uoa_avg_dte": round(float(avg_dte), 1),
            "opt_uoa_type": uoa_type,
        }

    except Exception as e:
        # 建议你临时打印一次看看真实错误
        # print(f"[UOA ERROR] {t.ticker}: {e}")
        return empty_uoa


# 读取单个股票的基本面数据
def load_fundamentals_by_yf(symbol):
    """
    从 yfinance 加载单个股票的基本面数据
    """
    fundamentals_info = {}
    try:
        t = yf.Ticker(finnhub_to_yahoo(symbol))
        info = t.info

        # 提取期权情绪数据
        option_sentiment = extract_option_sentiment_from_yf(t)

        # 获取下一次财报日期
        next_earnings_date = None
        try:
            cal = t.calendar
            # 1. 如果是 DataFrame
            if cal is not None and isinstance(cal, pd.DataFrame) and not cal.empty:
                if 'Earnings Date' in cal.index:
                    row_data = cal.loc['Earnings Date']
                    if len(row_data) > 0:
                        next_earnings_date = row_data.iloc[0]
                elif 0 in cal.index:
                    next_earnings_date = cal.iloc[0, 0]
            
            # 2. 如果是字典 (针对 TIRX, HVMC 等报错点的修复)
            elif isinstance(cal, dict) and 'Earnings Date' in cal:
                e_list = cal['Earnings Date']
                if isinstance(e_list, list) and len(e_list) > 0:
                    next_earnings_date = e_list[0]
            
            # 3. 方法B：如果 A 失败，尝试从 info 获取时间戳
            if not next_earnings_date:
                ts = info.get('earningsTimestamp') or info.get('earningsTimestampStart')
                if ts:
                    next_earnings_date = datetime.fromtimestamp(ts).date()
            
            # 统一转换为 date 对象
            if isinstance(next_earnings_date, (datetime, pd.Timestamp)):
                next_earnings_date = next_earnings_date.date()
                
        except Exception as e_cal:
            # 即使获取日期失败，也不要中断整个基本面的抓取
            print(f"财报日期解析跳过 {symbol}: {e_cal}") 
            next_earnings_date = None

        # === 期权 UOA（结构化） ===
        uoa_struct = extract_uoa_structured_from_yf(t)

        opt_uoa_score = uoa_struct["opt_uoa_score"]
        opt_uoa_call_bias = uoa_struct["opt_uoa_call_bias"]
        opt_uoa_avg_dte = uoa_struct["opt_uoa_avg_dte"]
        opt_uoa_type = uoa_struct["opt_uoa_type"]

        # --- 金律字段提取 ---
        market_cap = info.get('marketCap', 0) or 0

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
        sector = info.get("sector")
        industry = info.get("industry")
        
        # 提取 CAN SLIM 指标
        quarterly_eps_growth = info.get("earningsQuarterlyGrowth")  # C
        annual_eps_growth = info.get("earningsGrowth")  # A (年度)
        rev_growth = info.get("revenueGrowth")  # 辅助
        roe = info.get("returnOnEquity")
        shares_outstanding = info.get("sharesOutstanding")  # S
        inst_own = info.get("heldPercentInstitutions")  # I
        fcf = info.get("freeCashflow")
        ocf = info.get("operatingCashflow")
        # 计算前瞻每股收益增长率
        # forward_eps_growth = (info.get('forwardEps', 0) / info.get('trailingEps', 1) - 1) if info.get('trailingEps') else None
        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
        target_mean_price = info.get('targetMeanPrice')
        # 计算自由现金流质量
        fcf_quality = (fcf / ocf) if (fcf and ocf and ocf > 0) else None

        # 计算 CAN SLIM 分数 (简化：每个组件达标加1分)
        score = 0
        if quarterly_eps_growth and quarterly_eps_growth > 0.25: score += 1  # C >25%
        if annual_eps_growth and annual_eps_growth > 0.25: score += 1  # A >25%
        if rev_growth and rev_growth > 0.15: score += 1  # 营收辅助
        if shares_outstanding and shares_outstanding < 100000000: score += 1  # S: 低股本 <1亿股 (可调)
        if inst_own and inst_own > 0.5: score += 1  # I: 机构 >50%
        # if forward_eps_growth > 0.20: score += 1  # L: 前瞻 EPS 增长 >20%
        if current_price and target_mean_price and current_price > 0:
            implied_growth = (target_mean_price / current_price) - 1
            if implied_growth > 0.20:  # 分析师预期上涨 >20%
                score += 1
        if fcf_quality is not None and fcf_quality > 0.8: score += 1  # 高质量现金转化

        fundamentals_info["symbol"] = symbol
        fundamentals_info["sector"] = sector
        fundamentals_info["industry"] = industry
        fundamentals_info["quarterly_eps_growth"] = quarterly_eps_growth
        fundamentals_info["annual_eps_growth"] = annual_eps_growth
        fundamentals_info["rev_growth"] = rev_growth
        fundamentals_info["roe"] = roe
        fundamentals_info["shares_outstanding"] = shares_outstanding
        fundamentals_info["inst_own"] = inst_own
        fundamentals_info["fcf_quality"] = fcf_quality
        fundamentals_info["score"] = score
        fundamentals_info["market_cap"] = market_cap
        fundamentals_info["opt_pc_ratio"] = option_sentiment.get("opt_pc_ratio")
        fundamentals_info["opt_avg_iv"] = option_sentiment.get("opt_avg_iv")
        fundamentals_info["opt_uoa_detected"] = option_sentiment.get("opt_uoa_detected")
        fundamentals_info["opt_uoa_score"] = opt_uoa_score
        fundamentals_info["opt_uoa_call_bias"] = opt_uoa_call_bias
        fundamentals_info["opt_uoa_avg_dte"] = opt_uoa_avg_dte
        fundamentals_info["opt_uoa_type"] = opt_uoa_type
        fundamentals_info["next_earnings_date"] = next_earnings_date
    except Exception as e:
        # 捕获可能的限流异常
        if "429" in str(e):
            print(f"检测到限流 (429)，尝试大幅度延迟...")
            time.sleep(random.uniform(3, 8)) 
        raise e

    return fundamentals_info


def fundamental_consumer():
    con = duckdb.connect(DUCKDB_PATH)
    fundamental_buffer = []
    
    SQL_INSERT_FUNDAMENTAL = """
        INSERT OR REPLACE INTO stock_fundamentals 
        VALUES (?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    sector_buffer = []
    SQL_INSERT_SECTOR = """
        UPDATE stock_ticker
        SET sector = ?, industry = ?, updated_at = CURRENT_TIMESTAMP
        WHERE symbol = ?
    """
    while True:
        info = fundamental_queue.get()

        if info is STOP_SIGNAL:
            break

        # 逐行获取fundamental数据
        fundamental_buffer.append((
            info.get("symbol"), 
            info.get("quarterly_eps_growth"), 
            info.get("annual_eps_growth"), 
            info.get("rev_growth"), 
            info.get("roe"), 
            info.get("shares_outstanding"), 
            info.get("inst_own"), 
            info.get("fcf_quality"), 
            info.get("score"), 
            info.get("market_cap"), 
            0, 
            info.get("opt_pc_ratio"), 
            info.get("opt_avg_iv"), 
            info.get("opt_uoa_detected"),
            info.get("opt_uoa_score"),
            info.get("opt_uoa_call_bias"),
            info.get("opt_uoa_avg_dte"),
            info.get("opt_uoa_type"),
            info.get("next_earnings_date")
        ))
        # 逐行获取sector数据
        sector_buffer.append((info.get("sector"), info.get("industry"), info.get("symbol")))

        # 批量fundamental插入
        if len(fundamental_buffer) >= INSERT_BATCH_SIZE:
            con.executemany(SQL_INSERT_FUNDAMENTAL, fundamental_buffer)
            fundamental_buffer.clear()
        # 批量sector更新
        if len(sector_buffer) >= INSERT_BATCH_SIZE:
            con.executemany(SQL_INSERT_SECTOR, sector_buffer)
            sector_buffer.clear()
    
        print(f"              🛠️ 已处理基本面数据: {info.get('symbol')}")

    # 插入剩余数据
    if fundamental_buffer:
        con.executemany(SQL_INSERT_FUNDAMENTAL, fundamental_buffer)
    # 更新剩余sector数据
    if sector_buffer:
        con.executemany(SQL_INSERT_SECTOR, sector_buffer)

    con.close()


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
        # 1. 识别已更新的股票：在过去 7 天内未更新过的
        recent_tickers = con.execute("""
            SELECT stock_code FROM stock_fundamentals 
            WHERE update_date > CURRENT_DATE - INTERVAL '7 days'
        """).df()['stock_code'].tolist()
        # 2. 差集计算：不在 recent_tickers 里的才需要更新
        need_update = [t for t in ticker_list if t not in recent_tickers]

    if not need_update:
        print("✅ 所有基本面数据均在有效期内，无需更新。")
        return

    consumer_thread = threading.Thread(target=fundamental_consumer, daemon=True)
    consumer_thread.start()

    print(f"🚀 开始更新 {len(need_update)} 只股票的基本面...")
    update_count = 1
    for ticker in need_update:
        try:
            fundamentals_info = load_fundamentals_by_yf(ticker)
            fundamental_queue.put(fundamentals_info)
            print(f"  [{update_count}/{len(need_update)}] 已下载基本面数据: {ticker}")
            update_count += 1
        except Exception as e:
            print(f"Error updating {ticker}: {e}")
    
    fundamental_queue.put(STOP_SIGNAL)
    consumer_thread.join()

    con.close()
    print(f"✅ 基本面数据更新完成，共更新 {update_count} 只股票。")


# 获取数据库中最新的交易日期
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


def check_market_regime() -> dict:
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
    con = duckdb.connect(DUCKDB_PATH)

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

    # 添加 RSI 计算（可选）
    spy_prices = con.execute("SELECT close FROM stock_price WHERE stock_code='SPY' ORDER BY trade_date DESC LIMIT 14").df()['close']
    delta = spy_prices.diff()
    gain = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(span=14, adjust=False).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs.iloc[-1]))

    is_bull = (spy_close > spy_ma200) and (qqq_close > qqq_ma50) and (rsi < 70)  # 避免超买

    con.close

    return {
        "is_bull": is_bull,
        "spy_close": spy_close,
        "spy_ma200": spy_ma200,
        "qqq_close": qqq_close,
        "qqq_ma50": qqq_ma50
    }


def update_volume_trend_features(latest_trading_day: str):
    con = duckdb.connect(DUCKDB_PATH)
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
            SUM(ad_delta) OVER w AS ad,

            /* ===== avg_vol_20 ===== */
            AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as avg_vol_20,

            /* ===== 计算价格绝对涨跌幅 ===== */
            ABS((close - LAG(close) OVER w) / NULLIF(LAG(close) OVER w, 0)) as daily_change_pct
        FROM base
        WINDOW w AS (PARTITION BY stock_code ORDER BY trade_date)
    ),

    trends AS (          -- 新CTE：在全历史上计算窗口
        SELECT
            stock_code,
            trade_date,
            obv,
            AVG(obv) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING) AS obv_ma20,
            (obv - LAG(obv, 20) OVER (PARTITION BY stock_code ORDER BY trade_date)) / NULLIF(avg_vol_20 * 20, 0) AS obv_slope_20,
            (obv - LAG(obv, 5) OVER (PARTITION BY stock_code ORDER BY trade_date)) / NULLIF(avg_vol_20 * 5, 0) AS obv_slope_5,
            obv >= MAX(obv) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 59 PRECEDING) AS obv_high_60,
            ad,
            (ad - LAG(ad, 20) OVER (PARTITION BY stock_code ORDER BY trade_date)) / NULLIF(avg_vol_20 * 20, 0) AS ad_slope_20,
            (ad - LAG(ad, 5) OVER (PARTITION BY stock_code ORDER BY trade_date)) / NULLIF(avg_vol_20 * 5, 0) AS ad_slope_5,
            AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING) AS vol20,
            volume / NULLIF(AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING), 0) AS vol_rs,
            AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 49 PRECEDING) AS vol50,
            -- 5日均量 / 60日均量 (这能完美识别缩量干涸)
            (AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 4 PRECEDING)) / 
            (NULLIF(AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 59 PRECEDING), 0)) as vol_rs_vcp,
            
            -- 计算紧致度 (Price Tightness)
            (MAX(high) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 4 PRECEDING) - 
            MIN(low) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 4 PRECEDING)) / NULLIF(close, 0) as price_tightness,

            -- 当日量比 (Volume Spike)
            volume / NULLIF(AVG(volume) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 20 PRECEDING), 0) as vol_spike_ratio,
            -- 日价格变动幅度(百分比)
            daily_change_pct
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
    con.close()


# =========================
# V3 新增：量价形势判定
# | obv_ad_label        | 资金行为本质        | 风险等级        |
# | ------------------  | ------------------ | -------------- |
# | 极度缩量(隐蔽吸筹)    | 隐蔽建仓（VCP/WWD） | 🟢 低         |
# | 明确吸筹             | 主动进攻            | 🟢 低         |
# | 趋势加速放量         | 趋势加速            | 🟢 低          |
# | 强趋势回撤           | 洗盘 / 换手         | 🟢 低~中       |
# | 底部试探             | 早期试水            | 🟡 中          |
# | 趋势中资金分歧        | 多空未统一          | 🟡 中          |
# | 量价中性             | 无信息              | 🟡 中          |
# | 高位放量分歧          | 高位博弈            | 🔴 高         |
# | 高位滞涨(出货嫌疑)	  | 诱多/滞涨(Churning) | 🔴 高 (准派发) |
# | 控盘减弱预警          | 主力松动            | 🔴 高         |
# | 派发阶段             | 明确撤退            | 🔴 极高        |
# =========================
def classify_obv_ad_enhanced(
    obv_s20,
    ad_s20,
    obv_s5,
    ad_s5,
    vol_rs_vcp,             # 5d/60d 缩量比
    raw_price_tightness,    # 5d 波幅占比
    vol_spike_ratio,        # 当日量比
    daily_change_pct,       # 当日价格变动幅度
    market_regime="多头"
):
    # 阈值定义
    STRONG = 0.1 # 只要平均每日净流入达到日均成交量的 10% 就算强
    WEAK = 0.02

    # === 新增：短期资金是否加速 ===
    obv_accel = obv_s5 - obv_s20
    ad_accel = ad_s5 - ad_s20

    is_fund_accelerating = (
        obv_s20 > STRONG and
        ad_s20 > STRONG and
        obv_accel > WEAK and
        ad_accel > WEAK
    )
    
    price_tightness = 1.0 # 默认不紧致
    try:
        vol_spike = float(vol_spike_ratio) if vol_spike_ratio is not None else 1.0
        daily_chg = float(daily_change_pct) if daily_change_pct is not None else 0.02
        # 如果是字符串 '0.00124'，转为 float；如果转换失败或为 None，默认 1.0 (不紧致)
        price_tightness = float(raw_price_tightness) if raw_price_tightness is not None else 1.0
    except (ValueError, TypeError):
        vol_spike, daily_chg, price_tightness = 1.0, 0.02, 1.0

    # 异常放量预警 (Churning / Stalling) ===
    # 逻辑：成交量是均量的 2 倍以上，但股价变动小于 1%，且 OBV 并没有大幅走强
    # 这通常意味着主力在高位对倒出货
    if vol_spike > 2.0 and daily_chg < 0.01 and (obv_s20 <= STRONG or ad_s20 <= STRONG):
        return "高位滞涨(出货嫌疑)"

    # --- 1️⃣ 核心新增：VCP 极度缩量（隐蔽吸筹 - WWD型） ---
    # 逻辑：价格波幅极其紧致（<4%）且成交量极度萎缩（比均量少30%以上）
    if price_tightness < 0.045 and vol_rs_vcp < 0.75:
        return "极度缩量(隐蔽吸筹)"

    # --- 2️⃣ 明确吸筹 (主动进攻型) ---
    if obv_s20 > STRONG and ad_s20 > STRONG:
        if obv_s5 > 0:
            return "明确吸筹"
        return "强趋势回撤"

    # --- 3️⃣ 高位放量震荡 (WS预警型) ---
    # 逻辑：如果成交量很大(vol_rs_vcp > 1.2)，但价格波幅很大(>8%)，说明分歧严重
    if vol_rs_vcp > 1.2 and price_tightness > 0.08:
        # ✅ 新增：如果资金在“加速流入”，判为趋势加速，而不是分歧
        if is_fund_accelerating:
            return "趋势加速放量"
        # ❌ 否则，才是真正的高位分歧
        return "高位放量分歧"

    # --- 4️⃣ 其他原逻辑 ---
    if obv_s20 > WEAK and ad_s20 < -WEAK:
        if market_regime == "多头":
            return "趋势中资金分歧"
        return "控盘减弱预警"

    # 3️⃣ 底部试探
    if obv_s20 < -WEAK and obv_s5 > STRONG and ad_s5 > WEAK:
        return "底部试探"

    # 4️⃣ 明确派发
    if obv_s20 < -STRONG and ad_s20 < -STRONG:
        return "派发阶段"

    return "量价中性"


# =========================
# V3 修正版：量价交易资格判定（返回二元组）
# =========================
def obv_ad_trade_gate(
    obv_ad_label: str,
    trend_strength: str,
    trend_stage: str,
    trend_activity: str,
    atr5: float,
    atr20: float,
    market_cap: float
):
    """
    基于 trend_strength + trend_stage + trend_activity + OBV/AD 的风险导向交易闸门
    返回：
        (allow_trade: bool, action_hint: str)
    """

    # ==================================================
    # 1️⃣ 趋势结构前置过滤（硬条件）
    # ==================================================
    tradable_trends = {
        "strong_uptrend",
        "uptrend",
        "trend_pullback",
    }

    if trend_strength not in tradable_trends:
        return False, "非趋势结构，仅观察"

    # ==================================================
    # 2️⃣ 趋势是否真实在“走”
    # ==================================================
    if trend_activity == "no_trend":
        return False, "趋势未启动，仅观察"

    # ==================================================
    # 3️⃣ 生命周期级别硬风险
    # ==================================================
    if trend_stage == "distribution":
        return False, "趋势进入派发期，禁止交易"

    if obv_ad_label == "派发阶段":
        return False, "资金明确派发，禁止交易"
    
    # ==================================================
    # 4️⃣ 高风险量价结构（强烈压制）
    # ==================================================
    if obv_ad_label in {"高位放量分歧", "控盘减弱预警"}:
        if trend_activity == "trend_stalling":
            return False, "趋势停滞+资金分歧，禁止交易"
        if trend_stage == "late":
            return False, "高位资金博弈，避免参与"
        return False, "资金结构恶化，仅观察"

    # ==================================================
    # 5️⃣ 生命周期分阶段处理（⭐核心修改区）
    # ==================================================

    # 波动变化率
    # | atr5/atr20 | 市场状态             |
    # | ---------- | ---------------- |
    # | < 0.8      | 极度压缩（VCP / flag） |
    # | 0.8–1.2    | 正常               |
    # | > 1.5      | 波动扩张             |
    # | > 2.0      | 爆发 / 新闻驱动        |
    atr_expansion_ratio = atr5 / atr20 if atr20 > 0 else None
    
    # 大市值判断（与Stage 2一致）
    is_large_cap = False
    try:
        market_cap = market_cap if market_cap else None
        is_large_cap = market_cap is not None and market_cap >= 100000000000  # ≥1000亿美元
    except:
        pass

    # late stage 阈值动态化
    late_atr_threshold = 1.80 if is_large_cap else 1.60

    # ---------- late：高位只允许“强中强” ----------
    if trend_stage == "late":
        # 修正：如果资金在疯狂吸筹或加速，这是 Climax Run 的特征，允许参与
        if (
            obv_ad_label in {"明确吸筹", "趋势加速放量"}
            and atr_expansion_ratio is not None
            and atr_expansion_ratio < late_atr_threshold
        ):
            return True, "高位加速，严设止损"
        
        if (
            obv_ad_label == "强趋势回撤"
            and trend_activity == "trend_active"
        ):
            return True, "高位回撤，轻仓参与"
        return False, "高位阶段或趋势停滞，风险偏大"

    # ---------- mid：主升浪（允许换手分歧） ----------
    if trend_stage == "mid":

        # 🚫 只有“趋势停滞 + 分歧”才否决
        if (
            trend_activity == "trend_stalling"
            and obv_ad_label in {"高位放量分歧", "控盘减弱预警", "趋势中资金分歧"}
        ):
            return False, "趋势停滞且资金分歧，等待确认"

        # ✅ 主升浪允许的资金结构
        if obv_ad_label in {
            "明确吸筹",
            "极度缩量(隐蔽吸筹)",
            "强趋势回撤",
            "趋势加速放量",
            "高位放量分歧",   # ⭐关键放行
        }:
            return True, "主升浪建仓/加仓"

        if obv_ad_label in {
            "底部试探",
        }:
            return True, "主升浪小仓试探"

        return False, "量价支持不足"

    # ---------- early：趋势初期 ----------
    if trend_stage == "early":
        if obv_ad_label in {
            "明确吸筹",
            "极度缩量(隐蔽吸筹)",
            "底部试探",
            "强趋势回撤",
        }:
            return True, "趋势初期建仓"

        if obv_ad_label == "趋势中资金分歧":
            return True, "早期分歧，小仓"

        return False, "早期信号不足"

    # ==================================================
    # 6️⃣ unknown 生命周期（保守）
    # ==================================================
    if trend_stage == "unknown":
        if (
            trend_strength == "strong_uptrend"
            and obv_ad_label in {
                "明确吸筹",
                "极度缩量(隐蔽吸筹)",
            }
        ):
            return True, "阶段不明，小仓试探"

        return False, "阶段不明，仅观察"

    # ==================================================
    # 7️⃣ 兜底
    # ==================================================
    return False, "仅跟踪"


# =========================
# ADX：用标准 Wilder 定义
# =========================
def compute_adx(df, period=14):
    """
    计算 ADX (平均趋向指标)
    采用 Wilder's Smoothing (RMA) 算法，这是标准技术分析软件的通用实现。
    """
    if len(df) < period * 2:
        return pd.Series(index=df.index, data=np.nan)

    high = df["high"]
    low = df["low"]
    close = df["close"]

    # 1. 计算 +DM 和 -DM (趋向变动值)
    plus_dm_raw = high.diff()
    minus_dm_raw = low.diff()

    # 判定逻辑：只有当高点上移幅度大于低点下移幅度，且大于0时，+DM有效
    plus_dm = np.where((plus_dm_raw > -minus_dm_raw) & (plus_dm_raw > 0), plus_dm_raw, 0.0)
    # 判定逻辑：只有当低点下移幅度大于高点上移幅度，且大于0时，-DM有效
    minus_dm = np.where((-minus_dm_raw > plus_dm_raw) & (-minus_dm_raw > 0), -minus_dm_raw, 0.0)

    # 2. 计算 TR (真实波幅)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    # 3. 使用 Wilder's Smoothing 平滑处理 (EWM 算法)
    # Wilder 的 alpha 等于 1/period
    tr_smoothed = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_dm_smoothed = pd.Series(plus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean()
    minus_dm_smoothed = pd.Series(minus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean()

    # 4. 计算 +DI 和 -DI
    plus_di = 100 * (plus_dm_smoothed / tr_smoothed)
    minus_di = 100 * (minus_dm_smoothed / tr_smoothed)

    # 5. 计算 DX 并进一步平滑得到 ADX
    # 处理分母为0的极端情况
    dx_denom = plus_di + minus_di
    dx = 100 * (abs(plus_di - minus_di) / dx_denom.replace(0, np.nan))
    dx = dx.fillna(0)
    
    adx = dx.ewm(alpha=1/period, adjust=False).mean()

    return adx


# =========================
# 为单只股票计算最新 ADX（行级）
# =========================
def compute_latest_adx_from_row(
    row,
    price_history_map: dict,
    period: int = 14,
    min_bars: int = 60
) -> float:
    """
    为单一股票计算“当前时刻”的 ADX 值
    设计用于 DataFrame.apply（行级）
    """

    code = row.get("stock_code")
    hist = price_history_map.get(code)

    if hist is None or len(hist) < min_bars:
        return float("nan")

    # 只取最近一段，避免无意义的全历史计算
    h = hist.tail(120).copy()

    # 确保必要字段存在
    required_cols = {"high", "low", "close"}
    if not required_cols.issubset(h.columns):
        return float("nan")

    adx_series = compute_adx(h, period=period)

    # 取最后一个有效 ADX
    adx_series = adx_series.dropna()
    if adx_series.empty:
        return float("nan")

    return float(adx_series.iloc[-1])


def classify_trend_activity_from_row(
    row,
    adx_strong: float = 25,
    adx_weak: float = 18
) -> str:
    """
    二阶趋势确认（行级）
    返回：
    - trend_active     → 趋势真实在推进
    - trend_stalling   → 有趋势结构，但在震荡/分歧
    - no_trend         → 非趋势环境
    """

    trend_strength = row.get("trend_strength")
    adx_value = row.get("adx")
    market_cap = row.get("market_cap", 0)

    if pd.isna(adx_value) or trend_strength is None:
        return "no_trend"
    
    # 大市值特权：市值大于 5000 亿美金，ADX 门槛大幅降低
    if market_cap is not None and market_cap >= 5e11:
        adx_strong = 18  # 巨头有18的ADX就已经很强了
        adx_weak = 12

    # === 强趋势结构 ===
    if trend_strength in {"strong_uptrend", "trend_pullback"}:
        if adx_value >= adx_strong:
            return "trend_active"
        elif adx_value >= adx_weak:
            return "trend_stalling"
        else:
            return "no_trend"

    # === 普通上升趋势 ===
    if trend_strength == "uptrend":
        if adx_value >= adx_strong:
            return "trend_active"
        elif adx_value >= adx_weak:
            return "trend_stalling"
        else:
            return "no_trend"

    return "no_trend"


# =========================
# 方向是否明确？
# EMA20 > EMA50 → 上行结构
# EMA20 < EMA50 → 下行结构
# 结构是否稳定?
# ADX 高 → 价格在“走趋势”
# ADX 低 → 价格在“来回震荡”
# 当前处在什么地形？
# trend_strength	地形隐喻	含义
# strong_uptrend	高速公路	可以执行几乎所有多头资金信号
# uptrend	        普通公路	可执行高质量资金信号
# sideways	        平地/沙地	只观察，不冲锋
# downtrend	        下坡路	    禁止做多
# =========================
def compute_trend_strength_from_row(
    row,
    price_history_map: dict,
    min_bars: int = 60
) -> str:
    """
    实盘级趋势强度判定（稳健 / 非未来函数）
    返回枚举：
    - strong_uptrend
    - uptrend
    - trend_pullback
    - range
    - downtrend
    - unknown
    """

    code = row.get("stock_code")
    hist = price_history_map.get(code)

    if hist is None or len(hist) < min_bars:
        return "unknown"

    # === 取最近数据 ===
    h = hist.tail(120).copy()

    close = h["close"]
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    last_close = close.iloc[-1]
    last_ma20 = ma20.iloc[-1]
    last_ma50 = ma50.iloc[-1]
    last_ma200 = ma200.iloc[-1] if len(ma200.dropna()) > 0 else None

    # === 均线斜率（避免用 pct_change 过激） ===
    ma20_slope = (ma20.iloc[-1] - ma20.iloc[-6]) / ma20.iloc[-6]
    ma50_slope = (ma50.iloc[-1] - ma50.iloc[-11]) / ma50.iloc[-11]

    # === 回撤幅度 ===
    recent_high = close.tail(30).max()
    pullback_pct = (recent_high - last_close) / recent_high

    # ==========================================================
    # 1️⃣ 强上升趋势
    # ==========================================================
    if (
        last_close > last_ma20 > last_ma50
        and (last_ma200 is None or last_ma50 > last_ma200)
        and ma20_slope > 0.003   # 原来是 0.01，大盘股 5 天涨 1% 太苛刻，改为 0.3%
        and ma50_slope > 0.002   # 原来是 0.005，改为 0.2%
    ):
        return "strong_uptrend"

    # ==========================================================
    # 2️⃣ 趋势中健康回撤（⭐核心交易区）
    # ==========================================================
    if (
        last_ma20 >= last_ma50
        and ma50_slope > 0
        and 0.03 <= pullback_pct <= 0.18
    ):
        return "trend_pullback"

    # ==========================================================
    # 3️⃣ 普通上升趋势
    # ==========================================================
    if (
        last_close > last_ma50
        and ma50_slope > 0
    ):
        return "uptrend"

    # ==========================================================
    # 4️⃣ 明确下行
    # ==========================================================
    if (
        last_close < last_ma50
        and ma50_slope < 0
    ):
        return "downtrend"

    # ==========================================================
    # 5️⃣ 震荡区间
    # ==========================================================
    return "range"


# =========================
# V3：准实盘综合评分模型
# =========================
# '明确吸筹', '强趋势回撤', '趋势中资金分歧', '控盘减弱预警', '底部试探', '派发阶段', '量价中性'
OBV_SCORE_MAP = {
    # === 主动进攻型 ===
    "明确吸筹": 1.00,           # 最理想：趋势 + 资金 + 共振
    "极度缩量(隐蔽吸筹)": 0.98,  # 这种形态通常是爆发前夜，极具价值
    "趋势加速放量": 0.90,       # 强势趋势中的加速，极高价值
    # === 趋势中健康结构 ===
    "强趋势回撤": 0.85,        # 上升趋势中的洗盘，极高价值
    "底部试探": 0.75,          # 早期资金介入，允许小仓位
    # === 中性 / 观察区 ===
    "量价中性": 0.60,          # 盘整期，留在雷达内
    "趋势中资金分歧": 0.50,    # 内外资分歧，需等待确认
    "高位放量分歧": 0.35,        # 调低分数，规避类似 WS 的假信号
    # === 风险预警区 ===
    "控盘减弱预警": 0.30,      # 不宜新开仓，防止诱多
    # === 明确回避 ===
    "派发阶段": 0.00           # 资金持续流出
}

# =========================
# V3 动态量价评分模型
# =========================
def get_stateful_obv_score(
    obv_label: str,
    trend_activity: str
) -> float:
    """
    根据 资金形态 × 趋势活动度 动态给分
    """

    # === 基础分（你原有逻辑，作为锚点） ===
    base_score = OBV_SCORE_MAP.get(obv_label, 0.5)

    # === 状态调制矩阵 ===
    # 行为 × 状态 → 权重因子
    STATE_MULTIPLIER = {
        # ===== 明确进攻型 =====
        "明确吸筹": {
            "trend_active": 1.15,     # A+ 信号
            "trend_stalling": 0.90,   # B 信号
            "no_trend": 0.60          # 雷达
        },
        "极度缩量(隐蔽吸筹)": {
            "trend_active": 1.10,
            "trend_stalling": 1.00,   # ⭐ 这里反而不减分（VCP 常见）
            "no_trend": 0.70
        },

        # ===== 趋势中结构 =====
        "强趋势回撤": {
            "trend_active": 1.05,
            "trend_stalling": 0.85,
            "no_trend": 0.60
        },
        "底部试探": {
            "trend_active": 0.90,
            "trend_stalling": 0.80,
            "no_trend": 0.65
        },

        # ===== 分歧 / 高风险 =====
        "趋势中资金分歧": {
            "trend_active": 0.75,
            "trend_stalling": 0.60,
            "no_trend": 0.40
        },
        "高位放量分歧": {
            "trend_active": 0.55,     # ⭐ 关键：趋势在走 ≠ 立刻死刑
            "trend_stalling": 0.30,   # WS / CDE 的真实位置
            "no_trend": 0.15
        },
        "高位滞涨(出货嫌疑)": {
            "trend_active": 0.20,    # 即使趋势还在，也是强弩之末
            "trend_stalling": 0.05,  # 几乎死刑
            "no_trend": 0.00         # 死刑
        },
        "控盘减弱预警": {
            "trend_active": 0.45,
            "trend_stalling": 0.25,
            "no_trend": 0.10
        },

        # ===== 明确派发 =====
        "派发阶段": {
            "trend_active": 0.10,
            "trend_stalling": 0.05,
            "no_trend": 0.00
        }
    }

    state_map = STATE_MULTIPLIER.get(obv_label)
    if not state_map:
        return base_score

    multiplier = state_map.get(trend_activity, 0.6)
    return base_score * multiplier


def compute_trade_score(row, sector_avg_rs: dict) -> float:
    """
    V3.1：线性 Alpha Trade Score（修复极端 0 / 100）
    集成了 VCP 紧致度奖励与板块风险过滤
    """

    allow_trade = row.get("allow_trade", False)

    # === 技术结构 ===
    # OBV 动态评分
    obv_score = get_stateful_obv_score(
        row.get("obv_ad_interpretation"),
        row.get("trend_activity")
    )

    # 逻辑增强：引入 VCP 临门一脚的“价格紧致度”奖励
    # 股价 5 日波幅越小，筹码锁定越好，赋予额外技术加分
    tightness = 1.0 # 默认不紧致
    raw_tightness = row.get("price_tightness")
    try:
        # 如果是字符串 '0.00124'，转为 float；如果转换失败或为 None，默认 1.0 (不紧致)
        tightness = float(raw_tightness) if raw_tightness is not None else 1.0
    except (ValueError, TypeError):
        tightness = 1.0
    tightness_bonus = 0.0
    if tightness < 0.045:  # VCP 标准：5日内波幅小于 4.5%
        tightness_bonus = 0.08
    elif tightness < 0.07: # 次优紧致度
        tightness_bonus = 0.03

    rs_raw = min(row.get("rs_rank", 50) / 100.0, 1.0)
    rs_score = rs_raw ** 1.3

    # 综合技术分：OBV + RS + 紧致度奖励
    technical_score = (obv_score + tightness_bonus) * 0.6 + rs_score * 0.4

    # === 基本面 ===
    canslim_score = min(row.get("canslim_score", 0) / 5.0, 1.0)
    base_score = technical_score * 0.6 + canslim_score * 0.4

    # === 趋势偏置（替代乘数） ===
    trend_bias_map = {
        "strong_uptrend": 0.08,
        "uptrend": 0.05,
        "trend_pullback": 0.02,
        "range": -0.05,
        "downtrend": -0.15,
    }
    trend_strength = row.get("trend_strength", "unknown")
    base_score += trend_bias_map.get(trend_strength, 0.0)

    # === 财报风险动态调节 ===
    # 逻辑：如果财报在未来 5 天内，且当前不是"隐蔽吸筹"，则大幅扣分以避险
    earnings_penalty = 1.0
    next_earnings = row.get("next_earnings_date")
    if next_earnings:
        try:
            # 确保是 date 对象
            if isinstance(next_earnings, str):
                next_earnings = datetime.strptime(next_earnings, "%Y-%m-%d").date()
            
            days_to_earnings = (next_earnings - date.today()).days
            
            if 0 <= days_to_earnings <= 5:
                print(f"⚠️ {row.get('stock_code')} 财报临近 ({days_to_earnings}天)，触发避险扣分")
                earnings_penalty = 0.5 # 砍掉一半分数
            elif 0 <= days_to_earnings <= 14:
                earnings_penalty = 0.85 # 提醒性质扣分
        except:
            pass

    if row.get("obv_ad_interpretation") == "极度缩量(隐蔽吸筹)":
        earnings_penalty = 1.0  # 吸筹形态豁免
    base_score *= earnings_penalty

    # === 板块强度过滤 (解决 WS 假信号关键) ===
    # 利用传入的 sector_avg_rs 字典进行比对
    stock_sector = row.get("sector")
    avg_sector_rs = sector_avg_rs.get(stock_sector, 50.0)
    
    sector_multiplier = 1.0
    # 逻辑：如果个股 RS 远高于行业平均，说明是领头羊（加分）
    # 如果行业平均 RS 低于 55，说明板块整体走弱，个股容易被拖累（减分）
    if avg_sector_rs < 55.0:
        sector_multiplier = 0.85  # 板块平庸，得分打 85 折
    elif avg_sector_rs > 75.0:
        sector_multiplier = 1.05  # 板块强势，得分加成 5%

    base_score *= sector_multiplier

    # === 期权情绪（保留，但限制放大） ===
    option_mult = option_sentiment_multiplier(row)
    option_mult = min(option_mult, 1.1)
    base_score *= option_mult

    # === allow_trade 软惩罚 ===
    if not allow_trade:
        base_score *= 0.6

    final_score = max(0.0, min(base_score * 100, 100.0))

    # === 终极硬风险修正（交易状态） ===
    trade_state = row.get("trade_state")
    if "禁止交易" in trade_state:
        final_score = min(final_score, 40)
    elif "阶段不明" in trade_state:
        final_score = min(final_score, 75)
    elif "非趋势结构" in trade_state:
        final_score = min(final_score, 65)
    
    # ====== 主升浪 + 极强 RS 的 score 上限释放 ======
    trend_stage = row.get("trend_stage")
    rs_rank = row.get("rs_rank")
    if (
        trade_state == "主升浪建仓/加仓"
        and rs_rank is not None
        and rs_rank >= 95
        and trend_stage == "mid"
    ):
        final_score = max(final_score, 80)

    # === Extension 修正 (新增) ===
    if row.get("trend_stage") == "extended(超买)":
        final_score = min(final_score, 60) # 强制压低评分

    return round(final_score, 2)


# =========================
# V3.1 修改：期权风险保险丝（结构化 UOA）
# =========================
def options_risk_gate(row) -> bool:
    """
    第一层：期权风险保险丝（V3.1）
    机构逻辑：
    - 只拦截「末端狂欢」
    - 不拦截「机构趋势建仓」
    """

    pc = row.get("opt_pc_ratio")
    iv = row.get("opt_avg_iv")

    # === V3.1 新增：结构化 UOA ===
    uoa_type = row.get("opt_uoa_type", "none")
    uoa_score = row.get("opt_uoa_score", 0.0)
    uoa_call_bias = row.get("opt_uoa_call_bias", 0.0)

    # === 兼容旧逻辑 ===
    uoa_detected = row.get("opt_uoa_detected", False)

    # 数据缺失 → 不否决
    if pc is None or iv is None:
        return True

    # === V3.1 核心否决条件 ===
    # 极端情绪 + IV 透支 + 明显事件型期权
    if (
        pc < 0.4
        and iv > 1.1
        and (
            uoa_type == "event"
            or (uoa_detected and uoa_score < 0.3)
        )
    ):
        # 这是「末尾狂欢」而不是机构建仓
        return False

    return True


# =========================
# V3.1 修改：期权情绪调制因子（结构化 UOA）
# =========================
def option_sentiment_multiplier(row) -> float:
    """
    第二层：期权情绪调制因子（V3.1）
    用于放大 / 压缩 trade_score
    """

    multiplier = 1.0

    pc = row.get("opt_pc_ratio")
    iv = row.get("opt_avg_iv")

    # === V3.1 新增：结构化 UOA ===
    uoa_type = row.get("opt_uoa_type", "none")
    uoa_score = row.get("opt_uoa_score", 0.0)
    uoa_call_bias = row.get("opt_uoa_call_bias", 0.0)

    # === 1️⃣ Put/Call 情绪（顺势） ===
    if pc is not None:
        if pc < 0.6:
            multiplier *= 1.15
        elif pc > 1.3:
            multiplier *= 0.80

    # === 2️⃣ IV 风险（真正过热） ===
    if iv is not None and iv > 0.95:
        multiplier *= 0.75

    # === 3️⃣ 机构级 UOA 加权（核心升级） ===
    if (
        uoa_type == "institutional"
        and uoa_score > 0.4
        and uoa_call_bias > 0
    ):
        # 趋势型机构建仓
        multiplier *= (1.0 + min(uoa_score, 0.3))

    # === 4️⃣ 事件型 UOA 惩罚 ===
    if uoa_type == "event":
        multiplier *= 0.7

    # === 5️⃣ 方向不一致惩罚 ===
    if uoa_call_bias < 0:
        multiplier *= 0.8

    # === 最大上限保护 ===
    multiplier = min(multiplier, 1.3)

    return multiplier


# =========================
# V3 新增：期权情绪状态枚举
# =========================
class OptionSentimentState(Enum):
    NEUTRAL = "neutral"                 # 正常              不影响
    BULLISH_BUT_CROWDED = "crowded"     # 看多但拥挤        降仓/慢买  
    BEARISH_HEDGE = "bearish_hedge"     # 防御性看空        延后/观望
    EVENT_RISK = "event_risk"           # 事件风险          禁止追
    SMART_MONEY_BULLISH = "smart_bull"  # 聪明钱偏多        可加权


# =========================
# V3 新增：期权情绪状态中文映射
# =========================
OPTION_STATE_CN_MAP = {
    OptionSentimentState.NEUTRAL: "中性",
    OptionSentimentState.BULLISH_BUT_CROWDED: "多头拥挤",
    OptionSentimentState.BEARISH_HEDGE: "防御性对冲",
    OptionSentimentState.EVENT_RISK: "事件风险",
    OptionSentimentState.SMART_MONEY_BULLISH: "机构偏多",
}


# =========================
# V3.1 修改：期权情绪状态解析（结构化 UOA）
# =========================
def resolve_option_sentiment_state(row) -> OptionSentimentState:
    """
    将期权原始指标 → 离散状态（V3.1）
    """

    pc = row.get("opt_pc_ratio")
    iv = row.get("opt_avg_iv")
    rs_rank = row.get("rs_rank", 0)

    # === V3.1 新增：结构化 UOA ===
    uoa_type = row.get("opt_uoa_type", "none")
    uoa_score = row.get("opt_uoa_score", 0.0)
    uoa_call_bias = row.get("opt_uoa_call_bias", 0.0)

    # 缺数据 → 中性
    if pc is None or iv is None:
        return OptionSentimentState.NEUTRAL

    # === 1️⃣ 事件风险 ===
    if iv > 0.95 and uoa_type == "event":
        return OptionSentimentState.EVENT_RISK

    # === 2️⃣ 机构偏多（最优） ===
    if (
        uoa_type == "institutional"
        and uoa_score > 0.4
        and uoa_call_bias > 0
        and iv < 0.85
    ):
        return OptionSentimentState.SMART_MONEY_BULLISH

    # === 3️⃣ 多头拥挤 ===
    if pc < 0.5:
        if rs_rank > 90 and iv > 0.7:
            return OptionSentimentState.BULLISH_BUT_CROWDED
        else:
            # 非拥挤 → 动量确认
            return OptionSentimentState.SMART_MONEY_BULLISH

    # === 4️⃣ 防御性对冲 ===
    if pc > 1.3 or uoa_call_bias < 0:
        return OptionSentimentState.BEARISH_HEDGE

    return OptionSentimentState.NEUTRAL


def check_data_integrity():
    """
    检查数据库数据完整性
    """
    print("\n" + "="*80)
    print("🔍 数据完整性检查")
    print("="*80)
    con = duckdb.connect(DUCKDB_PATH)
    
    # 1. 检查ticker表
    ticker_count = con.execute("""
        SELECT COUNT(*) FROM stock_ticker
        WHERE type = 'Common Stock'
        AND mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
    """).fetchone()[0]
    print(f"\n📋 Ticker表中的普通股数量: {ticker_count}")
    
    # 2. 检查价格表
    latest_date = con.execute("SELECT MAX(trade_date) FROM stock_price").fetchone()[0]
    print(f"📅 价格表最新日期: {latest_date}")
    
    latest_price_count = con.execute(f"""
        SELECT COUNT(DISTINCT stock_code)
        FROM stock_price
        WHERE trade_date = DATE '{latest_date}'
    """).fetchone()[0]
    print(f"📊 最新日期有价格的股票数: {latest_price_count}")
    
    # 3. 检查各交易所分布
    exchange_dist = con.execute(f"""
        SELECT t.mic, COUNT(DISTINCT p.stock_code) as cnt
        FROM stock_price p
        INNER JOIN stock_ticker t ON p.stock_code = t.symbol
        WHERE p.trade_date = DATE '{latest_date}'
        AND t.type = 'Common Stock'
        GROUP BY t.mic
        ORDER BY cnt DESC
    """).fetchdf()
    
    if not exchange_dist.empty:
        print(f"\n📈 各交易所分布:")
        for _, row in exchange_dist.iterrows():
            print(f"   {row['mic']}: {row['cnt']} 支")
    
    # 4. 检查yf_price_available标记
    unavailable_count = con.execute("""
        SELECT COUNT(*)
        FROM stock_ticker
        WHERE type = 'Common Stock'
        AND mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
        AND yf_price_available = FALSE
    """).fetchone()[0]
    print(f"\n⚠️  被标记为yf不可用的股票: {unavailable_count}")
    
    # 5. 检查基本面数据
    fundamental_count = con.execute("SELECT COUNT(DISTINCT(stock_code)) FROM stock_fundamentals").fetchone()[0]
    print(f"📊 有基本面数据的股票: {fundamental_count}")
    
    # 6. 推断问题
    print("\n" + "="*80)
    print("🔍 诊断结果:")
    print("="*80)
    
    if ticker_count > 1000 and latest_price_count < 100:
        print("❌ 严重问题：Ticker表有大量股票，但价格数据极少！")
        print("   可能原因：")
        print("   1. fetch_all_prices() 从未完整运行过")
        print("   2. yf_price_available 被错误标记")
        print("   3. 数据下载被中断")
        print("\n💡 建议：")
        print("   - 运行 fetch_all_prices() 进行全量下载")
        print("   - 或者重置 yf_price_available 标记")
    elif latest_price_count < 50:
        print("⚠️  警告：可用股票数量过少，无法进行有效筛选")
        print(f"   当前只有 {latest_price_count} 支股票有数据")
    else:
        print(f"✅ 数据看起来正常，有 {latest_price_count} 支股票可供筛选")
    
    print("="*80 + "\n")

    # 如果数据严重不足，给出警告和选项
    if latest_price_count < 100:
        print("\n" + "="*80)
        print("⚠️  警告：数据库中股票数量严重不足！")
        print("="*80)
        print("\n请选择以下操作之一：")
        print("1. 全量下载所有股票价格（首次运行，耗时较长）")
        print("2. 重置yf标记后增量更新")
        print("3. 忽略警告继续运行（仅用于测试）")
        print("\n如需执行操作1或2，请在代码中取消相应注释")
        print("="*80 + "\n")
        
        # 🔥 选项1：全量下载（取消下面注释）
        # fetch_all_prices()
        
        # 🔥 选项2：重置标记（取消下面注释）
        # reset_yf_availability(con)
        # update_recent_prices([])  # 空列表=更新所有缺失的
        
        # 🔥 选项3：继续运行（默认）
        print("⏭️  继续使用现有数据运行...")
    
    con.close()
    return latest_price_count


def diagnose_stage2_filters(target_date):
    """
    修复后的诊断函数 - 使用正确的f-string格式化
    """
    
    print("\n" + "="*80)
    print("🔍 Stage2 筛选条件诊断报告")
    print("="*80)

    con = duckdb.connect(DUCKDB_PATH)
    
    # 🔥 修复：使用f-string格式化所有SQL
    total_sql = f"""
        SELECT COUNT(DISTINCT p.stock_code) as cnt
        FROM stock_price p
        LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
        WHERE
            p.trade_date = DATE '{target_date}'
            AND t.type = 'Common Stock'
            AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
            AND COALESCE(t.yf_price_available, TRUE) = TRUE
    """
    total = con.execute(total_sql).fetchone()[0]
    print(f"\n📊 基准：有最新价格数据的股票总数 = {total}")
    
    if total == 0:
        print("\n❌ 错误：没有找到任何符合条件的股票！")
        print("   请先运行数据完整性检查")
        return
    
    # 测试各个条件
    conditions = [
        ("均线排列: close > ma50 > ma200", f"""
            WITH base AS (
                SELECT
                    p.stock_code,
                    p.trade_date,
                    p.close,
                    AVG(p.close) OVER (
                        PARTITION BY p.stock_code
                        ORDER BY p.trade_date
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS ma50,
                    AVG(p.close) OVER (
                        PARTITION BY p.stock_code
                        ORDER BY p.trade_date
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS ma200
                FROM stock_price p
                JOIN stock_ticker t ON p.stock_code = t.symbol
                WHERE
                    t.type = 'Common Stock'
                    AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                    AND COALESCE(t.yf_price_available, TRUE) = TRUE
            )
            SELECT COUNT(*)
            FROM base
            WHERE
                trade_date = DATE '{target_date}'
                AND close > ma50
                AND ma50 > ma200;
        """),
        
        ("52周位置: ≥1.25*low_52w AND ≥0.55*high_52w", f"""
            WITH base AS (
                SELECT
                    p.stock_code,
                    p.close,
                    MAX(p.high) OVER (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 251 PRECEDING) AS high_52w,
                    MIN(p.low) OVER (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 251 PRECEDING) AS low_52w
                FROM stock_price p
                LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
                WHERE
                    p.trade_date = DATE '{target_date}'
                    AND t.type = 'Common Stock'
                    AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                    AND COALESCE(t.yf_price_available, TRUE) = TRUE
            )
            SELECT COUNT(*) FROM base
            WHERE close >= 1.25 * low_52w AND close >= 0.55 * high_52w
        """),
        
        ("RS排名: ≥65", f"""
            WITH base AS (
                SELECT
                    p.stock_code,
                    p.trade_date,
                    p.close,
                    COUNT(*) OVER w_all AS trading_days
                FROM stock_price p
                LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
                WHERE
                    t.type = 'Common Stock'
                    AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                    AND COALESCE(t.yf_price_available, TRUE) = TRUE
                WINDOW w_all AS (PARTITION BY p.stock_code)
            ),
            returns AS (
                SELECT
                    stock_code,
                    trade_date,
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
            )
            SELECT COUNT(*) FROM rs_scores
            WHERE trade_date = DATE '{target_date}' AND rs_rank >= 65
        """),
        
        ("成交量: 20日均量 > 10万股", f"""
            WITH vol AS (
                SELECT
                    p.stock_code,
                    AVG(p.volume) OVER (PARTITION BY p.stock_code ORDER BY p.trade_date ROWS 19 PRECEDING) AS vol20
                FROM stock_price p
                LEFT JOIN stock_ticker t ON p.stock_code = t.symbol
                WHERE
                    p.trade_date = DATE '{target_date}'
                    AND t.type = 'Common Stock'
                    AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                    AND COALESCE(t.yf_price_available, TRUE) = TRUE
            )
            SELECT COUNT(*) FROM vol WHERE vol20 > 100000
        """),
        
        ("基本面数据存在 + 市值≥10亿", """
            SELECT COUNT(DISTINCT f.stock_code)
            FROM stock_fundamentals f
            INNER JOIN stock_ticker t ON f.stock_code = t.symbol
            WHERE
                t.type = 'Common Stock'
                AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                AND f.market_cap >= 1e9
        """),
        
        ("ATR波动收缩: atr5/atr20 < 1.05", f"""
            WITH atr_raw AS (
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
            atr_stats AS (
                SELECT
                    stock_code,
                    trade_date,
                    AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 4 PRECEDING) AS atr5,
                    AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 19 PRECEDING) AS atr20
                FROM atr_raw
            )
            SELECT COUNT(*)
            FROM atr_stats a
            LEFT JOIN stock_ticker t ON a.stock_code = t.symbol
            WHERE
                a.trade_date = DATE '{target_date}'
                AND t.type = 'Common Stock'
                AND t.mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
                AND (a.atr5 / NULLIF(a.atr20, 0)) < 1.05
        """)
    ]
    
    print("\n" + "-"*80)
    print("逐项筛选结果：")
    print("-"*80)
    
    for name, sql in conditions:
        try:
            count = con.execute(sql).fetchone()[0]
            pass_rate = (count / total * 100) if total > 0 else 0
            
            # 🎨 彩色输出
            if pass_rate > 20:
                status = "✅"
            elif pass_rate > 10:
                status = "⚠️ "
            else:
                status = "❌"
            
            print(f"\n{status} {name}")
            print(f"  通过数量: {count:>6} / {total}  ({pass_rate:>5.1f}%)")
            print(f"  被过滤: {total - count:>6} 支")
        except Exception as e:
            print(f"\n✗ {name}")
            print(f"  错误: {str(e)[:100]}")
    
    con.close()
    
    print("\n" + "="*80)
    print("💡 建议：")
    print("-"*80)
    print("1. 如果某个条件通过率 < 10%，考虑放宽该条件")
    print("2. 如果'基本面数据存在'通过率很低，需要先运行 update_fundamentals()")
    print("3. 多头市场建议通过率在 5-15% 之间（即筛选出50-200支股票）")
    print("="*80 + "\n")


def reset_yf_availability():
    """
    重置所有股票的yf_price_available标记
    用于修复被错误标记的股票
    """
    print("\n🔄 重置yf_price_available标记...")
    
    con = duckdb.connect(DUCKDB_PATH)
    
    # 重置所有标记
    con.execute("""
        UPDATE stock_ticker
        SET yf_price_available = TRUE
        WHERE type = 'Common Stock'
        AND mic IN ('XNYS','XNGS','XNAS','XASE','ARCX','BATS','IEXG')
    """)

    con.close()
    print("   现在可以重新运行 fetch_all_prices() 或 update_recent_prices()")


def load_all_price_data(
    min_date: str | None = None
) -> pd.DataFrame:
    """
    实盘级行情入口
    - 只做一件事：加载所有可用价格数据
    - 不筛选、不判断、不加工
    """
    con = duckdb.connect(DUCKDB_PATH)

    where_clause = ""
    if min_date:
        where_clause = f"WHERE trade_date >= '{min_date}'"

    sql = f"""
    SELECT
        stock_code,
        trade_date,
        open,
        high,
        low,
        close,
        volume
    FROM stock_price
    {where_clause}
    ORDER BY stock_code, trade_date
    """

    df = con.execute(sql).fetch_df()

    # ====== 基本防御 ======
    if df.empty:
        raise RuntimeError("❌ load_all_price_data: 价格表为空")

    df["trade_date"] = pd.to_datetime(df["trade_date"])

    con.close()

    return df


def build_price_history_map(min_bars: int = 60) -> dict:
    """
    构建股票 → 历史行情映射（实盘安全）
    """
    price_df = load_all_price_data()

    required_cols = {"stock_code", "trade_date", "close"}

    if not required_cols.issubset(price_df.columns):
        raise ValueError(f"price_df 缺少必要字段: {required_cols}")

    price_df = (
        price_df
        .sort_values(["stock_code", "trade_date"])
        .copy()
    )

    price_history_map = {}

    for code, g in price_df.groupby("stock_code"):
        if len(g) >= min_bars:
            price_history_map[code] = g.reset_index(drop=True)

    print(
        f"📦 price_history_map 构建完成："
        f"{len(price_history_map)} / "
        f"{price_df['stock_code'].nunique()} 只股票"
    )

    return price_history_map


def get_us_trading_date(latest_date_in_db):
    """
    根据美东时间 + 数据库最新交易日，判断当前应使用的美股交易日
    """
    et = pytz.timezone("America/New_York")
    now_et = datetime.now(et)

    latest_date = (
        latest_date_in_db
        if isinstance(latest_date_in_db, date)
        else datetime.strptime(latest_date_in_db, "%Y-%m-%d").date()
    )

    # 如果现在还是在 latest_date 的盘前（04:00–09:30 ET）
    premarket_start = et.localize(
        datetime.combine(latest_date, datetime.min.time()).replace(hour=4)
    )
    rth_start = et.localize(
        datetime.combine(latest_date, datetime.min.time()).replace(hour=9, minute=30)
    )

    if premarket_start <= now_et < rth_start:
        return latest_date

    # 如果已经进入下一个交易日的盘前（例如周一凌晨）
    if now_et.date() > latest_date:
        return now_et.date()

    return latest_date


# =========================
# V3 新增：VWAP 和盘前高点获取
# =========================
def get_vwap_and_premarket_high(ticker, target_date):
    try:
        # ---- normalize date ----
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        elif isinstance(target_date, datetime):
            target_date = target_date.date()
        elif not isinstance(target_date, date):
            raise ValueError("Invalid target_date")

        et = pytz.timezone("America/New_York")

        pm_date = get_us_trading_date(target_date)

        # =========================
        # 1️⃣ VWAP: 用 target_date
        # =========================
        df_vwap = yf.download(
            ticker,
            start=target_date,
            end=target_date + timedelta(days=1),
            interval="1m",
            prepost=True,
            progress=False,
            threads=False, 
            auto_adjust=True
        )

        if df_vwap.empty:
            return None, None

        if isinstance(df_vwap.columns, pd.MultiIndex):
            df_vwap.columns = df_vwap.columns.get_level_values(0)

        df_vwap.columns = [c.lower() for c in df_vwap.columns]
        df_vwap = df_vwap.loc[:, ~df_vwap.columns.duplicated()]

        if df_vwap.index.tz is None:
            df_vwap.index = df_vwap.index.tz_localize("UTC").tz_convert(et)
        else:
            df_vwap.index = df_vwap.index.tz_convert(et)

        base = datetime.combine(target_date, datetime.min.time())
        regular_start = et.localize(base.replace(hour=9, minute=30))
        regular_end   = et.localize(base.replace(hour=16, minute=0))

        regular = df_vwap[(df_vwap.index >= regular_start) & (df_vwap.index < regular_end)]

        vwap = None
        if not regular.empty and "volume" in regular.columns:
            vol = regular["volume"]
            if isinstance(vol, pd.DataFrame):
                vol = vol.iloc[:, 0]

            if vol.sum() > 0:
                high = regular["high"]
                low = regular["low"]
                close = regular["close"]

                if isinstance(high, pd.DataFrame):
                    high = high.iloc[:, 0]
                    low = low.iloc[:, 0]
                    close = close.iloc[:, 0]

                typical_price = (high + low + close) / 3
                vwap = (typical_price * vol).sum() / vol.sum()

        # =========================
        # 2️⃣ Premarket High: 用 pm_date
        # =========================
        df_pm = yf.download(
            ticker,
            start=pm_date,
            end=pm_date + timedelta(days=1),
            interval="1m",
            prepost=True,
            progress=False,
            threads=False,
            auto_adjust=True
        )

        if df_pm is None or len(df_pm) == 0:
            return None, None

        premarket_high = None
        if not df_pm.empty:
            if isinstance(df_pm.columns, pd.MultiIndex):
                df_pm.columns = df_pm.columns.get_level_values(0)

            df_pm.columns = [c.lower() for c in df_pm.columns]
            df_pm = df_pm.loc[:, ~df_pm.columns.duplicated()]

            if df_pm.index.tz is None:
                df_pm.index = df_pm.index.tz_localize("UTC").tz_convert(et)
            else:
                df_pm.index = df_pm.index.tz_convert(et)

            pm_base = datetime.combine(pm_date, datetime.min.time())
            pm_start = et.localize(pm_base.replace(hour=4))
            rth_start = et.localize(pm_base.replace(hour=9, minute=30))

            pm = df_pm[(df_pm.index >= pm_start) & (df_pm.index < rth_start)]

            if not pm.empty and "high" in pm.columns:
                high_pm = pm["high"]
                if isinstance(high_pm, pd.DataFrame):
                    high_pm = high_pm.iloc[:, 0]
                premarket_high = high_pm.max()
            
            last_close = regular["close"].iloc[-1] if not regular.empty else None
            if last_close and (premarket_high > last_close * 1.15 or premarket_high < last_close * 0.7):
                premarket_high = None  # 异常值丢弃

        print(
            f"[获取VWAP数据成功] {ticker}: "
            f"VWAP({target_date})={vwap}, "
            f"Premarket High({pm_date})={premarket_high}"
        )

        return (
            float(vwap) if vwap is not None else None,
            float(premarket_high) if premarket_high is not None else None
        )

    except Exception as e:
        print(f"[获取VWAP数据失败] {ticker}: {e}")
        return None, None


def integrate_vwap_and_premarket(final_with_sim, latest_date_in_db):
    """
    集成 VWAP 和盘前高点到 final_with_sim DataFrame
    """
    # Assuming final_with_sim is your pf (processed DataFrame)
    # Get unique tickers (use 'stock_code' column)
    tickers = final_with_sim['stock_code'].unique().tolist()

    # Batch fetch (max_workers=5 to avoid yf rate limits)
    vwap_dict = {}
    premarket_high_dict = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticker = {executor.submit(get_vwap_and_premarket_high, ticker, latest_date_in_db): ticker
                            for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                vwap, pm_high = future.result()
                vwap_dict[ticker] = vwap
                premarket_high_dict[ticker] = pm_high
            except Exception as e:
                print(f"Batch error for {ticker}: {e}")

    # Map back to DataFrame
    final_with_sim['vwap'] = final_with_sim['stock_code'].map(vwap_dict)
    final_with_sim['premarket_high'] = final_with_sim['stock_code'].map(premarket_high_dict)


# =========================
# V3 新增：入场区间计算（含 VIX 调节因子）
# =========================
def calculate_entry_zone_vix(
    close: float,
    vwap: float | None,
    atr: float | None,
    premarket_high: float | None,
    high_5d: float | None,
    trend_strength: str,
    current_vix: float
) -> tuple[str, float]:
    if atr is None or atr <= 0:
        atr = close * 0.03  # 默认波动

    vix_mult = get_vix_multiplier(current_vix)
    adj_atr = atr * vix_mult  # VIX 调节波动宽度

    # 改进 base：优先 vwap，其次 close
    base = vwap if vwap else close

    # 改进 pivot：不再总是取 max high_5d，避免偏高；用加权平均（70% base + 30% high_5d）
    effective_high = high_5d if high_5d else base
    pivot = 0.7 * base + 0.3 * effective_high  # 中性化，减少偏高

    if premarket_high:
        pivot = max(pivot, premarket_high)  # 只在有 premarket 时覆盖

    # 根据 trend_strength 区分偏移
    if trend_strength == "strong_uptrend":
        # 突破型：区间略向上偏移，entry_price 取上限（追小涨）
        entry_low = pivot
        entry_high = pivot + (adj_atr * 0.5)  # 放大到 0.5 以覆盖更多
        entry_price = entry_high  # 建议在上限执行（突破确认）
    elif trend_strength == "uptrend":
        # 中性：区间对称，entry_price 取中点
        entry_low = pivot - (adj_atr * 0.3)
        entry_high = pivot + (adj_atr * 0.3)
        entry_price = pivot  # 中点执行
    elif trend_strength == "trend_pullback":
        # 低吸型：区间向下偏移，entry_price 取下限
        entry_low = pivot - (adj_atr * 0.5)
        entry_high = pivot + (adj_atr * 0.2)
        entry_price = entry_low  # 建议在下限执行（回撤买入）
    else:
        # 其他：保守，窄区间，中点执行
        entry_low = base - (adj_atr * 0.2)
        entry_high = base + (adj_atr * 0.2)
        entry_price = base

    # 极端保护：防止偏离 close 太多
    entry_low = max(entry_low, close * 0.97)  # 不低于 close -3%
    entry_high = min(entry_high, close * 1.03)  # 不高于 close +3%
    entry_price = max(min(entry_price, entry_high), entry_low)

    return f"{entry_low:.2f} - {entry_high:.2f}", round(entry_price, 2)


# =========================
# V3 新增：止损价计算（含 VIX 调节因子）
# =========================
def calculate_hard_stop_vix(
    entry_price: float,
    atr: float | None,
    low_5d: float | None,     # 新增参数：5日最低价，作为技术支撑位
    current_vix: float
) -> float:
    """
    修改逻辑：将止损从“固定点数”改为“技术位支撑 + 波动保护”
    """
    if atr is None or atr <= 0:
        atr = entry_price * 0.03

    vix_mult = get_vix_multiplier(current_vix)
    
    # 1. 基于 ATR 的动态止损 (通常在 1.5 到 2 倍 ATR 之间)
    # VIX 越高，乘数越大，给波动留出呼吸空间
    atr_stop = entry_price - (atr * 1.5 * vix_mult)
    
    # 2. 基于技术结构的止损 (5日最低价下方 0.5%)
    # 逻辑：如果跌破了过去5天的最低点，VCP 形态就彻底走坏了
    tech_support = (low_5d * 0.995) if low_5d else (entry_price * 0.94)
    
    # --- 严谨取值 ---
    # 我们选取 ATR 止损和技术支撑止损中“较近”的一个，但不能太近
    # 保证至少有 2.5% 的空间，防止无谓震仓
    hard_stop = min(atr_stop, tech_support)
    
    # 极端保护：单笔损耗限制在 entry_price 的 8% 以内（防止断崖下跌）
    max_risk_limit = entry_price * 0.92
    hard_stop = max(hard_stop, max_risk_limit)

    return round(hard_stop, 2)


# =========================
# V3 新增：目标价计算（含 VIX 调节因子）
# =========================
def calculate_target_price_vix(
    entry_price: float,       # 由 close 改为 entry_price，更符合盈亏比逻辑
    atr: float | None,
    trend_strength: str,
    rs_rank: float,
    current_vix: float
) -> float:
    if atr is None or atr <= 0:
        atr = entry_price * 0.03

    vix_mult = get_vix_multiplier(current_vix)
    adj_atr = atr * vix_mult

    # 针对 RS_Rank > 90 的领头羊，使用更激进的获利预期
    if trend_strength == "strong_uptrend":
        # 领头羊（RS>90）给 3.0 倍 ATR 空间，普通强趋势给 2.0 倍 (不能太高，防止目标价过高，2.0 为宜)
        atr_mult = 3.0 if rs_rank > 90 else 2.0
    elif trend_strength == "uptrend":
        atr_mult = 1.7
    elif trend_strength == "trend_pullback":
        atr_mult = 1.4
    else:
        atr_mult = 1.1

    target = entry_price + (adj_atr * atr_mult)

    # === 动态上限：不超过 2.8 ATR 或 12%（取小）===
    atr_cap = entry_price + adj_atr * 2.8
    pct_cap = entry_price * 1.12

    return round(min(target, atr_cap, pct_cap), 2)


def calculate_target_price_vix_pm(
    entry_price: float,
    atr: float | None,
    trend_strength: str,
    rs_rank: float,
    current_vix: float,
    premarket_high: float | None = None
) -> float:
    """
    基于现有 calculate_target_price_vix，
    结合盘前高点动态修正 target_price
    """

    # === 1. 原始 target（基于 entry_price） ===
    base_target = calculate_target_price_vix(
        entry_price=entry_price,
        atr=atr,
        trend_strength=trend_strength,
        rs_rank=rs_rank,
        current_vix=current_vix
    )

    # === 2. 没有盘前数据，直接返回 ===
    if premarket_high is None:
        return base_target

    # === 3. 盘前已经突破 entry_price（Gap / 强势）===
    if premarket_high > entry_price * 1.01:
        # 抬高锚点，用盘前高点重新计算
        adjusted_target = calculate_target_price_vix(
            entry_price=premarket_high,
            atr=atr,
            trend_strength=trend_strength,
            rs_rank=rs_rank,
            current_vix=current_vix
        )
        return round(max(adjusted_target, premarket_high * 1.03), 2)

    # === 4. 盘前未突破，仅作为保护下限 ===
    return round(max(base_target, premarket_high), 2)


# =========================
# V3 新增：VIX 波动率调节因子
# =========================
def get_vix_multiplier(current_vix: float) -> float:
    if current_vix >= 30:
        return 1.5
    elif current_vix >= 24:
        return 1.3
    elif current_vix >= 18:
        return 1.15
    else:
        return 1.0


# =========================
# V3 新增：批量注入入场区间、止损价、目标价（含 VIX 调节因子）
# =========================
def apply_entry_stop_target_vix(
    df,
    current_vix: float
):
    """
    VIX-aware Entry / Stop / Target 注入
    保留 atr_15 / vix_adj，确保下游代码不崩
    """

    ideal_entry_list = []
    entry_price_list = []
    hard_stop_list = []
    target_profit_list = []
    atr_15_list = []
    vix_adj_list = []

    vix_factor = get_vix_multiplier(current_vix)

    for _, row in df.iterrows():

        close = row["close"]
        atr = row.get("atr")
        vwap = row.get("vwap")
        premarket_high = row.get("premarket_high")
        high_5d = row.get("high_5d")
        low_5d = row.get("low_5d")
        trend_strength = row.get("trend_strength", "unknown")
        rs_rank = row.get("rs_rank", 0)
        
        # 判断 vwap 是否正常，不正常就用close替代
        vwap_valid = (vwap > 0) and (vwap <= close * 1.5) and (vwap >= close * 0.5) if vwap is not None else False
        vwap = vwap if vwap_valid else close

        # === ATR 兜底 ===
        if atr is None or atr <= 0:
            atr = close * 0.03

        # === Entry ===
        ideal_entry, entry_price = calculate_entry_zone_vix(
            close=close,
            vwap=vwap,
            atr=atr,
            premarket_high=premarket_high,
            high_5d=high_5d,
            trend_strength=trend_strength,
            current_vix=current_vix
        )

        # === Hard Stop ===
        hard_stop = calculate_hard_stop_vix(
            entry_price=entry_price,
            atr=atr,
            low_5d=low_5d,
            current_vix=current_vix
        )

        # === Target ===
        target_profit = calculate_target_price_vix_pm(
            entry_price=entry_price,
            atr=atr,
            trend_strength=trend_strength,
            rs_rank=rs_rank,
            current_vix=current_vix,
            premarket_high=premarket_high
        )

        ideal_entry_list.append(ideal_entry)
        entry_price_list.append(entry_price)
        hard_stop_list.append(hard_stop)
        target_profit_list.append(target_profit)

        # === 兼容旧 schema 的字段 ===
        atr_15_list.append(round(atr, 2))
        vix_adj_list.append(round(vix_factor, 2))

    df = df.copy()
    df["ideal_entry"] = ideal_entry_list
    df["entry_price"] = entry_price_list
    df["hard_stop"] = hard_stop_list
    df["target_profit"] = target_profit_list
    df["atr_15"] = atr_15_list
    df["vix_adj"] = vix_adj_list

    return df


# TrendStage 四分类（硬规则）
# early          → 趋势刚启动
# mid            → 主升浪可交易段
# late           → 趋势后段，风险上升
# distribution   → 派发 / 高位结构破坏
def classify_trend_stage(row) -> str:
    """
    基于 trend_strength 的趋势生命周期判定
    只依赖 pf / final_with_sim 中已存在的截面字段

    返回：
        'early' | 'mid' | 'late' | 'distribution' | 'unknown'
    """

    try:
        # ======================================================
        # 1. 趋势主状态（唯一权威来源）
        # ======================================================
        trend_strength = row.get("trend_strength")

        if trend_strength not in {
            "strong_uptrend",
            "uptrend",
            "trend_pullback",
            "range",
            "downtrend",
        }:
            return "unknown"

        # ======================================================
        # 2. 基础价格与指标（截面）
        # ======================================================
        close = row.get("close")
        ma20 = row.get("ma20")
        ma50 = row.get("ma50")
        ma200 = row.get("ma200")

        high_52w = row.get("high_52w")
        atr_15 = row.get("atr_15")

        rs_rank = row.get("rs_rank")
        obv_slope_20 = row.get("obv_slope_20")

        if close is None or close <= 0:
            return "unknown"

        # ======================================================
        # 3. 派生比例（只用截面可算的）
        # ======================================================
        dist_to_52w_high = (
            (high_52w - close) / high_52w
            if high_52w and high_52w > 0
            else None
        )

        ma20_dist = (
            (close - ma20) / ma20
            if ma20 and ma20 > 0
            else None
        )

        # 波动强度（仓位 / late 判断）
        # | atr/price | 市场含义          |
        # | --------- | ------------- |
        # | < 1.5%    | 稳定、适合加仓       |
        # | 1.5%–3%   | 正常趋势          |
        # | > 3%      | 情绪化 / 加速段     |
        # | > 5%      | Climax / 疯狂波动 |
        atr_price_ratio = (
            atr_15 / close
            if atr_15 and atr_15 > 0
            else None
        )

        # ======================================================
        # 4️⃣ distribution（结构性风险，优先级最高）
        # ======================================================
        distribution_conditions = 0

        if ma50 and close < ma50:
            distribution_conditions += 1

        if obv_slope_20 is not None and obv_slope_20 < 0:
            distribution_conditions += 1

        if rs_rank is not None and rs_rank < 50:
            distribution_conditions += 1

        if trend_strength in {"range", "downtrend"}:
            distribution_conditions += 1

        if distribution_conditions >= 2:
            return "distribution"
        
        # 在 classify_trend_stage 函数的开头加入特权判断
        price_tightness = float(row.get("price_tightness", 1.0))
        # 豁免逻辑：如果是紧致的高位旗形，不算 Late，算 Mid (最佳买点)
        if (trend_strength == "strong_uptrend" 
            and price_tightness < 0.06 
            and dist_to_52w_high < 0.05):
            return "mid"
        
        # 2. Climax / Extended (新增：超买预警)
        # 逻辑：股价偏离 MA50 超过 25%，通常意味着短期不可持续
        # === MA50 偏离度 (Extension)
        ma50_extension = ((close - ma50) / ma50) if ma50 else 0.0
        if ma50_extension > 0.25:
            return "extended(超买)"

        # ======================================================
        # 5️⃣ late stage（强趋势后段：位置 + 乖离 + 波动）
        # ======================================================
        # 原逻辑
        # and ma20_dist > 0.06
        # 修改后逻辑：
        # 对于超级强势股(RS>90)，允许更大的乖离率才算 Late
        # 大市值判断
        is_large_cap = False
        try:
            market_cap = row.get("market_cap") if "market_cap" in row else None
            is_large_cap = market_cap is not None and market_cap >= 100000000000
        except:
            pass

        # late stage 阈值动态化：大市值允许更大波动才算 late（不易过早判晚期）
        late_atr_threshold = 0.045 if is_large_cap else 0.03
        threshold = 0.12 if (rs_rank and rs_rank > 90) else 0.08
        if (
            trend_strength == "strong_uptrend"
            and dist_to_52w_high is not None
            and dist_to_52w_high < 0.03
            and ma20_dist is not None
            and ma20_dist > threshold  # <--- 使用动态阈值
            and atr_price_ratio is not None
            and atr_price_ratio > late_atr_threshold
        ):
            return "late"

        # ======================================================
        # 6️⃣ mid stage（主升浪：最优 Swing 区）
        # ======================================================
        if (
            trend_strength in {"strong_uptrend", "uptrend"}  # 允许 uptrend 进入
            and dist_to_52w_high is not None
            and 0.00 <= dist_to_52w_high <= 0.20             # 允许距离高点很近 (从 0.03 改为 0.00)
            and ma20_dist is not None
            and 0.00 <= ma20_dist <= 0.08                    # 放宽 ma20 偏离度要求
        ):
            return "mid"

        # ======================================================
        # 7️⃣ early stage（趋势初期）
        # ======================================================
        if (
            trend_strength in {"uptrend", "trend_pullback"}
            and dist_to_52w_high is not None
            and dist_to_52w_high > 0.20
            and ma50 and close > ma50
            and ma200 and close > ma200
        ):
            return "early"

        return "unknown"

    except Exception:
        return "unknown"


def fetch_current_vix():
    """
    获取当前 VIX 指数值"""
    try:
        vix_df = yf.download("^VIX", period="1d", progress=False, auto_adjust=True, proxy=PROXIES["http"])
        # 获取最新 VIX 收盘价，若失败则取默认值 18.0
        current_vix = vix_df['Close'].iloc[-1] if not vix_df.empty else 18.0
        if isinstance(current_vix, pd.Series): current_vix = current_vix.iloc[0]
        print(f"当前 VIX 指数: {current_vix:.2f} (调节系数: {max(1.0, 1+(current_vix-18)*0.05):.2f}x)")
    except Exception as e:
        print(f"VIX 获取失败，使用基准值: {e}")
        current_vix = 18.0
    return current_vix


# ===================== 字段中文映射 =====================
COLUMN_CN_MAP = {
    "is_current_hold": "是否持仓",
    "stock_code": "股票代码",
    "close": "最新收盘价",

    "ideal_entry": "理想买入区",
    "entry_price": "参考入场价",
    "hard_stop": "硬止损价",

    "target_profit": "目标止盈价",
    "rs_rank": "相对强度RS",
    "canslim_score": "CANSLIM评分",

    "quarterly_eps_growth": "季度EPS增长率",
    "annual_eps_growth": "年度EPS增长率",
    "revenue_growth": "营收增长率",
    "roe": "ROE",

    "shares_outstanding": "流通股本",
    "inst_ownership": "机构持仓",
    "fcf_quality": "自由现金流质量",
    "market_cap": "市值",
    "sector": "行业",

    "trade_state": "交易阶段",
    "trade_score": "综合评分",
    "obv_ad_interpretation": "量价解读",

    "trend_strength": "趋势强度",
    "trend_stage": "趋势阶段",
    "option_state_cn": "期权结构"
}


def autosize_excel_columns(file_path, sheet_name=None, min_width=10, max_width=40):
    wb = load_workbook(file_path)
    ws = wb[sheet_name] if sheet_name else wb.active

    for column_cells in ws.columns:
        max_length = 0
        column_letter = column_cells[0].column_letter

        for cell in column_cells:
            if cell.value is not None:
                max_length = max(max_length, len(str(cell.value)))

        adjusted_width = min(max(max_length + 2, min_width), max_width)
        ws.column_dimensions[column_letter].width = adjusted_width

    wb.save(file_path)


# ====== trade_state → 最大允许仓位映射 ======
TRADE_STATE_MAX_POSITION = {
    "主升浪建仓/加仓": 1.00,

    "阶段不明，小仓试探": 0.30,
    "阶段不明，仅观察": 0.20,

    "趋势未启动，仅观察": 0.20,
    "非趋势结构，仅观察": 0.15,
    "量价支持不足": 0.15,

    "资金结构恶化，仅观察": 0.10,

    "趋势停滞+资金分歧，禁止交易": 0.00,
    "资金明确派发，禁止交易": 0.00,
}


# ===================== 配置 =====================
# 填写你当前持仓或重点观察的股票
CURRENT_SELECTED_TICKERS = ["AVGO", "ORCL","GOOG", "TLSA", "MU", "NVO", "NVDA", "AMD", "PLTR", "CDE", "NFLX", "RTX"]
# ===============================================

# ===================== 主流程 =====================
def main():
    # 1️⃣ State 1: A, Finnhub ticker
    # 首次执行时解开注释执行，以后每天轮动不用再执行
    # ticker_df = fetch_us_tickers()
    # upsert_stock_tickers(ticker_df)

    # 2️⃣ State 1: B, yfinance 批量加载所有4600左右流动股的价格
    # 首次执行时解开注释执行，以后每天轮动不用再执行
    # fetch_all_prices()

    # reset_yf_availability()

    # 3️⃣ State 1: C, 每天只需更新最新的股票价格数据即可
    print(f"🚀 Stage 1: 更新最新的股票价格数据")
    # 新增：确保SPY和QQQ数据更新，用于Market Regime Filter
    update_recent_prices(CURRENT_SELECTED_TICKERS + ["SPY", "QQQ"])

    # 构建价格历史映射
    price_history_map = build_price_history_map(min_bars=60)

    # 更新基本面数据
    print(f"🚀 Stage 1: 更新最新的基本面数据")
    update_fundamentals(get_tickers_missing_recent_fundamentals(get_recent_trading_days_smart(10)) + CURRENT_SELECTED_TICKERS + ["SPY", "QQQ"], force_update=False)

    # 🔥 新增：先检查数据完整性
    check_data_integrity()

    # 🚀 修复点：自动获取库中最新的交易日期
    latest_date_in_db = get_latest_date_in_db()
    if not latest_date_in_db:
        print("❌ 数据库中没有价格数据，请先运行 fetch_all_prices()")
        return

    # 新增：建议3 - Market Regime Filter
    # SPY > MA200 AND QQQ > MA50，否则不交易
    print("🔍 检查市场 Regime...")
    regime = check_market_regime()
    market_regime = "多头" if regime.get("is_bull", False) else "非多头"
    print(f"市场形态判定: {market_regime}")

    # 🔥 新增：运行诊断
    # diagnose_stage2_filters(latest_date_in_db)

    # 更新量价趋势特征表
    update_volume_trend_features(latest_date_in_db)

    # 4️⃣ Stage 2: SwingTrend 技术筛选
    print(f"🚀 Stage 2: SwingTrend 技术筛选 (包含监控名单: {CURRENT_SELECTED_TICKERS})")
    use_strict_rule = False
    stage2 = pd.DataFrame()
    if use_strict_rule:
        stage2 = build_stage2_swingtrend(latest_date_in_db, monitor_list=CURRENT_SELECTED_TICKERS, market_regime=market_regime)
    else:
        stage2 = build_stage2_swingtrend_balanced(latest_date_in_db, monitor_list=CURRENT_SELECTED_TICKERS, market_regime=market_regime)
    print(f"Stage 2 股票数量: {len(stage2)}")

    # if stage2.empty:
    #     print("❌ 今日无符合技术面筛选的股票，程序结束。")
    #     return # 或者保存一个空结果

    final = stage2.copy()
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
        .query("market_cap >= 1_000_000_000 and quarterly_eps_growth.notna()")
        .sort_values(["canslim_score", "rs_rank", "is_current_hold"], ascending=False)
    )
    print(f"按市值【10亿美元】和季度每股收益增长【quarterly_eps_growth】过滤后股票总数: {len(final_filtered)}")

    # 5️⃣ 集成 VWAP 和盘前高点
    print("\n🔍 正在获取 VWAP 和盘前高点数据...")
    integrate_vwap_and_premarket(final_filtered, latest_date_in_db)

    # 6️⃣ 波动模拟 (VIX 调节)
    print("\n🔍 正在获取市场 VIX 数据以调节波动区间...")
    current_vix = fetch_current_vix()
    
    # 注入回撤模拟数据
    # 注入 VIX-aware Entry / Stop / Target
    print("🛠️ 注入 VIX-aware Entry / Stop / Target ...")
    final_with_sim = apply_entry_stop_target_vix(
        final_filtered,
        current_vix=current_vix
    )

    final_with_sim = final_with_sim.query("entry_price.notna()")
    print(f"按必须包含买入价【entry_price】过滤后股票总数: {len(final_with_sim)}")

    for col in ["obv_slope_20", "obv_slope_5", "ad_slope_20", "ad_slope_5", "vol_rs_vcp", "price_tightness"]:
        final_with_sim[col] = final_with_sim[col].fillna(0.0)

    # 量价趋势特征解读
    final_with_sim['obv_ad_interpretation'] = final_with_sim.apply(
        lambda row: classify_obv_ad_enhanced(
            row.get('obv_slope_20'),
            row.get('ad_slope_20'),
            row.get('obv_slope_5'),
            row.get('ad_slope_5'),
            row.get('vol_rs_vcp'),
            row.get('price_tightness'),
            row.get('vol_spike_ratio'),
            row.get('daily_change_pct'),
            market_regime=market_regime
        ),
        axis=1
    )

    # 计算趋势强度
    final_with_sim["trend_strength"] = final_with_sim.apply(
        lambda row: compute_trend_strength_from_row(row, price_history_map),
        axis=1
    )

    # 计算趋势生命周期阶段
    final_with_sim["trend_stage"] = final_with_sim.apply(
        lambda row: classify_trend_stage(row),
        axis=1
    )

    # 计算 ADX（趋势是否在“走”）
    final_with_sim["adx"] = final_with_sim.apply(
        lambda row: compute_latest_adx_from_row(row, price_history_map),
        axis=1
    )

    # 基于 ADX 计算趋势活动度
    final_with_sim["trend_activity"] = final_with_sim.apply(
        classify_trend_activity_from_row,
        axis=1
    )

    # =========================
    # V3：应用量价交易 Gate
    # =========================
    final_with_sim[["allow_trade", "trade_state"]] = final_with_sim.apply(
        lambda row: obv_ad_trade_gate(
            row["obv_ad_interpretation"],
            row["trend_strength"],
            row["trend_stage"],
            row["trend_activity"],
            row["atr5"],
            row["atr20"],
            row["market_cap"]
        ),
        axis=1,
        result_type="expand"
    )

    # 在允许交易的基础上，应用期权风险保险丝
    final_with_sim["allow_trade"] = (
        final_with_sim["allow_trade"]
        & final_with_sim.apply(options_risk_gate, axis=1)
    )

    # 添加期权情绪状态
    final_with_sim["option_state"] = final_with_sim.apply(
        resolve_option_sentiment_state,
        axis=1
    )
    # 把期权情绪状态映射为中文
    final_with_sim["option_state_cn"] = final_with_sim["option_state"].map(
        OPTION_STATE_CN_MAP
    )

    # =========================
    # V3：生成最终交易评分
    # =========================
    # 计算各行业平均 RS 排名
    sector_avg_rs = final_with_sim.groupby('sector')['rs_rank'].mean().to_dict()
    # 处理 None 或缺失的 sector（安全起见）
    sector_avg_rs[None] = 50.0        # 或更保守的值，如 40.0
    # 使用 partial 固定 sector_avg_rs 参数，生成一个只接受 row 的新函数
    final_with_sim['trade_score'] = final_with_sim.apply(
        lambda row: compute_trade_score(row, sector_avg_rs),
        axis=1
    )
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
        "ideal_entry", "entry_price", "hard_stop",
        "target_profit", "rs_rank","canslim_score",
        "quarterly_eps_growth", "annual_eps_growth",
        "revenue_growth", "roe", "inst_ownership", 
        "fcf_quality", "market_cap", 'sector', 
        'trade_state', 'trade_score', 'obv_ad_interpretation', 
        'trend_strength', 'trend_stage', 'option_state_cn'
    ]
    display_cols_cn = [COLUMN_CN_MAP[col] for col in display_cols]
    final_with_sim = final_with_sim[display_cols].copy()
    final_with_sim.columns = display_cols_cn
    print(final_with_sim[display_cols_cn].to_string(index=False))

    # 保存结果
    if not final_with_sim.empty:
        file_name_xlsx = ""
        if use_strict_rule:
            file_name_xlsx = f"swing_strategy_vix_sim_strict_{datetime.now():%Y%m%d}.xlsx"
        else:
            file_name_xlsx = f"swing_strategy_vix_sim_balanced_{datetime.now():%Y%m%d}.xlsx"
        try:
            final_with_sim[display_cols_cn].to_excel(file_name_xlsx, index=False, engine='openpyxl')
            autosize_excel_columns(file_name_xlsx)
            print(f"\n📊 详细策略报告已生成 Excel: {file_name_xlsx}")
        except Exception as e:
            print(f"❌ Excel 生成失败 (请检查是否安装 openpyxl): {e}")
            # 备选保存为 CSV
            file_name_csv = file_name_xlsx.replace(".xlsx", ".csv")
            final_with_sim[display_cols_cn].to_csv(file_name_csv, index=False, encoding="utf-8-sig")
            print(f"\n📊 详细策略报告已生成: {file_name_csv}")
    else:
        print("⚠️ 经过基本面严格筛选后，没有符合条件的股票。")

if __name__ == "__main__":
    # main()
    fetch_current_vix()