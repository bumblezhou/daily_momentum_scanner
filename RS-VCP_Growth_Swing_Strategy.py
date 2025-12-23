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
            AND t.mic IN ('XNYS','XNGS','XASE','ARCX','BATS','IEXG')
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
def update_recent_prices():
    print(f"🕒 当前上海时间: {datetime.now():%Y-%m-%d %H:%M}")
    
    # 1. 自动根据 NYSE 日历获取最近 10 个有效交易日
    # 这里面已经自动排除了周末、圣诞节、感恩节等
    trading_days = get_recent_trading_days_smart(10)
    print(f"📅 纽交所最近有效交易日：{trading_days}")
    
    target_date = trading_days[-1]
    print(f"🎯 目标同步日期: {target_date}")

    # 2. 检查数据库缺失
    raw_tickers = get_tickers_missing_recent_data(trading_days)

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

def build_stage2_swingtrend(target_date: date, monitor_list: list = []) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH)

    # 将列表转换为 SQL 字符串格式 ('AAPL', 'TSLA')
    monitor_str = ", ".join([f"'{t}'" for t in monitor_list]) if monitor_list else "''"

    sql = f"""
    /* ======================================================
       Stage 2 – SwingTrend 技术筛选
       所有参数均可根据注释位置自行调整
       ====================================================== */

    WITH base AS (
        SELECT
            stock_code,
            trade_date,
            close,
            high,
            low,
            volume,

            /* ===== 均线参数（可调） ===== */
            AVG(close) OVER w10  AS ma10,    -- 短线持仓用
            AVG(close) OVER w50  AS ma50,
            AVG(close) OVER w150 AS ma150,
            AVG(close) OVER w200 AS ma200,

            /* ===== 52 周高低点窗口（252 日） ===== */
            MAX(high) OVER w252 AS high_52w,  -- 修正：实战中多用 high
            MIN(low) OVER w252 AS low_52w,    -- 修正：实战中多用 low

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price
        WINDOW
            w10  AS (PARTITION BY stock_code ORDER BY trade_date ROWS 9 PRECEDING),
            w50  AS (PARTITION BY stock_code ORDER BY trade_date ROWS 49 PRECEDING),
            w150 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 149 PRECEDING),
            w200 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 199 PRECEDING),
            w252 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 251 PRECEDING),
            w_all AS (PARTITION BY stock_code)
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

    rs_ranked AS (
        SELECT
            stock_code,
            trade_date,

            /* RS 权重（可调） */
            0.4*r1y + 0.3*r6m + 0.2*r3m + 0.1*r1m AS rs_score,

            /* 全市场百分位排名 */
            PERCENT_RANK() OVER (
                PARTITION BY trade_date
                ORDER BY 0.4*r1y + 0.3*r6m + 0.2*r3m + 0.1*r1m
            ) * 100 AS rs_rank
        FROM returns
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

    atr_stats AS (
        SELECT
            stock_code,
            trade_date,
            AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 9 PRECEDING)  AS atr10,
            AVG(tr) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS 49 PRECEDING) AS atr50
        FROM atr_raw
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
                ROWS 49 PRECEDING
            ) AS vol50
        FROM stock_price
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        r.rs_rank,
        b.ma10, b.ma50, b.ma150, b.ma200,
        b.high_52w, b.low_52w,
        a.atr10, a.atr50,
        p.pivot_price,
        v.volume, v.vol50

    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN atr_stats a USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    JOIN volume_check v USING (stock_code, trade_date)

    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            (
                /* ===== Trend Template（可调阈值） ===== */
                b.close > b.ma150
                AND b.ma150 > b.ma200
                AND b.ma50  > b.ma150
                AND b.close >= 1.25 * b.low_52w   -- 距 52 周低点至少 +25%
                AND b.close >= 0.75 * b.high_52w  -- 距 52 周高点不超过 -25%

                /* ===== RS Rank 门槛 ===== */
                AND r.rs_rank >= 70

                /* ===== VCP：波动正在收缩 ===== */
                AND a.atr10 < a.atr50

                /* ===== Pivot 放量突破 ===== */
                AND b.close > p.pivot_price * 0.99
                AND v.volume >= 1.2 * v.vol50     -- 放量倍数（可调）
            )
            OR
            -- 新增：如果是自选股，强制通过筛选
            b.stock_code IN ({monitor_str})
        )

    """

    df = con.execute(sql).df()
    con.close()
    return df


def build_stage3_fundamental_fast(stage2_df: pd.DataFrame) -> pd.DataFrame:
    """
    从本地 DuckDB 直接获取基本面评分 (极速版)
    """
    if stage2_df.empty:
        return pd.DataFrame()
    
    con = duckdb.connect(DUCKDB_PATH)

    # 将 Stage 2 的结果注册为临时表，方便与基本面表 JOIN
    con.register("tmp_stage2", stage2_df)
    
    ticker_list = stage2_df["stock_code"].unique().tolist()
    update_fundamentals(con, ticker_list, force_update=True)

    sql = """
        SELECT 
            s2.*,
            f.eps_growth,
            f.roe,
            f.fundamental_score
        FROM tmp_stage2 s2
        LEFT JOIN stock_fundamentals f ON s2.stock_code = f.stock_code
        ORDER BY s2.rs_rank DESC, f.fundamental_score DESC
    """
    # 💡 核心修正：只选择基本面相关的列 + 关联主键
    sql = """
        SELECT 
            f.stock_code,
            f.fundamental_score,
            f.eps_growth AS eps_acceleration, -- 对应你 final 打印时的字段名
            f.roe,
            f.revenue_growth,
            f.fcf_quality
        FROM stock_fundamentals f
        WHERE f.stock_code IN (SELECT stock_code FROM tmp_stage2)
    """
    
    result_df = con.execute(sql).df()

    con.close()

    return result_df


# 创建基本面数据表结构
def init_fundamental_table(con):
    """初始化基本面数据表"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS stock_fundamentals (
            stock_code VARCHAR,
            update_date DATE,
            eps_growth DOUBLE,
            roe DOUBLE,
            revenue_growth DOUBLE,
            fcf_quality DOUBLE,
            inst_own DOUBLE,
            fundamental_score DOUBLE,
            PRIMARY KEY (stock_code)
        )
    """)


# 编写“增量更新”脚本
def update_fundamentals(con, ticker_list, force_update=False):
    """
    定期更新基本面数据
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
            WHERE update_date > CURRENT_DATE - INTERVAL '7 days'
        """).df()['stock_code'].tolist()
        need_update = [t for t in ticker_list if t not in existing]

    if not need_update:
        print("✅ 所有基本面数据均在有效期内，无需更新。")
        return

    print(f"🚀 开始更新 {len(need_update)} 只股票的基本面...")

    for symbol in need_update:
        try:
            t = yf.Ticker(symbol)
            info = t.info
            
            # 提取指标 (保留你之前的核心逻辑)
            eps_growth = info.get("earningsQuarterlyGrowth")
            roe = info.get("returnOnEquity")
            rev_growth = info.get("revenueGrowth")
            fcf = info.get("freeCashflow")
            ocf = info.get("operatingCashflow")
            fcf_quality = (fcf / ocf) if (fcf and ocf and ocf > 0) else None
            inst_own = info.get("heldPercentInstitutions")

            # 计算 Minervini 评分
            score = 0
            if eps_growth and eps_growth > 0.2: score += 1
            if roe and roe > 0.17: score += 1
            if rev_growth and rev_growth > 0.15: score += 1
            if fcf_quality and fcf_quality > 0.8: score += 1

            # 使用 UPSERT 逻辑 (DuckDB 0.9.x+ 支持)
            con.execute("""
                INSERT OR REPLACE INTO stock_fundamentals 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, datetime.now().date(), eps_growth, roe, 
                rev_growth, fcf_quality, inst_own, score
            ))
            
            print(f"  [OK] {symbol} (Score: {score})")
            time.sleep(0.5) # 频率控制

        except Exception as e:
            print(f"  [ERR] {symbol} 更新失败: {e}")
            continue


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


# 3-40 天周期的操作建议在 SwingTrend 系统中，这个周期的操作核心是**“止损上移”**：
# 时间点动作第 1 天突破 Pivot Point 买入，止损设在 -5%。
# 第 5-10 天如果利润达到 5-8%，将止损移至成本价（确保不亏）。
# 第 10-40 天观察 10日均线 (MA10)。只要收盘价不破 MA10，就一直持有。
# 卖出信号跌破关键均线或 RS Rank 掉出前 70 名。

# ===================== 配置 =====================
# 填写你当前持仓或重点观察的股票
CURRENT_SELECTED_TICKERS = ["CDE", "CSTM", "MLI"]
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
    update_recent_prices()

    # 🚀 修复点：自动获取库中最新的交易日期
    latest_date_in_db = get_latest_date_in_db()
    if not latest_date_in_db:
        print("❌ 数据库中没有价格数据，请先运行 fetch_all_prices()")
        return

    # 4️⃣ Stage 2: SwingTrend 技术筛选
    print(f"🚀 Stage 2: SwingTrend 技术筛选 (包含监控名单: {CURRENT_SELECTED_TICKERS})")
    stage2 = build_stage2_swingtrend(latest_date_in_db, monitor_list=CURRENT_SELECTED_TICKERS)
    print(f"Stage 2 股票数量: {len(stage2)}")

    if stage2.empty:
        print("❌ 今日无符合技术面筛选的股票，程序结束。")
        return # 或者保存一个空结果

    # 6️⃣ Stage 3: 基本面分析
    print("📊 Stage 3: 基本面分析")
    stage3 = build_stage3_fundamental_fast(stage2)
    # stage3 = build_stage3_fundamental(stage2)

    # 合并结果
    final = stage2.merge(stage3, on="stock_code", how="left")
    # 填充缺失的基本面分数为 0，防止 query 报错
    final["fundamental_score"] = final["fundamental_score"].fillna(0)

    # 5. 标记来源（可选：方便你在结果中区分哪些是买入的，哪些是新选出的）
    final["is_current_hold"] = final["stock_code"].apply(lambda x: "✅" if x in CURRENT_SELECTED_TICKERS else "❌")

    # 过滤与排序
    # 💡 注意：如果你放宽了条件，这里的 fundamental_score >= 3 可能又会把结果过滤成 0
    # 建议先打印看看
    print(f"合并后带评分的股票总数: {len(final)}")
    
    # 暂时降低过滤门槛以确保有输出
    final_filtered = (
        final
        .query("fundamental_score >= 0") # 先改成 0 跑通流程
        .sort_values(["fundamental_score", "rs_rank", "is_current_hold"], ascending=False)
        .head(20)
    )

    # 7️⃣ 获取实时 VIX 作为调节因子
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

    # 8️⃣ 注入回撤模拟数据
    print("🛠️ 正在计算个股波动容错区间...")
    pullback_list = []
    for ticker in final_filtered['stock_code']:
        p_data = simulate_pullback_range(ticker, current_vix=current_vix)
        pullback_list.append(p_data if p_data else {})
    
    # 合并模拟结果
    pullback_df = pd.DataFrame(pullback_list)
    final_with_sim = pd.concat([final_filtered.reset_index(drop=True), pullback_df], axis=1)

    # 9️⃣ 最终打印输出
    print("\n✅ 最终买入候选及波动模拟 (含 VIX 调节)")
    print("-" * 150)
    display_cols = [
        "is_current_hold", "stock_code", "close", 
        "ideal_entry", "hard_stop", "vix_adj", "rs_rank", "fundamental_score"
    ]
    print(final_with_sim[display_cols].to_string(index=False))

    # 保存结果
    file_name = f"swing_strategy_vix_sim_{datetime.now():%Y%m%d}.csv"
    final_with_sim.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"\n📊 详细策略报告已生成: {file_name}")

if __name__ == "__main__":
    main()
