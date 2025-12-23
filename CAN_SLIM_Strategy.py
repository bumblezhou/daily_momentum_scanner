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
            period="1y",
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
            AND t.mic IN ('XNYS','XNGS','XASE','XNAS','ARCX','BATS','IEXG')
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
# CAN SLIM 实现：扩展基本面和筛选逻辑
# ============================================================

# 创建基本面数据表结构（扩展为 CAN SLIM 字段）
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
            WHERE update_date > CURRENT_DATE - INTERVAL '7 days'
        """).df()['stock_code'].tolist()
        need_update = [t for t in ticker_list if t not in existing]

    if not need_update:
        print("✅ 所有基本面数据均在有效期内，无需更新。")
        return

    print(f"🚀 开始更新 {len(need_update)} 只股票的基本面...")

    for symbol in need_update:
        try:
            t = yf.Ticker(finnhub_to_yahoo(symbol))
            info = t.info

            # --- 金律字段提取 ---
            market_cap = info.get('marketCap', 0) or 0
            
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, datetime.now().date(), quarterly_eps_growth, annual_eps_growth, 
                rev_growth, roe, shares_outstanding, inst_own, fcf_quality, score, market_cap
            ))

            print(f"  [OK] {symbol} (CAN SLIM Score: {score})")
            time.sleep(0.5)  # 频率控制

        except Exception as e:
            print(f"  [ERR] {symbol} 更新失败: {e}")
            continue


def build_canslim_screen(target_date: date, monitor_list: list = []) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH)

    # 将列表转换为 SQL 字符串格式 ('AAPL', 'TSLA')
    monitor_str = ", ".join([f"'{t}'" for t in monitor_list]) if monitor_list else "''"

    sql = f"""
    /* ======================================================
       CAN SLIM 筛选
       先基本面 (C,A,S,I)，再技术 (N,L,M)
       ====================================================== */

    WITH base AS (
        SELECT
            stock_code,
            trade_date,
            close,
            high,
            low,
            volume,

            /* 均线参数 */
            AVG(close) OVER w50  AS ma50,
            AVG(close) OVER w200 AS ma200,

            /* 52 周高低点 */
            MAX(high) OVER w252 AS high_52w,
            MIN(low) OVER w252 AS low_52w,

            COUNT(*) OVER w_all AS trading_days
        FROM stock_price
        WINDOW
            w50  AS (PARTITION BY stock_code ORDER BY trade_date ROWS 49 PRECEDING),
            w200 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 199 PRECEDING),
            w252 AS (PARTITION BY stock_code ORDER BY trade_date ROWS 251 PRECEDING),
            w_all AS (PARTITION BY stock_code)
    ),

    /* RS Rank 计算 */
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
            ) - 1 AS r1y
        FROM base
    ),

    rs_ranked AS (
        SELECT
            stock_code,
            trade_date,
            PERCENT_RANK() OVER (
                PARTITION BY trade_date
                ORDER BY r1y
            ) * 100 AS rs_rank  -- L: RS Rank
    FROM returns
    ),

    /* Pivot 和成交量 */
    pivot_data AS (
        SELECT
            stock_code,
            trade_date,
            MAX(high) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS BETWEEN 40 PRECEDING AND 1 PRECEDING
            ) AS pivot_price  -- N: 新高突破
        FROM stock_price
    ),

    volume_check AS (
        SELECT
            stock_code,
            trade_date,
            volume,
            AVG(volume) OVER (
                PARTITION BY stock_code
                ORDER BY trade_date
                ROWS 49 PRECEDING
            ) AS vol50  -- S: 高成交量
        FROM stock_price
    )

    SELECT
        b.stock_code,
        b.trade_date,
        b.close,
        r.rs_rank,
        b.ma50, b.ma200,
        b.high_52w,
        p.pivot_price,
        v.volume, v.vol50,
        f.quarterly_eps_growth,
        f.annual_eps_growth,
        f.shares_outstanding,
        f.inst_ownership,
        f.canslim_score

    FROM base b
    JOIN rs_ranked r USING (stock_code, trade_date)
    JOIN pivot_data p USING (stock_code, trade_date)
    JOIN volume_check v USING (stock_code, trade_date)
    JOIN stock_fundamentals f USING (stock_code)

    WHERE
        b.trade_date = DATE '{target_date}'
        AND (
            (
                /* 基本面: C, A, S, I (阈值可调) */
                /* f.quarterly_eps_growth > 0.25
                AND f.annual_eps_growth > 0.25
                AND f.shares_outstanding < 100000000  -- 低股本
                AND f.inst_ownership > 0.5 */

                /* 技术面: N (新高突破), L (RS >80), M (市场上行: close > ma200) */
                b.close > b.high_52w * 0.95         -- 接近新高
                AND b.close > p.pivot_price * 0.98  -- 突破近期枢轴点
                AND v.volume > 1.5 * v.vol50        -- S: 高成交量
                AND r.rs_rank > 65                  -- L: RS Rank >65
                AND b.close > b.ma200               -- M: 价格 >200日MA
            )
            AND
            b.stock_code IN ({monitor_str})
        )

    ORDER BY f.canslim_score DESC, r.rs_rank DESC
    """

    df = con.execute(sql).df()
    con.close()
    return df


def get_latest_date_in_db():
    con = duckdb.connect(DUCKDB_PATH)
    latest_date_in_db = con.execute("SELECT MAX(trade_date) FROM stock_price").fetchone()[0]
    con.close()
    return latest_date_in_db


# ===================== 配置 =====================
# 填写你当前持仓或重点观察的股票
CURRENT_SELECTED_TICKERS = []
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

    # # 先用宽松技术条件找出潜在候选（避免基本面更新太多股票）
    # con = duckdb.connect(DUCKDB_PATH)
    # potential_tickers = con.execute(f"""
    #     SELECT DISTINCT stock_code 
    #     FROM stock_price 
    #     WHERE trade_date = DATE '{latest_date_in_db}'
    #       AND close > 10  -- 可加简单过滤，避免仙股
    # """).df()['stock_code'].tolist()
    # con.close()

    # print(f"潜在候选股票 {len(potential_tickers)} 只，准备增量更新基本面")

    # # 增量更新：只更新这部分，且只更新过期的
    # con = duckdb.connect(DUCKDB_PATH)
    # update_fundamentals(con, potential_tickers, force_update=False)  # 关键：非强制
    # con.close()
    # print(f"✅ 基本面数据更新完成")

    # 先用宽松技术条件找出潜在候选（避免基本面更新太多股票）
    con = duckdb.connect(DUCKDB_PATH)
    candidate_tickers = con.execute(f"""
        SELECT DISTINCT stock_code FROM stock_fundamentals 
        WHERE canslim_score >= 5 AND fcf_quality IS NOT NULL AND roe IS NOT NULL
        ORDER BY canslim_score DESC, fcf_quality DESC, quarterly_eps_growth DESC, annual_eps_growth DESC, revenue_growth DESC;
    """).df()['stock_code'].tolist()
    con.close()

    monitor_and_candidates = list(set(candidate_tickers) | set(CURRENT_SELECTED_TICKERS))
    # 4️⃣ CAN SLIM 筛选
    print(f"🚀 CAN SLIM 筛选 (包含监控名单: {CURRENT_SELECTED_TICKERS})")
    canslim_df = build_canslim_screen(latest_date_in_db, monitor_list=monitor_and_candidates)
    print(f"CAN SLIM 股票数量: {len(canslim_df)}")

    if canslim_df.empty:
        print("❌ 今日无符合 CAN SLIM 筛选的股票，程序结束。")
        return

    # 保存结果
    file_name = f"canslim_strategy_{datetime.now():%Y%m%d}.csv"
    canslim_df.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"\n📊 详细策略报告已生成: {file_name}")

if __name__ == "__main__":
    main()