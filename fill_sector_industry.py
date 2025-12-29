import duckdb
import yfinance as yf
import time
from datetime import datetime

DUCKDB_PATH = "stock_data.duckdb"

# 如果你需要代理
PROXIES = {
    "http": "http://127.0.0.1:8118",
    "https": "http://127.0.0.1:8118",
}

yf.set_config(proxy=PROXIES["http"])

def finnhub_to_yahoo(symbol: str) -> str:
    return symbol.replace(".", "-")


def get_tickers_need_fill(con):
    """
    只处理：
    - 普通股
    - sector 为空的
    - 主流交易所
    """
    sql = """
        SELECT symbol
        FROM stock_ticker
        WHERE
            type = 'Common Stock'
            AND sector IS NULL
            AND mic IN ('XNYS','XNAS','XNGS','XASE','ARCX','BATS','IEXG')
    """
    return [r[0] for r in con.execute(sql).fetchall()]


def fill_sector_industry(batch_size=20, sleep_sec=1.2):
    con = duckdb.connect(DUCKDB_PATH)

    symbols = get_tickers_need_fill(con)
    total = len(symbols)

    print(f"📦 需要补齐 sector / industry 的股票数量: {total}")

    for i, symbol in enumerate(symbols, 1):
        yahoo_symbol = finnhub_to_yahoo(symbol)

        try:
            t = yf.Ticker(yahoo_symbol)
            info = t.info

            sector = info.get("sector")
            industry = info.get("industry")

            # ETF / 特殊票通常没有 sector，直接跳过
            if not sector:
                print(f"[{i}/{total}] ⚠️ {symbol} 无 sector，跳过")
                continue

            con.execute(
                """
                UPDATE stock_ticker
                SET
                    sector = ?,
                    industry = ?,
                    updated_at = ?
                WHERE symbol = ?
                """,
                (sector, industry, datetime.now(), symbol)
            )

            print(f"[{i}/{total}] ✅ {symbol} → {sector} / {industry}")

        except Exception as e:
            print(f"[{i}/{total}] ❌ {symbol} 失败: {e}")

        # 防止 Yahoo 限流
        time.sleep(sleep_sec)

    con.close()
    print("🎉 sector / industry 补齐完成")


if __name__ == "__main__":
    fill_sector_industry()
