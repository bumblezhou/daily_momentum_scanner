# rs_rating_test_proxy.py
# Finviz + HTTP 代理 (127.0.0.1:8118) 稳定抓取 RS Rating
# 已测试：代理正常时 Status 200

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

# ==================== 代理配置 ====================
PROXY = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118'  # HTTP 代理转发 HTTPS
}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
}

# 创建带代理的 Session
SESSION = requests.Session()
SESSION.proxies.update(PROXY)
SESSION.headers.update(HEADERS)
# 忽略 TLS 警告（代理常见）
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# ===============================================

def test_proxy_connection():
    """测试代理是否可用"""
    try:
        response = SESSION.get('https://finviz.com/quote.ashx?t=TSM', timeout=10, verify=False)
        print(f"代理测试: Status {response.status_code} {'✅ 成功' if response.status_code == 200 else '❌ 失败'}")
        return response.status_code == 200
    except Exception as e:
        print(f"代理测试失败: {e}")
        return False

def get_single_stock_rs(ticker: str):
    """带代理抓取单只股票 RS Rating"""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    try:
        print(f"正在访问 (代理): {url}")
        response = SESSION.get(url, timeout=15, verify=False)
        if response.status_code != 200:
            print(f"HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table', class_='snapshot-table2')
        if not table:
            print("未找到数据表格")
            return None

        data = {}
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                data[key] = value

        rs_rating = data.get('Relative Strength')
        try:
            rs_rating = int(rs_rating) if rs_rating and rs_rating != '-' else None
        except:
            rs_rating = None

        return {
            'Ticker': ticker,
            'Price': data.get('Price'),
            'RS_Rating': rs_rating,
            'RSI_14': data.get('RSI (14)'),
            'Volume': data.get('Volume'),
            'Rel_Volume': data.get('Rel Volume')
        }

    except Exception as e:
        print(f"抓取失败: {e}")
        return None

def screen_high_rs_stocks(min_rs: int = 80, limit: int = 10):
    """带代理筛选高 RS 股票"""
    try:
        print(f"筛选 RS >= {min_rs} (代理)...")
        base = "https://finviz.com/screener.ashx"
        params = {
            'v': '111',
            'f': f'sh_relstr_{min_rs}p',
            'o': '-relstr',
            'r': '1'
        }
        url = base
        all_data = []

        for page in range(1, 100):
            params['r'] = str((page - 1) * 20 + 1)
            response = SESSION.get(url, params=params, timeout=15, verify=False)
            if response.status_code != 200:
                break

            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', class_='screener-body-table')
            if not table:
                break

            rows = table.find_all('tr')[1:]
            if not rows:
                break

            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 11:
                    continue
                ticker = cols[1].get_text(strip=True)
                company = cols[2].get_text(strip=True)
                sector = cols[3].get_text(strip=True)
                price = cols[8].get_text(strip=True)
                change = cols[9].get_text(strip=True)
                volume = cols[10].get_text(strip=True)

                rs_match = re.search(r'Relative Strength\s+(\d+)', row.get_text())
                rs_rating = int(rs_match.group(1)) if rs_match else None

                all_data.append({
                    'Ticker': ticker,
                    'Company': company,
                    'Sector': sector,
                    'Price': price,
                    'Change': change,
                    'Volume': volume,
                    'RS_Rating': rs_rating
                })

                if len(all_data) >= limit:
                    break
            if len(all_data) >= limit:
                break
            time.sleep(1)  # 代理下延时更重要

        if not all_data:
            print("未找到数据")
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        df = df.sort_values('RS_Rating', ascending=False).head(limit)
        return df

    except Exception as e:
        print(f"筛选失败: {e}")
        return pd.DataFrame()

def main():
    print("=== Finviz + 代理 (127.0.0.1:8118) 测试 ===")
    
    # 测试代理
    if not test_proxy_connection():
        print("⚠️  代理不可用！请检查 127.0.0.1:8118 是否运行。")
        return

    ticker = "TSM"

    # 单股查询
    print("\n" + "="*60)
    print("1. 获取 TSM 的 RS Rating")
    print("="*60)
    result = get_single_stock_rs(ticker)

    if result and result['RS_Rating'] is not None:
        rs = result['RS_Rating']
        print(f"{ticker} RS Rating = {rs}/99")
        print("强势股（RS ≥ 80）" if rs >= 80 else "中强" if rs >= 70 else "偏弱")

        df_single = pd.DataFrame([result])
        print("\nFinviz 快照：")
        print(df_single.to_string(index=False))
    else:
        print("TSM 获取失败")

    # 高 RS 榜单
    print("\n" + "="*60)
    print("2. 筛选 RS ≥ 80 的强势股")
    print("="*60)
    df_high_rs = screen_high_rs_stocks(min_rs=80, limit=10)

    if not df_high_rs.empty:
        print(df_high_rs.to_string(index=False))
        filename = f"finviz_high_rs_proxy_{datetime.now():%Y%m%d}.xlsx"
        df_high_rs.to_excel(filename, index=False)
        print(f"\n已导出：{filename}")
    else:
        print("无数据")

    if not df_high_rs.empty and ticker in df_high_rs['Ticker'].values:
        rank = df_high_rs[df_high_rs['Ticker'] == ticker].index[0] + 1
        print(f"\n{ticker} 排名第 {rank} 位（RS = {rs}）")

if __name__ == "__main__":
    main()