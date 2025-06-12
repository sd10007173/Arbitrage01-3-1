import aiohttp
import asyncio
import sqlite3
from datetime import datetime, timezone, timedelta
import time
import ssl
import certifi
import pandas as pd

# --- 新增：處理 Python 3.12 的 sqlite3 日期時間 DeprecationWarning ---
# 1. 定義一個新的 adapter，將 python datetime 物件轉換為 ISO 8601 字串
def adapt_datetime_iso(val):
    """將 datetime.datetime 適配為時區感知的 ISO 8601 字串"""
    return val.isoformat()

# 2. 註冊這個 adapter 來處理所有的 datetime 物件
sqlite3.register_adapter(datetime, adapt_datetime_iso)
# --- 結束 ---

DB_PATH = "data/funding_rate.db"
SUPPORTED_EXCHANGES = ['binance', 'bybit', 'okx']
CHUNK_DAYS = 5 # 每次 API 抓取區間（天）
WAIT_TIME = 0.5 # 每次 API 呼叫間隔

def get_connection():
    """獲取資料庫連接"""
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

async def get_target_pairs(conn, exchanges, top_n):
    """
    從資料庫中根據市值排名和交易所支援情況，篩選出目標交易對。
    返回一個任務列表，每個任務包含 symbol, exchange 和 list_date。
    """
    cursor = conn.cursor()
    tasks = []
    
    # 構建查詢語句
    # 我們需要動態地檢查每個請求的交易所是否被支援
    placeholders = ','.join('?' for _ in exchanges)
    query = f"""
        SELECT id, symbol, trading_pair, market_cap_rank, 
               {', '.join([f'{ex}_support' for ex in exchanges])},
               {', '.join([f'{ex}_list_date' for ex in exchanges])}
        FROM trading_pair
        WHERE market_cap_rank IS NOT NULL AND market_cap_rank <= ?
        ORDER BY market_cap_rank
    """
    
    cursor.execute(query, (top_n,))
    rows = cursor.fetchall()
    
    for row in rows:
        for ex in exchanges:
            # 檢查該交易所是否支援此交易對
            if row[f'{ex}_support']:
                # 只處理支援的交易所
                if ex in SUPPORTED_EXCHANGES:
                    tasks.append({
                        "symbol": row['symbol'],
                        "trading_pair": row['trading_pair'], # e.g., BTCUSDT
                        "exchange": ex,
                        "list_date": row[f'{ex}_list_date']
                    })
    return tasks

async def save_funding_rates(conn, df, exchange, symbol):
    """將處理過的 DataFrame (含NULL) 的資金費率數據批量存入資料庫"""
    if df.empty:
        return 0

    to_insert = []
    for timestamp_utc, row in df.iterrows():
        # pandas.NaN 在這裡會被正確處理，sqlite3驅動會將其轉換為NULL
        funding_rate = row['funding_rate'] if pd.notna(row['funding_rate']) else None
        to_insert.append((
            timestamp_utc.to_pydatetime(),  # 從 pandas Timestamp 轉換為 python datetime
            symbol,
            exchange,
            funding_rate
        ))

    if not to_insert:
        return 0

    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO funding_rate_history (timestamp_utc, symbol, exchange, funding_rate)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timestamp_utc, symbol, exchange) DO UPDATE SET
            funding_rate=excluded.funding_rate, updated_at=CURRENT_TIMESTAMP
        """, to_insert)
        conn.commit()
        return cursor.rowcount
    except sqlite3.Error as e:
        print(f"❌ 資料庫儲存時出錯 ({symbol}_{exchange}): {e}")
        conn.rollback()
        return 0

async def fetch_funding_rates_rest(session, exchange, symbol, trading_pair, start_dt, end_dt):
    """使用 aiohttp 直接請求 REST API"""
    all_data = []
    current_dt = start_dt
    
    while current_dt < end_dt:
        fetch_end = min(current_dt + timedelta(days=CHUNK_DAYS), end_dt)
        params = {}
        url = ""

        if exchange == 'binance':
            url = "https://fapi.binance.com/fapi/v1/fundingRate"
            params.update({
                "symbol": trading_pair,
                "startTime": int(current_dt.timestamp() * 1000),
                "endTime": int(fetch_end.timestamp() * 1000),
                "limit": 1000
            })
        elif exchange == 'bybit':
            url = "https://api.bybit.com/v5/market/funding/history"
            params.update({
                "symbol": trading_pair,
                "category": "linear",
                "startTime": int(current_dt.timestamp() * 1000),
                "endTime": int(fetch_end.timestamp() * 1000),
                "limit": 200
            })
        elif exchange == 'okx':
            url = "https://www.okx.com/api/v5/public/funding-rate-history"
            params = {
                "instId": f"{symbol}-USDT-SWAP",
                "after": int(fetch_end.timestamp() * 1000),
                "limit": 100
            }
        
        try:
            async with session.get(url, params=params, timeout=20) as response:
                response.raise_for_status()
                data = await response.json()

                if exchange == 'binance':
                    all_data.extend(data)
                elif exchange == 'bybit':
                    if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                        all_data.extend(data["result"]["list"])
                elif exchange == 'okx':
                    if data.get("code") == "0":
                         all_data.extend(data.get("data", []))

        except aiohttp.ClientError as e:
            print(f"❌ ({exchange.upper()}) {symbol} {current_dt.strftime('%Y-%m-%d')} 請求錯誤: {e}")
        except asyncio.TimeoutError:
            print(f"❌ ({exchange.upper()}) {symbol} {current_dt.strftime('%Y-%m-%d')} 請求超時")

        await asyncio.sleep(WAIT_TIME)
        current_dt = fetch_end
        if exchange == 'okx': # OKX 是反向遍歷，拿到一次就夠了
            break
            
    return all_data

async def fetch_and_save_fr(session, task, start_date, end_date):
    symbol = task['symbol']
    exchange_id = task['exchange']
    trading_pair = task['trading_pair']

    actual_start_date = start_date
    if task['list_date']:
        list_date_dt = datetime.fromisoformat(task['list_date']).replace(tzinfo=timezone.utc)
        actual_start_date = max(start_date, list_date_dt)

    if actual_start_date >= end_date:
        print(f"🟡 ({exchange_id.upper()}) {symbol}: 上市日期晚於或等於請求區間，跳過。")
        return

    print(f"🚀 ({exchange_id.upper()}) 開始獲取 {symbol} 從 {actual_start_date.strftime('%Y-%m-%d')} 的數據...")

    api_rates = await fetch_funding_rates_rest(session, exchange_id, symbol, trading_pair, actual_start_date, end_date)

    # --- 新增邏輯：生成完整時間軸並合併 ---

    # 1. 創建完整的小時時間軸
    # pd.date_range 的結尾是包含的，但我們的 end_date 是開區間，所以減去一小時
    hourly_index = pd.date_range(start=actual_start_date, end=end_date - timedelta(hours=1), freq='h', tz='UTC')
    
    # 2. 將API返回的數據轉換為帶有時間索引的DataFrame
    api_df = None
    if api_rates:
        processed_rates = []
        for r in api_rates:
            try:
                rate_record = {}
                if exchange_id == 'binance':
                    rate_record['timestamp_utc'] = datetime.fromtimestamp(int(r['fundingTime']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                elif exchange_id == 'bybit':
                    rate_record['timestamp_utc'] = datetime.fromtimestamp(int(r['fundingRateTimestamp']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                elif exchange_id == 'okx':
                    rate_record['timestamp_utc'] = datetime.fromtimestamp(int(r['fundingTime']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                processed_rates.append(rate_record)
            except (KeyError, ValueError) as e:
                print(f"⚠️ ({exchange_id.upper()}) {symbol}: 解析API數據時跳過一筆記錄 - {e}")
        
        if processed_rates:
            api_df = pd.DataFrame(processed_rates).set_index('timestamp_utc')

    # 3. 以完整時間軸為基礎，合併API數據
    final_df = pd.DataFrame(index=hourly_index)
    final_df.index.name = 'timestamp_utc'
    if api_df is not None:
        final_df = final_df.join(api_df)

    # --- 邏輯結束 ---

    if not final_df.empty:
        conn = get_connection()
        inserted_count = await save_funding_rates(conn, final_df, exchange_id, symbol)
        conn.close()
        print(f"✅ ({exchange_id.upper()}) {symbol}: 成功處理 {inserted_count} 筆數據 (含NULL)。")
    else:
        print(f"ℹ️ ({exchange_id.upper()}) {symbol}: 在指定區間內未找到數據。")

async def main():
    """主執行程序"""
    # 獲取用戶輸入
    print("--- 資金費率歷史數據獲取工具 V2 ---")
    
    # 獲取交易所
    exchanges_input = input("請輸入要查詢的交易所, 用空格分隔 (例如: binance bybit okx): ").strip().lower()
    exchanges = [ex.strip() for ex in exchanges_input.split() if ex.strip()]
    while not exchanges:
        print("未輸入任何交易所，請重新輸入。")
        exchanges_input = input("請輸入要查詢的交易所, 用空格分隔 (例如: binance bybit okx): ").strip().lower()
        exchanges = [ex.strip() for ex in exchanges_input.split() if ex.strip()]

    # 獲取市值排名
    top_n = 0
    while top_n <= 0:
        try:
            top_n = int(input("請輸入要查詢的市值排名前 N (例如: 500): ").strip())
            if top_n <= 0:
                print("請輸入一個大於 0 的整數。")
        except ValueError:
            print("無效的輸入，請輸入一個數字。")

    # 獲取開始日期
    start_date_str = ""
    while not start_date_str:
        try:
            start_date_str = input("請輸入開始日期 (格式 YYYY-MM-DD): ").strip()
            datetime.fromisoformat(start_date_str)
        except ValueError:
            print("日期格式錯誤，請使用 YYYY-MM-DD 格式。")
            start_date_str = ""
    
    # 獲取結束日期
    end_date_str = ""
    while not end_date_str:
        try:
            end_date_str = input("請輸入結束日期 (格式 YYYY-MM-DD): ").strip()
            datetime.fromisoformat(end_date_str)
        except ValueError:
            print("日期格式錯誤，請使用 YYYY-MM-DD 格式。")
            end_date_str = ""
            
    print("-------------------------------------\n")
    
    # 解析並設定時區
    start_date = datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)
    end_date = datetime.fromisoformat(end_date_str).replace(tzinfo=timezone.utc)
    
    # 建立資料庫連線
    conn = get_connection()
    
    # 1. 獲取目標任務列表
    print(f"正在從資料庫查詢 市值前 {top_n} 且支援 {', '.join(exchanges)} 的交易對...")
    tasks = await get_target_pairs(conn, exchanges, top_n)
    conn.close()
    
    if not tasks:
        print(f"未找到任何符合條件的任務，程式終止。")
        return
        
    print(f"找到 {len(tasks)} 個任務，準備開始獲取數據...")

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        fetch_tasks = [fetch_and_save_fr(session, task, start_date, end_date) for task in tasks]
        await asyncio.gather(*fetch_tasks)

    print("\n🎉 所有任務執行完畢！")

if __name__ == '__main__':
    # 移除 argparse，直接運行 main
    asyncio.run(main())