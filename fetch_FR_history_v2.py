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

async def get_target_pairs(conn, exchanges, symbol):
    """
    從資料庫中根據使用者指定的 symbol 和 exchanges，
    返回一個任務列表，每個任務包含 symbol, trading_pair, exchange 和 list_date。
    """
    cursor = conn.cursor()
    tasks = []
    
    # 我們需要 symbol 來找到 trading_pair (例如 BTC -> BTCUSDT)
    # 並檢查指定交易所的支援情況和上市日期
    # 注意：這裡假設 symbol 到 trading_pair 的映射是唯一的，且我們取第一個找到的
    query = f"""
        SELECT id, symbol, trading_pair,
               {', '.join([f'{ex}_support' for ex in exchanges])},
               {', '.join([f'{ex}_list_date' for ex in exchanges])}
        FROM trading_pair
        WHERE symbol = ?
        LIMIT 1 
    """
    
    cursor.execute(query, (symbol,))
    row = cursor.fetchone()
    
    if not row:
        print(f"❌ 在 trading_pair 資料表中找不到 symbol: {symbol}")
        return []

    for ex in exchanges:
        # 檢查該交易所是否支援此交易對
        if ex in SUPPORTED_EXCHANGES and row[f'{ex}_support']:
            # 轉換 symbol 為 API 需要的格式，例如 BTC -> BTCUSDT
            # OKX 是一個特例，它直接使用 BTC-USDT-SWAP 這種格式，而我們在 API 呼叫時會處理
            # Binance 和 Bybit 則需要 trading_pair
            api_symbol = row['trading_pair'] if ex != 'okx' else row['symbol']
            
            tasks.append({
                "symbol": row['symbol'],
                "trading_pair": row['trading_pair'], # e.g., BTCUSDT for Binance/Bybit
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
    """使用 aiohttp 直接請求 REST API，並加入重試機制"""
    all_data = []
    current_dt = start_dt
    
    while current_dt < end_dt:
        fetch_end = min(current_dt + timedelta(days=CHUNK_DAYS), end_dt)
        params = {"symbol": trading_pair}
        url = ""

        if exchange == 'binance':
            url = "https://fapi.binance.com/fapi/v1/fundingRate"
            params.update({
                "startTime": int(current_dt.timestamp() * 1000),
                "endTime": int(fetch_end.timestamp() * 1000),
                "limit": 1000
            })
        elif exchange == 'bybit':
            url = "https://api.bybit.com/v5/market/funding/history"
            params.update({
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
        
        # --- 新增：重試邏輯 ---
        retries = 3
        for attempt in range(retries):
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
                
                break # 成功，跳出重試循環

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < retries - 1:
                    print(f"🟡 ({exchange.upper()}) {symbol} 請求失敗 (第 {attempt + 1}/{retries} 次): {e}. 在 2 秒後重試...")
                    await asyncio.sleep(2)
                else:
                    print(f"❌ ({exchange.upper()}) {symbol} {current_dt.strftime('%Y-%m-%d')} 請求錯誤: {e}")
        # --- 重試邏輯結束 ---

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

    # 增量更新檢查：查詢資料庫中最新的時間戳
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(timestamp_utc) FROM funding_rate_history WHERE symbol = ? AND exchange = ?",
        (symbol, exchange_id)
    )
    latest_db_timestamp_str = cursor.fetchone()[0]
    conn.close()

    if latest_db_timestamp_str:
        latest_db_date = datetime.fromisoformat(latest_db_timestamp_str).replace(tzinfo=timezone.utc)
        # 我們從資料庫最新時間的下一個小時開始抓取
        incremental_start_date = latest_db_date + timedelta(hours=1)
        # 取兩者中較晚的日期作為真正的開始日期
        actual_start_date = max(actual_start_date, incremental_start_date)


    if actual_start_date >= end_date:
        print(f"✅ ({exchange_id.upper()}) {symbol}: 數據已是最新，無需更新。")
        return

    print(f"🚀 ({exchange_id.upper()}) 開始獲取 {symbol} 從 {actual_start_date.strftime('%Y-%m-%d %H:%M:%S')} 的數據...")

    api_rates = await fetch_funding_rates_rest(session, exchange_id, symbol, trading_pair, actual_start_date, end_date)

    # --- 新增邏輯：生成完整時間軸並合併 ---

    # 1. 創建完整的小時時間軸
    # pd.date_range 的結尾是包含的，但我們的 end_date 是開區間，所以減去一小時
    hourly_index = pd.date_range(start=actual_start_date, end=end_date - timedelta(hours=1), freq='h', tz='UTC')
    
    # 2. 將API返回的數據轉換為帶有時間索引的DataFrame，並對齊到整點小時
    api_df = None
    if api_rates:
        processed_rates = []
        for r in api_rates:
            try:
                rate_record = {}
                ts = None
                if exchange_id == 'binance':
                    ts = datetime.fromtimestamp(int(r['fundingTime']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                elif exchange_id == 'bybit':
                    ts = datetime.fromtimestamp(int(r['fundingRateTimestamp']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                elif exchange_id == 'okx':
                    ts = datetime.fromtimestamp(int(r['fundingTime']) / 1000, tz=timezone.utc)
                    rate_record['funding_rate'] = float(r['fundingRate'])
                
                if ts:
                    # 核心修正：將時間戳向下對齊到最近的整點小時
                    rate_record['timestamp_utc'] = ts.replace(minute=0, second=0, microsecond=0)
                    processed_rates.append(rate_record)

            except (KeyError, ValueError) as e:
                print(f"⚠️ ({exchange_id.upper()}) {symbol}: 解析API數據時跳過一筆記錄 - {e}")
        
        if processed_rates:
            # 將 list of dicts 轉為 DataFrame
            temp_df = pd.DataFrame(processed_rates)
            # 處理同一小時內可能有多筆數據的情況，我們只保留最後一筆，確保數據的唯一性
            api_df = temp_df.groupby('timestamp_utc').last()

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
    print("--- 資金費率歷史數據獲取工具 V2 (指定交易對版本) ---")
    
    # 獲取交易所，並加入驗證
    exchanges = []
    while not exchanges:
        exchanges_input = input("請輸入要查詢的交易所, 用空格分隔 (例如: binance bybit okx): ").strip().lower()
        input_list = [ex.strip() for ex in exchanges_input.split() if ex.strip()]
        
        if not input_list:
            print("未輸入任何交易所，請重新輸入。")
            continue

        # 驗證所有輸入的交易所是否都有效
        invalid_exchanges = [ex for ex in input_list if ex not in SUPPORTED_EXCHANGES]
        
        if invalid_exchanges:
            print(f"❌ 錯誤：包含不支援或拼寫錯誤的交易所: {', '.join(invalid_exchanges)}")
            print(f"   目前支援的交易所為: {', '.join(SUPPORTED_EXCHANGES)}")
        else:
            exchanges = input_list # 全部有效，賦值並結束循環

    # 獲取交易對 Symbol
    symbol_input = ""
    while not symbol_input:
        symbol_input = input("請輸入要查詢的交易對 (API格式, 例如: BTCUSDT): ").strip().upper()
        if not symbol_input:
            print("未輸入任何交易對，請重新輸入。")

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
    # 修正：將結束日期視為包含當天。例如，輸入 2025-06-11，則抓取到 2025-06-11 23:00:00 的數據
    # 我們透過將日期加一天，並將其作為開區間的結束點來實現
    end_date = datetime.fromisoformat(end_date_str).replace(tzinfo=timezone.utc) + timedelta(days=1)
    
    # 1. 建立任務列表 (不查資料庫)
    print(f"準備為交易對 {symbol_input} 建立在 {', '.join(exchanges)} 上的抓取任務...")
    
    # 根據輸入決定用於API的交易對和用於資料庫的symbol
    if symbol_input.endswith("USDT"):
        db_symbol = symbol_input[:-4]
        api_trading_pair = symbol_input
    else:
        db_symbol = symbol_input
        api_trading_pair = f"{symbol_input}USDT"
        
    tasks = []
    for ex in exchanges:
        tasks.append({
            "symbol": db_symbol,
            "trading_pair": api_trading_pair,
            "exchange": ex,
            "list_date": None  # 不再需要 list_date
        })
    
    if not tasks:
        print("未建立任何任務，程式終止。")
        return
        
    print(f"找到 {len(tasks)} 個任務，準備開始獲取數據...")

    # --- 新增：併發控制器 ---
    # 設置一個Semaphore來限制同時運行的任務數量，例如10個
    CONCURRENCY_LIMIT = 10
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def run_with_semaphore(task_coro):
        async with semaphore:
            return await task_coro
    # --- 結束 ---

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        fetch_tasks = [run_with_semaphore(fetch_and_save_fr(session, task, start_date, end_date)) for task in tasks]
        await asyncio.gather(*fetch_tasks)

    print("\n🎉 所有任務執行完畢！")

if __name__ == '__main__':
    # 移除 argparse，直接運行 main
    asyncio.run(main())