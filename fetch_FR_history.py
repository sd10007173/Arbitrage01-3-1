#!/usr/bin/env python
import datetime
import requests
import time
import argparse
import pandas as pd

# 添加數據庫支持
from database_operations import DatabaseManager

# ---------------------------
# 預設參數 (當命令列參數未提供時會使用)
# ---------------------------
DEFAULT_EXCHANGE = "Bybit"      # 例如: "Binance", "Bybit", "Gate.io", "OKX"
DEFAULT_SYMBOL = "ETHUSDT"      # 交易對，例如 "BTCUSDT"
DEFAULT_START_DATE = "2024-01-01"   # 起始日期 (UTC, 格式 YYYY-MM-DD)
DEFAULT_END_DATE   = "2024-01-03"   # 結束日期 (UTC, 格式 YYYY-MM-DD)

# 每次 API 抓取區間（天）及呼叫間隔
CHUNK_DAYS = 5
WAIT_TIME = 0.5

# ---------------------------
# 符號格式轉換函式
# ---------------------------
def adjust_symbol(exchange, symbol):
    """
    根據不同交易所，調整交易對格式：
      - Gate.io: "BTCUSDT" -> "BTC_USDT"
      - OKX:    "BTCUSDT" -> "BTC-USDT-SWAP"
      - Binance、Bybit 則不轉換
    """
    exch = exchange.lower()
    if exch == "gate.io":
        return symbol[:-4] + "_" + symbol[-4:]
    elif exch == "okx":
        return symbol[:-4] + "-" + symbol[-4:] + "-SWAP"
    else:
        return symbol

# ---------------------------
# 數據庫操作函數
# ---------------------------
def check_existing_data(symbol, exchange, start_dt, end_dt):
    """
    檢查數據庫中已存在的資金費率數據
    返回需要獲取的時間範圍列表
    """
    try:
        db = DatabaseManager()
        
        # 查詢現有數據的時間範圍
        query = """
            SELECT MIN(timestamp_utc) as min_time, MAX(timestamp_utc) as max_time, COUNT(*) as count
            FROM funding_rate_history 
            WHERE symbol = ? AND exchange = ?
        """
        
        with db.get_connection() as conn:
            result = conn.execute(query, (symbol, exchange.lower())).fetchone()
        
        if not result or result[2] == 0:
            # 無現有數據，需要獲取完整範圍
            print(f"數據庫中無現有數據，需要獲取完整範圍")
            return [(start_dt, end_dt)]
        
        existing_start = pd.to_datetime(result[0])
        existing_end = pd.to_datetime(result[1])
        print(f"現有數據範圍：{existing_start} ~ {existing_end}")
        
        # 計算需要補充的時間範圍
        missing_ranges = []
        
        # 前段缺失
        if start_dt < existing_start:
            missing_ranges.append((start_dt, existing_start - pd.Timedelta(hours=1)))
            print(f"需要補充前段：{start_dt} ~ {existing_start - pd.Timedelta(hours=1)}")
        
        # 後段缺失
        if end_dt > existing_end:
            missing_ranges.append((existing_end + pd.Timedelta(hours=1), end_dt))
            print(f"需要補充後段：{existing_end + pd.Timedelta(hours=1)} ~ {end_dt}")
        
        if not missing_ranges:
            print("數據已完整，無需更新")
        
        return missing_ranges
        
    except Exception as e:
        print(f"檢查現有數據時出錯: {e}")
        return [(start_dt, end_dt)]

def save_to_database(data, exchange, symbol):
    """
    將資金費率數據保存到數據庫
    """
    db_data = []
    
    for dt_hour, rate in data.items():
        # 保持API原始邏輯：如果是"null"或沒有數據，保存為None
        rate_value = None if rate == "null" else rate
        
        db_data.append({
            'timestamp_utc': dt_hour,
            'symbol': symbol,
            'exchange': exchange,
            'funding_rate': rate_value
        })
    
    # 轉換為DataFrame
    df = pd.DataFrame(db_data)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    
    # 保存到數據庫
    db = DatabaseManager()
    inserted_count = db.insert_funding_rate_history(df)
    print(f"✅ 數據庫插入: {inserted_count} 條記錄 ({symbol}_{exchange})")
    
    return inserted_count

# ---------------------------
# 新增：檢查資料是否全為 null 的工具函式
# ---------------------------
def is_all_null(data):
    """
    檢查傳入的資料字典所有 funding rate 是否皆為 "null"。
    若資料為空也視為全 null。
    """
    if not data:
        return True
    for rate in data.values():
        if rate != "null":
            return False
    return True

# ---------------------------
# API 資金費率抓取函式
# ---------------------------
def fetch_binance_funding_rates(symbol, start_dt, end_dt):
    all_data = []
    current_dt = start_dt
    while current_dt < end_dt:
        fetch_end = min(current_dt + datetime.timedelta(days=CHUNK_DAYS), end_dt)
        params = {
            "symbol": symbol,
            "startTime": int(current_dt.timestamp() * 1000),
            "endTime": int(fetch_end.timestamp() * 1000),
            "limit": 1000
        }
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)
            print(f"[Binance] {symbol} {current_dt.strftime('%Y-%m-%d')} ~ {fetch_end.strftime('%Y-%m-%d')} 取得 {len(data)} 筆")
        except Exception as e:
            print(f"[Binance] {symbol} {current_dt.strftime('%Y-%m-%d')} 錯誤: {e}")
        time.sleep(WAIT_TIME)
        current_dt = fetch_end
    return all_data

def fetch_bybit_funding_rates(symbol, start_dt, end_dt, category="linear"):
    all_data = []
    current_dt = start_dt
    while current_dt < end_dt:
        fetch_end = min(current_dt + datetime.timedelta(days=CHUNK_DAYS), end_dt)
        params = {
            "category": category,
            "symbol": symbol,
            "startTime": int(current_dt.timestamp() * 1000),
            "endTime": int(fetch_end.timestamp() * 1000),
            "limit": 200
        }
        url = "https://api.bybit.com/v5/market/funding/history"
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            if result.get("retCode") == 0 and result.get("result", {}).get("list"):
                data = result["result"]["list"]
                all_data.extend(data)
                print(f"[Bybit] {symbol} {current_dt.strftime('%Y-%m-%d')} ~ {fetch_end.strftime('%Y-%m-%d')} 取得 {len(data)} 筆")
            else:
                print(f"[Bybit] {symbol} {current_dt.strftime('%Y-%m-%d')} 無資料或 API 錯誤，回傳: {result}")
        except Exception as e:
            print(f"[Bybit] {symbol} {current_dt.strftime('%Y-%m-%d')} 錯誤: {e}")
        time.sleep(WAIT_TIME)
        current_dt = fetch_end
    return all_data

def fetch_gateio_funding_rates(symbol, start_dt, end_dt):
    all_data = []
    contract = adjust_symbol("gate.io", symbol)
    current_dt = start_dt
    while current_dt < end_dt:
        fetch_end = min(current_dt + datetime.timedelta(days=CHUNK_DAYS), end_dt)
        params = {
            "contract": contract,
            "start": int(current_dt.timestamp()),  # Gate.io 使用秒
            "end": int(fetch_end.timestamp()),
            "limit": 1000
        }
        url = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate_history"
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)
            print(f"[Gate.io] {contract} {current_dt.strftime('%Y-%m-%d')} ~ {fetch_end.strftime('%Y-%m-%d')} 取得 {len(data)} 筆")
        except Exception as e:
            print(f"[Gate.io] {contract} {current_dt.strftime('%Y-%m-%d')} 錯誤: {e}")
        time.sleep(WAIT_TIME)
        current_dt = fetch_end
    return all_data

def fetch_okx_funding_rates(symbol, start_dt, end_dt):
    """
    從 OKX API 獲取資金費率歷史數據（使用分頁參數 after）
    此 API 僅能查詢最近三個月內的數據，故本函式從最新日期向較舊方向回溯
    """
    all_data = []
    instId = adjust_symbol("okx", symbol)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    params = {
        "instId": instId,
        "limit": 100
    }
    while True:
        url = "https://www.okx.com/api/v5/public/funding-rate-history"
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
        except Exception as e:
            print(f"[OKX] 請求錯誤: {e}")
            break
        if result.get("code") != "0":
            print(f"[OKX] API 錯誤，回傳: {result}")
            break
        data = result.get("data", [])
        if not data:
            print("[OKX] 無更多數據")
            break
        for record in data:
            try:
                ft = int(record["fundingTime"])
            except Exception as e:
                print("解析 fundingTime 錯誤:", record.get("fundingTime"), e)
                continue
            if ft > end_ms:
                continue
            if ft < start_ms:
                all_data.append(record)
                print("[OKX] 已達查詢區間下限")
                return all_data
            all_data.append(record)
        last_record = data[-1]
        last_ft = int(last_record["fundingTime"])
        if last_ft <= start_ms:
            break
        params["after"] = last_ft
        time.sleep(WAIT_TIME)
    return all_data

def aggregate_hourly(raw_data, start_dt, end_dt):
    """
    將原始資料依每1小時彙整：
      - 每筆資料轉換成整點時間 (格式 %Y-%m-%d %H:%M:%S)
      - 若該小時無資料，則直接記錄為 "null"，
        代表該時間點尚未結算資金費用或API無返回值。
    """
    data_points = {}
    for item in raw_data:
        ts = None
        if "fundingTime" in item:  # OKX, Binance (毫秒)
            try:
                ts = int(item["fundingTime"])
            except Exception as e:
                print("Error parsing fundingTime:", item.get("fundingTime"), e)
        elif "fundingRateTimestamp" in item:  # Bybit (毫秒)
            try:
                ts = int(item["fundingRateTimestamp"])
            except Exception as e:
                print("Error parsing fundingRateTimestamp:", item.get("fundingRateTimestamp"), e)
        elif "funding_time" in item:  # Gate.io (秒)
            try:
                ts = int(item["funding_time"]) * 1000
            except Exception as e:
                print("Error parsing funding_time:", item.get("funding_time"), e)
        if ts is None:
            continue
        dt = datetime.datetime.fromtimestamp(ts / 1000, datetime.timezone.utc)
        dt_hour = dt.replace(minute=0, second=0, microsecond=0)
        try:
            rate = float(item.get("fundingRate", 0))
        except Exception as e:
            print("Error parsing fundingRate:", item.get("fundingRate"), e)
            rate = 0.0
        # 記錄此整點的資金費率
        data_points[dt_hour] = rate
        print(f"Parsed data point: {dt_hour} -> fundingRate: {rate}")

    # 依照 start_dt ~ end_dt 每小時產生一筆結果，若該整點無資料則記為 "null"
    aggregated = {}
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    while current <= end_dt:
        ts_str = current.strftime("%Y-%m-%d %H:%M:%S")
        if current in data_points:
            aggregated[ts_str] = f"{data_points[current]:.8f}"
        else:
            aggregated[ts_str] = "null"
        current += datetime.timedelta(hours=1)
    return aggregated

# ---------------------------
# 主程式：純數據庫操作
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="抓取 Funding Rate 歷史資料並保存到數據庫")
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE, help="交易所，例如 Binance, Bybit, Gate.io, OKX")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="交易對，例如 BTCUSDT")
    parser.add_argument("--start_date", default=DEFAULT_START_DATE, help="起始日期 (YYYY-MM-DD, UTC)")
    parser.add_argument("--end_date", default=DEFAULT_END_DATE, help="結束日期 (YYYY-MM-DD, UTC)")
    args = parser.parse_args()

    EXCHANGE = args.exchange
    SYMBOL = args.symbol
    START_DATE = args.start_date
    END_DATE = args.end_date

    start_dt = datetime.datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    end_dt   = datetime.datetime.strptime(END_DATE, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    
    # 確保end_date包含當天的完整24小時數據
    end_dt = end_dt.replace(hour=23, minute=59, second=59)

    print(f"開始抓取 {EXCHANGE} {SYMBOL} 從 {START_DATE} 到 {END_DATE} 的 Funding Rate 資料")

    exch = EXCHANGE.lower()
    if exch == "binance":
        fetch_func = fetch_binance_funding_rates
    elif exch == "bybit":
        fetch_func = fetch_bybit_funding_rates
    elif exch == "gate.io":
        fetch_func = fetch_gateio_funding_rates
    elif exch == "okx":
        fetch_func = fetch_okx_funding_rates
    else:
        print(f"不支援的交易所：{EXCHANGE}")
        return

    # 檢查數據庫中的現有數據
    missing_ranges = check_existing_data(SYMBOL, EXCHANGE, start_dt, end_dt)
    
    if not missing_ranges:
        print("數據已完整，無需更新")
        return

    total_saved = 0
    
    # 對每個缺失範圍獲取數據
    for range_start, range_end in missing_ranges:
        print(f"📡 獲取數據範圍: {range_start} ~ {range_end}")
        
        raw_data = fetch_func(SYMBOL, range_start, range_end)
        
        if not raw_data:
            print("⚠️ API未返回數據")
            continue
        
        aggregated_data = aggregate_hourly(raw_data, range_start, range_end)
        
        if is_all_null(aggregated_data):
            print("⚠️ 查詢結果全為null，可能是未來日期或API異常")
            continue
        
        # 保存到數據庫
        saved_count = save_to_database(aggregated_data, EXCHANGE, SYMBOL)
        total_saved += saved_count

    if total_saved > 0:
        print(f"🎉 總共保存 {total_saved} 條記錄到數據庫")
    else:
        print("ℹ️ 沒有新數據需要保存")

if __name__ == "__main__":
    main()