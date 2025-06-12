#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import datetime
import subprocess
import time
import sys
import argparse
import pandas as pd

# 添加數據庫支持
from database_operations import DatabaseManager

# --------------------------------------
# 1. 取得專案根目錄，定義相對路徑
# --------------------------------------
project_root = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------
# 2. 檔案路徑設定（使用相對路徑）
# --------------------------------------
TEST_SCRIPT = os.path.join(project_root, "fetch_FR_history.py")  # 外部程式檔案
LOG_FILE = os.path.join(project_root, "logs", "scheduler_log.txt")

# 確保日誌目錄存在
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# --------------------------------------
# 3. 查詢參數預設值
# --------------------------------------
DEFAULT_START_DATE = "2025-06-06"  # 起始日期 (UTC, 格式 YYYY-MM-DD)
DEFAULT_END_DATE = "2025-06-10"  # 結束日期 (UTC, 格式 YYYY-MM-DD)
TOP_N = 500  # 取前 TOP_N 筆市值排名交易對
SELECTED_EXCHANGES = ["binance", "bybit"]  # 選擇要查詢的交易所

# --------------------------------------
# 4. 日誌紀錄函式
# --------------------------------------
def log_message(msg):
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} - {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# --------------------------------------
# 5. 讀取 trading_pairs 數據
# --------------------------------------
def read_trading_pairs_from_database():
    """
    從數據庫讀取交易對數據，回傳資料列表，每筆為 dict。
    """
    try:
        db = DatabaseManager()
        df = db.get_trading_pairs(min_market_cap=0)  # 獲取所有交易對

        if df.empty:
            log_message("⚠️ 數據庫中沒有交易對數據，請先運行 get_symbol_pair_v2.py")
            return []

        # 轉換為字典列表，保持與原CSV格式兼容
        data = []
        for _, row in df.iterrows():
            data.append({
                'Symbol': row['symbol'],
                'Exchange_A': row['exchange_a'],
                'Exchange_B': row['exchange_b'],
                'Market_Cap': row['market_cap'] if pd.notna(row['market_cap']) else 0,
                'FR_Date': row['fr_date'] if pd.notna(row['fr_date']) else ''
            })

        log_message(f"✅ 從數據庫讀取到 {len(data)} 筆交易對資料")
        return data

    except Exception as e:
        log_message(f"❌ 讀取交易對數據時出錯: {e}")
        return []

# --------------------------------------
# 6. 更新 trading_pairs 數據庫的 FR_Date 欄位
# --------------------------------------
def update_trading_pairs_database(updates):
    """
    更新數據庫中交易對的 FR_Date 欄位
    Args:
        updates: {symbol: fr_date} 字典
    """
    if not updates:
        return

    try:
        db = DatabaseManager()

        # 批量更新FR_Date
        with db.get_connection() as conn:
            for symbol, fr_date in updates.items():
                conn.execute('''
                    UPDATE trading_pairs 
                    SET fr_date = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE symbol = ?
                ''', (fr_date, symbol))

            log_message(f"✅ 已更新 {len(updates)} 個交易對的 FR_Date")

    except Exception as e:
        log_message(f"❌ 更新交易對 FR_Date 時出錯: {e}")

# --------------------------------------
# 7. 呼叫外部程式 fetch_FR_history.py（智能增量更新）
# --------------------------------------
def run_funding_rate_script_for_database(exchange, symbol, target_start_date, target_end_date):
    """
    調用 API 獲取資金費率數據並直接存入數據庫
    """
    log_message(f"{symbol}_{exchange}: 獲取數據 {target_start_date} 至 {target_end_date}")

    cmd = [
        sys.executable,
        TEST_SCRIPT,
        "--exchange", exchange,
        "--symbol", symbol,
        "--start_date", target_start_date,
        "--end_date", target_end_date
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True
        )
        log_message(f"✅ API調用成功: {target_start_date} ~ {target_end_date}")

        # fetch_FR_history.py 已經直接將數據保存到數據庫，不需要處理CSV文件
        # 檢查輸出中是否包含成功保存的信息
        if "數據庫插入:" in result.stdout or "保存" in result.stdout:
            log_message(f"✅ 數據已直接保存到數據庫 ({symbol}_{exchange})")
        else:
            log_message(f"⚠️ 未檢測到數據庫保存確認信息")

        # 檢查並清理可能存在的臨時CSV文件
        temp_filename = f"funding_rate_{exchange}_{symbol}.csv"
        temp_filepath = os.path.join(project_root, "csv", "FR_history", temp_filename)

        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            log_message(f"🗑️ 已清理臨時文件: {temp_filepath}")

        time.sleep(1)  # API限制
        return True

    except subprocess.CalledProcessError as e:
        log_message(f"❌ API調用失敗 {target_start_date}~{target_end_date}: {e.stderr}")
        return False
    except OSError as e:
        log_message(f"❌ 文件操作失敗: {e}")
        return False

# --------------------------------------
# 8. 收集所有市值>0的交易對及其涉及的交易所
# --------------------------------------
def collect_valid_symbols_and_exchanges(pairs):
    """
    從 trading_pairs 數據中收集所有 Market_Cap > 0 的交易對，
    並記錄每個交易對涉及的所有交易所
    回傳: {symbol: set(exchanges)}
    """
    symbol_exchanges = {}

    for row in pairs:
        try:
            market_cap = float(row.get("Market_Cap", "0") or "0")
            if market_cap > 0:
                symbol = row["Symbol"].strip()
                exchange_a = row.get("Exchange_A", "").strip()
                exchange_b = row.get("Exchange_B", "").strip()

                if symbol not in symbol_exchanges:
                    symbol_exchanges[symbol] = set()

                if exchange_a:
                    symbol_exchanges[symbol].add(exchange_a.lower())
                if exchange_b:
                    symbol_exchanges[symbol].add(exchange_b.lower())
        except (ValueError, TypeError) as e:
            log_message(f"❌ 解析市值時錯誤: {e}, 資料: {row}")

    return symbol_exchanges

# --------------------------------------
# 9. 主程式流程：智能增量更新
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(description="智能增量抓取市值>0交易對的 Funding Rate 資料")
    parser.add_argument("--start_date", default=DEFAULT_START_DATE, help="起始日期 (UTC, YYYY-MM-DD)")
    parser.add_argument("--end_date", default=DEFAULT_END_DATE, help="結束日期 (UTC, YYYY-MM-DD)")
    parser.add_argument("--top_n", type=int, default=TOP_N, help="選取市值前幾筆交易對")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    top_n = args.top_n

    log_message(f"開始智能增量處理市值>0的交易對，目標區間：{start_date} ~ {end_date}")

    # 讀取 trading_pairs 數據庫
    pairs = read_trading_pairs_from_database()
    if not pairs:
        log_message("❌ 無法讀取交易對數據，程序終止")
        return

    log_message(f"讀取到 {len(pairs)} 筆交易對資料。")

    # 收集所有市值>0的交易對及其涉及的交易所
    symbol_exchanges = collect_valid_symbols_and_exchanges(pairs)
    log_message(f"找到 {len(symbol_exchanges)} 個市值>0的交易對。")

    # 按市值排序，取前 top_n 個
    valid_pairs = [row for row in pairs if float(row.get("Market_Cap", "0") or "0") > 0]
    try:
        valid_pairs.sort(key=lambda r: float(r.get("Market_Cap", "0") or "0"), reverse=True)
    except Exception as e:
        log_message(f"❌ 排序 Market_Cap 時錯誤: {e}")

    # 取出前 top_n 個交易對的 symbol (去重)
    top_symbols = []
    seen_symbols = set()
    for row in valid_pairs:
        symbol = row["Symbol"].strip()
        if symbol not in seen_symbols and len(top_symbols) < top_n:
            top_symbols.append(symbol)
            seen_symbols.add(symbol)

    log_message(f"選取前 {len(top_symbols)} 個交易對進行智能增量處理。")

    # 處理每個交易對
    updates = {}
    for symbol in top_symbols:
        log_message(f"開始智能增量處理交易對 {symbol}")

        # 找到該交易對支持的交易所
        available_exchanges = symbol_exchanges.get(symbol, set())
        log_message(f"  可用交易所: {', '.join(sorted(available_exchanges))}")

        # 選擇要查詢的交易所（與SELECTED_EXCHANGES的交集）
        selected_exchanges = [ex for ex in SELECTED_EXCHANGES if ex in available_exchanges]
        if not selected_exchanges:
            log_message(f"  ⚠️ 沒有可用的選定交易所，跳過")
            continue

        log_message(f"  選擇的交易所: {', '.join(selected_exchanges)}")

        # 對每個選定的交易所獲取數據
        symbol_success = True
        for exchange in selected_exchanges:
            success = run_funding_rate_script_for_database(exchange, symbol, start_date, end_date)
            if not success:
                symbol_success = False

        if symbol_success:
            updates[symbol] = end_date

    # 更新數據庫中的 FR_Date 欄位
    if updates:
        update_trading_pairs_database(updates)

    log_message("✅ 智能增量處理完成")

if __name__ == "__main__":
    main()