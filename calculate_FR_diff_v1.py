#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import argparse
import datetime
from pathlib import Path

# 添加數據庫支持
from database_operations import DatabaseManager

# --------------------------------------
# 1. 取得專案根目錄
# --------------------------------------
project_root = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------
# 2. 日誌設定
# --------------------------------------
LOG_FILE = os.path.join(project_root, "logs", "calculate_FR_diff_log.txt")

# 確保日誌目錄存在
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log_message(msg):
    """記錄日誌訊息"""
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} - {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# --------------------------------------
# 3. 從數據庫讀取資金費率歷史數據
# --------------------------------------
def read_funding_rate_history_from_database(symbol=None, exchanges=None, start_date=None, end_date=None):
    """
    從數據庫讀取資金費率歷史數據
    Args:
        symbol: 交易對符號，None表示所有
        exchanges: 交易所列表，None表示所有
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
    Returns:
        DataFrame with columns: timestamp_utc, symbol, exchange, funding_rate
    """
    try:
        db = DatabaseManager()
        
        # 構建查詢條件
        where_conditions = []
        params = []
        
        if symbol:
            where_conditions.append("symbol = ?")
            params.append(symbol)
            
        if exchanges:
            placeholders = ','.join(['?' for _ in exchanges])
            where_conditions.append(f"exchange IN ({placeholders})")
            params.extend(exchanges)
        
        if start_date:
            where_conditions.append("DATE(timestamp_utc) >= ?")
            params.append(start_date)
            
        if end_date:
            where_conditions.append("DATE(timestamp_utc) <= ?")
            params.append(end_date)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT timestamp_utc, symbol, exchange, funding_rate
            FROM funding_rate_history 
            {where_clause}
            ORDER BY symbol, timestamp_utc, exchange
        """
        
        with db.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            log_message("⚠️ 數據庫中沒有符合條件的資金費率歷史數據")
            return pd.DataFrame()
        
        # 轉換時間戳
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        log_message(f"✅ 從數據庫讀取到 {len(df)} 條資金費率歷史記錄")
        
        return df
        
    except Exception as e:
        log_message(f"❌ 從數據庫讀取資金費率歷史數據時出錯: {e}")
        return pd.DataFrame()

# --------------------------------------
# 4. 計算資金費率差異
# --------------------------------------
def calculate_funding_rate_differences(df, exchange_pairs):
    """
    計算指定交易所對之間的資金費率差異
    Args:
        df: 包含資金費率數據的DataFrame
        exchange_pairs: 交易所對列表，如[('binance', 'bybit')]
    Returns:
        差異數據的DataFrame
    """
    if df.empty:
        log_message("⚠️ 輸入數據為空，無法計算差異")
        return pd.DataFrame()
    
    all_differences = []
    
    for exchange_a, exchange_b in exchange_pairs:
        log_message(f"計算 {exchange_a} vs {exchange_b} 的資金費率差異")
        
        # 分別獲取兩個交易所的數據
        df_a = df[df['exchange'] == exchange_a].copy()
        df_b = df[df['exchange'] == exchange_b].copy()
        
        if df_a.empty or df_b.empty:
            log_message(f"⚠️ {exchange_a} 或 {exchange_b} 的數據為空，跳過此交易所對")
            continue
        
        # 按symbol和timestamp合併數據
        merged = pd.merge(
            df_a[['timestamp_utc', 'symbol', 'funding_rate']],
            df_b[['timestamp_utc', 'symbol', 'funding_rate']],
            on=['timestamp_utc', 'symbol'],
            suffixes=(f'_{exchange_a}', f'_{exchange_b}'),
            how='inner'
        )
        
        if merged.empty:
            log_message(f"⚠️ {exchange_a} 和 {exchange_b} 沒有匹配的時間戳數據")
            continue
        
        # 計算差異 (exchange_a - exchange_b)
        merged['rate_diff'] = merged[f'funding_rate_{exchange_a}'] - merged[f'funding_rate_{exchange_b}']
        merged['exchange_a'] = exchange_a
        merged['exchange_b'] = exchange_b
        
        # 選擇輸出列
        result_df = merged[['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 
                          f'funding_rate_{exchange_a}', f'funding_rate_{exchange_b}', 'rate_diff']].copy()
        
        # 重命名列以標準化，匹配數據庫期望的列名
        result_df = result_df.rename(columns={
            f'funding_rate_{exchange_a}': 'funding_rate_a',
            f'funding_rate_{exchange_b}': 'funding_rate_b',
            'rate_diff': 'diff_ab'
        })
        
        all_differences.append(result_df)
        log_message(f"✅ 計算完成 {exchange_a} vs {exchange_b}: {len(result_df)} 條差異記錄")
    
    if all_differences:
        final_df = pd.concat(all_differences, ignore_index=True)
        final_df = final_df.sort_values(['symbol', 'timestamp_utc', 'exchange_a', 'exchange_b'])
        log_message(f"✅ 總共計算出 {len(final_df)} 條資金費率差異記錄")
        return final_df
    else:
        log_message("⚠️ 沒有計算出任何差異數據")
        return pd.DataFrame()

# --------------------------------------
# 5. 保存差異數據到數據庫
# --------------------------------------
def save_differences_to_database(df):
    """
    保存資金費率差異數據到數據庫
    Args:
        df: 差異數據DataFrame
    Returns:
        保存的記錄數
    """
    if df.empty:
        log_message("⚠️ 差異數據為空，無法保存")
        return 0
    
    try:
        db = DatabaseManager()
        
        # 準備數據庫格式的數據
        db_df = df.copy()
        
        # 確保必要的列存在並格式正確
        required_columns = ['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'funding_rate_a', 'funding_rate_b', 'diff_ab']
        for col in required_columns:
            if col not in db_df.columns:
                log_message(f"❌ 缺少必要列: {col}")
                return 0
        
        # 轉換時間戳格式
        db_df['timestamp_utc'] = pd.to_datetime(db_df['timestamp_utc'])
        
        # 保存到數據庫
        inserted_count = db.insert_funding_rate_diff(db_df)
        log_message(f"✅ 成功保存 {inserted_count} 條差異記錄到數據庫")
        return inserted_count
        
    except Exception as e:
        log_message(f"❌ 保存差異數據到數據庫時出錯: {e}")
        return 0

# --------------------------------------
# 6. 主程式
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(description="計算交易所間資金費率差異並保存到數據庫")
    parser.add_argument("--symbol", help="指定交易對符號 (可選)")
    parser.add_argument("--start_date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--exchanges", nargs='+', default=['binance', 'bybit'], 
                       help="要比較的交易所列表")
    
    args = parser.parse_args()
    
    log_message("=" * 50)
    log_message("開始計算資金費率差異")
    log_message(f"參數: symbol={args.symbol}, start_date={args.start_date}, end_date={args.end_date}")
    log_message(f"交易所: {args.exchanges}")
    
    # 從數據庫讀取資金費率歷史數據
    df = read_funding_rate_history_from_database(
        symbol=args.symbol,
        exchanges=args.exchanges,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    if df.empty:
        log_message("❌ 沒有可用的資金費率歷史數據，程序結束")
        return
    
    # 生成交易所對組合
    exchange_pairs = []
    exchanges = args.exchanges
    for i in range(len(exchanges)):
        for j in range(i + 1, len(exchanges)):
            exchange_pairs.append((exchanges[i], exchanges[j]))
    
    log_message(f"將計算以下交易所對的差異: {exchange_pairs}")
    
    # 計算差異
    diff_df = calculate_funding_rate_differences(df, exchange_pairs)
    
    if diff_df.empty:
        log_message("❌ 沒有計算出差異數據，程序結束")
        return
    
    # 保存到數據庫
    saved_count = save_differences_to_database(diff_df)
    
    if saved_count > 0:
        log_message(f"✅ 資金費率差異計算完成，共保存 {saved_count} 條記錄")
    else:
        log_message("❌ 保存數據失敗")
    
    log_message("=" * 50)

if __name__ == "__main__":
    main()