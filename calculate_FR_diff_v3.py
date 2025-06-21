#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
資金費率差異計算模組 - Pandas優化版本 v3
解決V2版本的INNER JOIN問題，實現完整的時間軸覆蓋和智能增量處理

=== V3 主要改進 ===
1. 使用Pandas在記憶體中處理，避免SQL JOIN丟失數據
2. 創建完整時間軸，確保每小時都有記錄
3. 實現智能增量與回填：自動檢測缺失範圍並補充
4. 正確處理NULL值：有數據-null=有數據，null-有數據=-有數據，null-null=null
5. 支持靈活的交易所組合配置
"""

import os
import pandas as pd
import argparse
import datetime
from pathlib import Path
from typing import List, Tuple, Set, Optional
import numpy as np

# 添加數據庫支持
from database_operations import DatabaseManager

# --------------------------------------
# 1. 取得專案根目錄和日誌設定
# --------------------------------------
project_root = os.path.dirname(os.path.abspath(__file__))
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
# 2. 智能增量檢測與範圍計算
# --------------------------------------
def get_data_range_info(symbol: str = None) -> dict:
    """
    獲取來源數據和結果數據的範圍信息
    
    Returns:
        dict: 包含來源和結果數據範圍的信息
    """
    db = DatabaseManager()
    
    try:
        # 查詢來源數據範圍
        source_query = """
            SELECT 
                MIN(timestamp_utc) as earliest_timestamp,
                MAX(timestamp_utc) as latest_timestamp,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols
            FROM funding_rate_history
        """
        source_params = []
        
        if symbol:
            source_query += " WHERE symbol = ?"
            source_params.append(symbol)
        
        with db.get_connection() as conn:
            source_info = pd.read_sql_query(source_query, conn, params=source_params).iloc[0]
        
        # 查詢結果數據範圍
        result_query = """
            SELECT 
                MIN(timestamp_utc) as earliest_timestamp,
                MAX(timestamp_utc) as latest_timestamp,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols
            FROM funding_rate_diff
        """
        result_params = []
        
        if symbol:
            result_query += " WHERE symbol = ?"
            result_params.append(symbol)
        
        with db.get_connection() as conn:
            result_info = pd.read_sql_query(result_query, conn, params=result_params).iloc[0]
        
        return {
            'source': {
                'earliest': source_info['earliest_timestamp'],
                'latest': source_info['latest_timestamp'],
                'records': source_info['total_records'],
                'symbols': source_info['unique_symbols']
            },
            'result': {
                'earliest': result_info['earliest_timestamp'],
                'latest': result_info['latest_timestamp'],
                'records': result_info['total_records'],
                'symbols': result_info['unique_symbols']
            }
        }
        
    except Exception as e:
        log_message(f"⚠️ 獲取數據範圍信息時出錯: {e}")
        return {
            'source': {'earliest': None, 'latest': None, 'records': 0, 'symbols': 0},
            'result': {'earliest': None, 'latest': None, 'records': 0, 'symbols': 0}
        }

def calculate_processing_ranges(symbol: str = None, start_date: str = None, end_date: str = None) -> List[Tuple[str, str]]:
    """
    智能計算需要處理的日期範圍
    
    Args:
        symbol: 指定交易對（可選）
        start_date: 用戶指定開始日期（可選）
        end_date: 用戶指定結束日期（可選）
    
    Returns:
        List[Tuple[str, str]]: 需要處理的日期範圍列表 [(start, end), ...]
    """
    log_message("🔍 分析數據範圍，計算處理策略...")
    
    range_info = get_data_range_info(symbol)
    source_info = range_info['source']
    result_info = range_info['result']
    
    log_message(f"📊 來源數據範圍: {source_info['earliest']} ~ {source_info['latest']} ({source_info['records']} 條)")
    log_message(f"📊 結果數據範圍: {result_info['earliest']} ~ {result_info['latest']} ({result_info['records']} 條)")
    
    # 如果用戶指定了日期範圍，直接使用
    if start_date and end_date:
        log_message(f"📅 使用用戶指定範圍: {start_date} ~ {end_date}")
        return [(start_date, end_date)]
    
    # 如果來源數據為空，無法處理
    if not source_info['earliest'] or source_info['records'] == 0:
        log_message("⚠️ 來源數據為空，無法計算差異")
        return []
    
    # 如果結果數據為空，處理全部來源數據
    if not result_info['earliest'] or result_info['records'] == 0:
        log_message("📝 結果數據為空，將處理全部來源數據")
        source_start = pd.to_datetime(source_info['earliest']).strftime('%Y-%m-%d')
        source_end = pd.to_datetime(source_info['latest']).strftime('%Y-%m-%d')
        return [(source_start, source_end)]
    
    # 智能增量與回填策略
    processing_ranges = []
    
    source_start = pd.to_datetime(source_info['earliest'])
    source_end = pd.to_datetime(source_info['latest'])
    result_start = pd.to_datetime(result_info['earliest'])
    result_end = pd.to_datetime(result_info['latest'])
    
    # 1. 回填歷史空洞（來源數據更早）
    if source_start < result_start:
        backfill_end = (result_start - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        processing_ranges.append((source_start.strftime('%Y-%m-%d'), backfill_end))
        log_message(f"📈 添加歷史回填範圍: {source_start.strftime('%Y-%m-%d')} ~ {backfill_end}")
    
    # 2. 追加新數據（來源數據更新）
    if source_end > result_end:
        # 計算需要處理的新數據範圍
        # 注意：我們需要基於小時而不是天來計算
        append_start = (result_end + pd.Timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        append_end = source_end.strftime('%Y-%m-%d %H:%M:%S')
        
        # 轉換為日期格式用於查詢
        append_start_date = (result_end + pd.Timedelta(hours=1)).strftime('%Y-%m-%d')
        append_end_date = source_end.strftime('%Y-%m-%d')
        
        processing_ranges.append((append_start_date, append_end_date))
        log_message(f"📊 添加新數據範圍: {append_start} ~ {append_end} (查詢範圍: {append_start_date} ~ {append_end_date})")
    
    # 如果沒有需要處理的範圍
    if not processing_ranges:
        log_message("✅ 數據已是最新，無需處理")
    
    return processing_ranges

# --------------------------------------
# 3. 獲取資金費率歷史數據
# --------------------------------------
def get_fr_history(symbol: str = None, exchanges: List[str] = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    從數據庫獲取資金費率歷史數據
    
    Args:
        symbol: 交易對符號
        exchanges: 交易所列表
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
    
    Returns:
        DataFrame: 資金費率歷史數據
    """
    db = DatabaseManager()
    
    # 構建查詢
    query = "SELECT * FROM funding_rate_history WHERE 1=1"
    params = []
    
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    
    if exchanges:
        exchange_placeholders = ','.join(['?' for _ in exchanges])
        query += f" AND exchange IN ({exchange_placeholders})"
        params.extend(exchanges)
    
    if start_date:
        query += " AND DATE(timestamp_utc) >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND DATE(timestamp_utc) <= ?"
        params.append(end_date)
    
    query += " ORDER BY timestamp_utc, symbol, exchange"
    
    log_message(f"🔍 查詢資金費率歷史數據: {len(params)} 個參數")
    
    with db.get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    
    if not df.empty:
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        log_message(f"✅ 獲取到 {len(df)} 條歷史數據")
        log_message(f"   📅 時間範圍: {df['timestamp_utc'].min()} ~ {df['timestamp_utc'].max()}")
        log_message(f"   🔗 交易對: {df['symbol'].nunique()}")
        log_message(f"   🏢 交易所: {df['exchange'].nunique()}")
    else:
        log_message("⚠️ 未找到符合條件的歷史數據")
    
    return df

# --------------------------------------
# 4. Pandas版本差異計算（解決INNER JOIN問題）
# --------------------------------------
def calculate_diff_for_symbol(symbol_data: pd.DataFrame, exchange_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    為單個交易對計算所有交易所對的差異
    使用Pandas避免SQL JOIN丟失數據的問題
    
    Args:
        symbol_data: 單個交易對的資金費率數據
        exchange_pairs: 交易所對列表
    
    Returns:
        DataFrame: 差異計算結果
    """
    if symbol_data.empty:
        return pd.DataFrame()
    
    symbol = symbol_data['symbol'].iloc[0]
    
    # 創建完整時間軸
    time_range = pd.date_range(
        start=symbol_data['timestamp_utc'].min(),
        end=symbol_data['timestamp_utc'].max(),
        freq='h'
    )
    
    # 透視表：將數據重塑為 timestamp x exchange 的格式
    pivot_df = symbol_data.pivot_table(
        index='timestamp_utc',
        columns='exchange',
        values='funding_rate',
        aggfunc='first'  # 如果有重複，取第一個值
    )
    
    # 重新索引到完整時間軸，缺失值保持為NaN
    pivot_df = pivot_df.reindex(time_range)
    
    # 計算所有交易所對的差異
    diff_results = []
    
    for exchange_a, exchange_b in exchange_pairs:
        if exchange_a not in pivot_df.columns or exchange_b not in pivot_df.columns:
            log_message(f"⚠️ {symbol}: 缺少交易所數據 {exchange_a} 或 {exchange_b}")
            continue
        
        # 獲取兩個交易所的費率數據
        rate_a = pivot_df[exchange_a]
        rate_b = pivot_df[exchange_b]
        
        # 計算差異：實現正確的NULL處理邏輯
        # 有數據 - null = 有數據
        # null - 有數據 = -有數據  
        # null - null = null
        
        # 使用自定義邏輯處理NULL值
        diff = pd.Series(index=rate_a.index, dtype=float)
        
        for idx in rate_a.index:
            a_val = rate_a.loc[idx]
            b_val = rate_b.loc[idx]
            
            if pd.notna(a_val) and pd.notna(b_val):
                # 有數據 - 有數據 = 正常計算
                diff.loc[idx] = a_val - b_val
            elif pd.notna(a_val) and pd.isna(b_val):
                # 有數據 - null = 有數據
                diff.loc[idx] = a_val
            elif pd.isna(a_val) and pd.notna(b_val):
                # null - 有數據 = -有數據
                diff.loc[idx] = -b_val
            else:
                # null - null = null
                diff.loc[idx] = np.nan
        
        # 創建結果DataFrame
        result_df = pd.DataFrame({
            'timestamp_utc': diff.index,
            'symbol': symbol,
            'exchange_a': exchange_a,
            'exchange_b': exchange_b,
            'funding_rate_a': rate_a.values,
            'funding_rate_b': rate_b.values,
            'diff_ab': diff.values
        })
        
        # 重要：保留所有記錄，包括diff_ab為NaN的情況
        # 這解決了V2版本INNER JOIN丟失數據的問題
        diff_results.append(result_df)
    
    if diff_results:
        final_result = pd.concat(diff_results, ignore_index=True)
        log_message(f"✅ {symbol}: 計算完成 {len(final_result)} 條差異記錄")
        return final_result
    else:
        log_message(f"⚠️ {symbol}: 沒有可計算的交易所對")
        return pd.DataFrame()

def calculate_funding_rate_differences_v3(df: pd.DataFrame, exchange_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    V3版本：使用Pandas計算資金費率差異
    解決V2版本的INNER JOIN問題
    
    Args:
        df: 資金費率歷史數據
        exchange_pairs: 交易所對列表
    
    Returns:
        DataFrame: 所有差異計算結果
    """
    if df.empty:
        log_message("⚠️ 輸入數據為空")
        return pd.DataFrame()
    
    log_message("🚀 V3版本：開始Pandas差異計算...")
    log_message(f"📊 輸入數據: {len(df)} 條記錄")
    log_message(f"🔗 交易對數量: {df['symbol'].nunique()}")
    log_message(f"🏢 交易所對: {exchange_pairs}")
    
    all_results = []
    symbols = df['symbol'].unique()
    
    for i, symbol in enumerate(symbols, 1):
        log_message(f"📈 處理交易對 {i}/{len(symbols)}: {symbol}")
        
        # 獲取該交易對的數據
        symbol_data = df[df['symbol'] == symbol].copy()
        
        # 計算該交易對的差異
        symbol_result = calculate_diff_for_symbol(symbol_data, exchange_pairs)
        
        if not symbol_result.empty:
            all_results.append(symbol_result)
    
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        
        # 統計信息
        total_records = len(final_df)
        non_null_records = final_df['diff_ab'].notna().sum()
        null_records = final_df['diff_ab'].isna().sum()
        
        log_message(f"✅ V3計算完成!")
        log_message(f"   📊 總記錄: {total_records}")
        log_message(f"   ✅ 有效差異: {non_null_records}")
        log_message(f"   ⚪ NULL差異: {null_records}")
        log_message(f"   📅 時間範圍: {final_df['timestamp_utc'].min()} ~ {final_df['timestamp_utc'].max()}")
        
        return final_df
    else:
        log_message("❌ 沒有計算出任何差異數據")
        return pd.DataFrame()

# --------------------------------------
# 5. 自定義數據庫插入函數（正確處理NULL值）
# --------------------------------------
def insert_fr_diff_with_nulls(db: DatabaseManager, df: pd.DataFrame) -> int:
    """
    自定義插入函數，正確處理NULL值
    避免使用現有的insert_funding_rate_diff函數，因為它會將NaN轉換為0.0
    
    Args:
        db: 數據庫管理器
        df: 要插入的DataFrame
    
    Returns:
        int: 插入的記錄數
    """
    if df.empty:
        return 0
    
    try:
        with db.get_connection() as conn:
            # 準備插入數據
            data_to_insert = []
            
            for _, row in df.iterrows():
                # 處理每一行，確保NULL值正確處理
                insert_row = []
                
                # timestamp_utc - 必需字段
                insert_row.append(row['timestamp_utc'])
                
                # symbol - 必需字段
                insert_row.append(row['symbol'])
                
                # exchange_a - 必需字段
                insert_row.append(row['exchange_a'])
                
                # funding_rate_a - 可能為NULL
                if pd.isna(row['funding_rate_a']) or row['funding_rate_a'] is None:
                    insert_row.append(None)
                else:
                    insert_row.append(str(row['funding_rate_a']))  # 轉為字符串以符合TEXT類型
                
                # exchange_b - 必需字段
                insert_row.append(row['exchange_b'])
                
                # funding_rate_b - 可能為NULL
                if pd.isna(row['funding_rate_b']) or row['funding_rate_b'] is None:
                    insert_row.append(None)
                else:
                    insert_row.append(str(row['funding_rate_b']))  # 轉為字符串以符合TEXT類型
                
                # diff_ab - 可能為NULL，但數據庫定義為NOT NULL
                if pd.isna(row['diff_ab']) or row['diff_ab'] is None:
                    # 根據數據庫schema，diff_ab是NOT NULL，但我們的業務邏輯需要處理null-null的情況
                    # 為了保持每小時都有記錄，我們將null-null的情況設為0
                    # 但在funding_rate_a和funding_rate_b中正確記錄NULL值
                    insert_row.append(0.0)  # null-null的差異設為0
                else:
                    # 解決浮點數精度問題：四捨五入到8位小數
                    rounded_diff = round(float(row['diff_ab']), 8)
                    insert_row.append(rounded_diff)
                
                data_to_insert.append(tuple(insert_row))
            
            if not data_to_insert:
                log_message("⚠️ 沒有有效數據可插入")
                return 0
            
            # 批量插入
            conn.executemany('''
                INSERT OR REPLACE INTO funding_rate_diff 
                (timestamp_utc, symbol, exchange_a, funding_rate_a, exchange_b, funding_rate_b, diff_ab)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            
            log_message(f"✅ 自定義插入完成: {len(data_to_insert)} 條記錄")
            return len(data_to_insert)
            
    except Exception as e:
        log_message(f"❌ 自定義插入失敗: {e}")
        return 0

# --------------------------------------
# 6. 保存差異數據到數據庫
# --------------------------------------
def save_fr_diff(df: pd.DataFrame) -> int:
    """
    保存資金費率差異數據到數據庫
    
    Args:
        df: 差異數據DataFrame
    
    Returns:
        int: 成功保存的記錄數
    """
    if df.empty:
        log_message("⚠️ 沒有數據需要保存")
        return 0
    
    try:
        db = DatabaseManager()
        
        # 準備數據：處理時間戳格式和NULL值
        save_df = df.copy()
        
        # 轉換時間戳為字符串格式
        save_df['timestamp_utc'] = pd.to_datetime(save_df['timestamp_utc']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 正確處理NULL值：由於數據庫schema中funding_rate_a/b是TEXT類型
        # 我們需要將NaN轉換為None，並確保數值類型正確
        
        # 對於數值列，將NaN轉換為None
        numeric_columns = ['funding_rate_a', 'funding_rate_b', 'diff_ab']
        for col in numeric_columns:
            if col in save_df.columns:
                # 將NaN轉換為None，保持其他值不變
                save_df[col] = save_df[col].where(pd.notna(save_df[col]), None)
        
        # 對於字符串列，確保None值正確處理
        string_columns = ['symbol', 'exchange_a', 'exchange_b']
        for col in string_columns:
            if col in save_df.columns:
                save_df[col] = save_df[col].where(pd.notna(save_df[col]), None)
        
        log_message(f"💾 開始保存 {len(save_df)} 條差異數據...")
        
        # 調試信息：檢查NULL值處理
        null_counts = {}
        for col in ['funding_rate_a', 'funding_rate_b', 'diff_ab']:
            if col in save_df.columns:
                null_count = save_df[col].isna().sum()
                null_counts[col] = null_count
        
        log_message(f"📊 NULL值統計: {null_counts}")
        
        # 自定義插入方法：正確處理NULL值
        inserted_count = insert_fr_diff_with_nulls(db, save_df)
        
        log_message(f"✅ 成功保存 {inserted_count} 條差異數據")
        return inserted_count
        
    except Exception as e:
        log_message(f"❌ 保存差異數據時出錯: {e}")
        return 0

# --------------------------------------
# 7. 主程式
# --------------------------------------
def main():
    log_message("🚀 資金費率差異計算程式啟動 (V3版本)")
    
    parser = argparse.ArgumentParser(description="計算交易所間資金費率差異 - V3版本 (Pandas優化)")
    parser.add_argument("--symbol", help="指定交易對符號 (可選)")
    parser.add_argument("--start-date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--exchanges", nargs='+', default=['binance', 'bybit'],
                        help="要比較的交易所列表")
    parser.add_argument("--force-full", action='store_true', 
                        help="強制全量計算，忽略增量檢測")
    
    args = parser.parse_args()
    
    log_message("=" * 60)
    log_message("📅 資金費率差異計算 V3 (Pandas優化版本)")
    log_message("=" * 60)
    log_message(f"參數: symbol={args.symbol}")
    log_message(f"日期: {args.start_date} ~ {args.end_date}")
    log_message(f"交易所: {args.exchanges}")
    log_message(f"強制全量: {args.force_full}")
    
    try:
        # 生成交易所對組合
        exchange_pairs = []
        exchanges = args.exchanges
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange_pairs.append((exchanges[i], exchanges[j]))
        
        log_message(f"將計算以下交易所對的差異: {exchange_pairs}")
        
        # 確定處理範圍
        if args.force_full or (args.start_date and args.end_date):
            # 使用指定範圍或強制全量
            if args.start_date and args.end_date:
                processing_ranges = [(args.start_date, args.end_date)]
            else:
                # 強制全量：查詢所有來源數據範圍
                range_info = get_data_range_info(args.symbol)
                if range_info['source']['earliest']:
                    start = pd.to_datetime(range_info['source']['earliest']).strftime('%Y-%m-%d')
                    end = pd.to_datetime(range_info['source']['latest']).strftime('%Y-%m-%d')
                    processing_ranges = [(start, end)]
                else:
                    log_message("❌ 沒有來源數據可處理")
                    return
        else:
            # 智能增量處理
            processing_ranges = calculate_processing_ranges(args.symbol)
        
        if not processing_ranges:
            log_message("✅ 沒有需要處理的數據範圍")
            return
        
        total_processed = 0
        
        # 處理每個範圍
        for start_date, end_date in processing_ranges:
            log_message(f"🔄 處理範圍: {start_date} ~ {end_date}")
            
            # 獲取資金費率歷史數據
            df = get_fr_history(
                symbol=args.symbol,
                exchanges=args.exchanges,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                log_message(f"⚠️ 範圍 {start_date} ~ {end_date} 沒有數據")
                continue
            
            # 計算差異
            diff_df = calculate_funding_rate_differences_v3(df, exchange_pairs)
            
            if diff_df.empty:
                log_message(f"⚠️ 範圍 {start_date} ~ {end_date} 沒有計算出差異")
                continue
            
            # 保存到數據庫
            saved_count = save_fr_diff(diff_df)
            total_processed += saved_count
            
            log_message(f"✅ 範圍 {start_date} ~ {end_date} 處理完成: {saved_count} 條")
        
        log_message("=" * 60)
        log_message(f"🎉 V3版本處理完成！總共處理 {total_processed} 條記錄")
        log_message("=" * 60)
        
    except Exception as e:
        log_message(f"❌ 程式執行出錯: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 