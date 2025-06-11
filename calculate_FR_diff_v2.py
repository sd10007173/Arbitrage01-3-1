#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
資金費率差異計算模組 - SQL優化版本 v2
從數據庫讀取funding_rate_history數據，計算交易所間差異
輸出到數據庫: funding_rate_diff表

=== 性能優化 ===
1. 使用SQL JOIN代替Python循環
2. 批量處理減少數據庫查詢
3. 向量化操作
"""

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
# 3. SQL優化版本：計算資金費率差異
# --------------------------------------
def calculate_funding_rate_differences_sql_optimized(symbol=None, exchanges=None, start_date=None, end_date=None, exchange_pairs=None):
    """
    SQL優化版本：一次性計算所有交易所對的資金費率差異
    Args:
        symbol: 交易對符號，None表示所有
        exchanges: 交易所列表，None表示所有
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        exchange_pairs: 交易所對列表，如[('binance', 'bybit')]
    Returns:
        DataFrame: 包含所有差異數據
    """
    try:
        db = DatabaseManager()
        
        log_message("🚀 SQL優化版本：計算資金費率差異...")
        log_message(f"參數: symbol={symbol}, exchanges={exchanges}")
        log_message(f"日期範圍: {start_date} 到 {end_date}")
        
        # 構建查詢條件
        where_conditions = []
        params = []
        
        if symbol:
            where_conditions.append("a.symbol = ?")
            params.append(symbol)
        
        if start_date:
            where_conditions.append("DATE(a.timestamp_utc) >= ?")
            params.append(start_date)
        
        if end_date:
            where_conditions.append("DATE(a.timestamp_utc) <= ?")
            params.append(end_date)
        
        # 如果指定了交易所，添加交易所過濾條件
        if exchanges:
            exchange_placeholders = ','.join(['?' for _ in exchanges])
            where_conditions.append(f"a.exchange IN ({exchange_placeholders})")
            where_conditions.append(f"b.exchange IN ({exchange_placeholders})")
            params.extend(exchanges)
            params.extend(exchanges)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # SQL優化版本：使用自連接(self-join)一次性計算所有交易所對的差異
        query = f"""
        WITH funding_data AS (
            -- 第一步：獲取基礎數據
            SELECT 
                timestamp_utc,
                symbol,
                exchange,
                COALESCE(funding_rate, 0.0) as funding_rate
            FROM funding_rate_history
            {where_clause.replace('a.', '').replace('b.', '') if where_clause else ''}
        )
        -- 第二步：使用自連接計算所有交易所對的差異
        SELECT 
            a.timestamp_utc,
            a.symbol,
            a.exchange as exchange_a,
            b.exchange as exchange_b,
            a.funding_rate as funding_rate_a,
            b.funding_rate as funding_rate_b,
            (a.funding_rate - b.funding_rate) as diff_ab
        FROM funding_data a
        INNER JOIN funding_data b 
            ON a.timestamp_utc = b.timestamp_utc 
            AND a.symbol = b.symbol 
            AND a.exchange < b.exchange  -- 避免重複組合 (如避免同時有 binance-bybit 和 bybit-binance)
        ORDER BY a.symbol, a.timestamp_utc, a.exchange, b.exchange
        """
        
        # 如果指定了特定的交易所對，需要修改查詢
        if exchange_pairs:
            log_message(f"指定交易所對: {exchange_pairs}")
            
            # 為每個交易所對生成UNION查詢
            union_queries = []
            union_params = []
            
            for exchange_a, exchange_b in exchange_pairs:
                pair_where_conditions = where_conditions.copy()
                pair_params = params.copy()
                
                # 移除原有的交易所過濾條件，添加特定交易所對條件
                # 重新構建不包含交易所過濾的where條件
                pair_where_conditions = []
                pair_params = []
                
                if symbol:
                    pair_where_conditions.append("a.symbol = ?")
                    pair_params.append(symbol)
                
                if start_date:
                    pair_where_conditions.append("DATE(a.timestamp_utc) >= ?")
                    pair_params.append(start_date)
                
                if end_date:
                    pair_where_conditions.append("DATE(a.timestamp_utc) <= ?")
                    pair_params.append(end_date)
                
                # 添加特定交易所對條件
                pair_where_conditions.append("a.exchange = ?")
                pair_where_conditions.append("b.exchange = ?")
                pair_params.extend([exchange_a, exchange_b])
                
                pair_where_clause = "WHERE " + " AND ".join(pair_where_conditions) if pair_where_conditions else ""
                
                pair_query = f"""
                SELECT 
                    a.timestamp_utc,
                    a.symbol,
                    a.exchange as exchange_a,
                    b.exchange as exchange_b,
                    COALESCE(a.funding_rate, 0.0) as funding_rate_a,
                    COALESCE(b.funding_rate, 0.0) as funding_rate_b,
                    (COALESCE(a.funding_rate, 0.0) - COALESCE(b.funding_rate, 0.0)) as diff_ab
                FROM funding_rate_history a
                INNER JOIN funding_rate_history b 
                    ON a.timestamp_utc = b.timestamp_utc 
                    AND a.symbol = b.symbol
                {pair_where_clause}
                """
                
                union_queries.append(pair_query)
                union_params.extend(pair_params)
            
            # 合併所有查詢
            final_query = " UNION ALL ".join(union_queries) + " ORDER BY 2, 1, 3, 4"  # 使用位置索引避免列名歧義
            final_params = union_params
        else:
            final_query = query
            final_params = params
        
        log_message("🔄 執行SQL查詢中...")
        log_message(f"📊 查詢參數數量: {len(final_params)}")
        
        with db.get_connection() as conn:
            results_df = pd.read_sql_query(final_query, conn, params=final_params)
        
        if results_df.empty:
            log_message("⚠️ SQL查詢沒有返回任何結果")
            return pd.DataFrame()
        
        log_message(f"✅ SQL優化計算完成!")
        log_message(f"   📊 差異記錄: {len(results_df)} 條")
        log_message(f"   📅 時間範圍: {results_df['timestamp_utc'].min()} 到 {results_df['timestamp_utc'].max()}")
        log_message(f"   🔗 交易對數量: {results_df['symbol'].nunique()}")
        log_message(f"   🏢 交易所對數量: {len(results_df[['exchange_a', 'exchange_b']].drop_duplicates())}")
        
        return results_df
        
    except Exception as e:
        log_message(f"❌ SQL優化計算時出錯: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# --------------------------------------
# 4. 舊版從數據庫讀取資金費率歷史數據 (保留向後兼容)
# --------------------------------------
def read_funding_rate_history_from_database(symbol=None, exchanges=None, start_date=None, end_date=None):
    """
    從數據庫讀取資金費率歷史數據 (舊版本，保留向後兼容)
    Args:
        symbol: 交易對符號，None表示所有
        exchanges: 交易所列表，None表示所有
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
    Returns:
        DataFrame with columns: timestamp_utc, symbol, exchange, funding_rate
    """
    log_message("⚠️ 使用舊版數據讀取方式，建議升級到SQL優化版本")
    
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
# 5. 舊版計算資金費率差異 (保留向後兼容)
# --------------------------------------
def calculate_funding_rate_differences(df, exchange_pairs):
    """
    計算指定交易所對之間的資金費率差異 (舊版本，保留向後兼容)
    Args:
        df: 包含資金費率數據的DataFrame
        exchange_pairs: 交易所對列表，如[('binance', 'bybit')]
    Returns:
        差異數據的DataFrame
    """
    log_message("⚠️ 使用舊版差異計算方式，建議升級到SQL優化版本")
    
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
# 7. 保存差異數據到數據庫 (優化版本)
# --------------------------------------
def save_differences_to_database_optimized(df, method='v2'):
    """
    優化版本：保存差異數據到數據庫
    Args:
        df: 包含差異數據的DataFrame  
        method: 插入方法選擇
               'v2' - 解法2：批量處理+SQLite優化 (默認)
               'v1' - 解法1：向量化處理
               'legacy' - 舊版：逐行處理
    Returns:
        bool: 是否成功保存
    """
    if df.empty:
        log_message("⚠️ 沒有數據需要保存")
        return False
    
    method_descriptions = {
        'v2': '解法2：批量處理+SQLite優化',
        'v1': '解法1：向量化處理', 
        'legacy': '舊版：逐行處理'
    }
    
    log_message(f"💾 開始保存 {len(df)} 條差異記錄到數據庫...")
    log_message(f"🔧 使用方法: {method_descriptions.get(method, method)}")
    
    try:
        db = DatabaseManager()

        # 準備數據庫格式的數據
        db_df = df.copy()

        # 確保必要的列存在並格式正確
        required_columns = ['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'funding_rate_a', 'funding_rate_b',
                            'diff_ab']
        for col in required_columns:
            if col not in db_df.columns:
                log_message(f"❌ 缺少必要列: {col}")
                return False

        # 轉換時間戳格式
        db_df['timestamp_utc'] = pd.to_datetime(db_df['timestamp_utc'])

        # 記錄開始時間
        import time
        start_time = time.time()
        
        # 選擇插入方法
        if method == 'v2':
            # 解法2：批量處理+SQLite優化
            inserted_count = db.insert_funding_rate_diff(db_df)
        elif method == 'v1':
            # 解法1：向量化處理
            inserted_count = db.insert_funding_rate_diff_v1(db_df)
        elif method == 'legacy':
            # 舊版：逐行處理
            inserted_count = db.insert_funding_rate_diff_legacy(db_df)
        else:
            log_message(f"❌ 未知的插入方法: {method}")
            return False
        
        # 計算耗時
        elapsed_time = time.time() - start_time
        
        method_name = method_descriptions.get(method, method)
        log_message(f"✅ {method_name}完成: {inserted_count:,} 條記錄")
        log_message(f"⏱️ 插入耗時: {elapsed_time:.2f} 秒")
        if elapsed_time > 0:
            log_message(f"📊 插入速度: {inserted_count/elapsed_time:,.0f} 條/秒")
        
        return True

    except Exception as e:
        log_message(f"❌ 保存差異數據到數據庫時出錯: {e}")
        import traceback
        traceback.print_exc()
        return False


# --------------------------------------
# 8. 保存差異數據到數據庫 (舊版本 - 保留用於對比)
# --------------------------------------
def save_differences_to_database(df):
    """
    舊版本：保存差異數據到數據庫 (保留用於性能對比)
    Args:
        df: 包含差異數據的DataFrame
    Returns:
        int: 保存的記錄數量
    """
    if df.empty:
        log_message("⚠️ 沒有數據需要保存")
        return 0
    
    log_message(f"💾 開始保存 {len(df)} 條差異記錄到數據庫 (舊版方法)...")
    
    try:
        db = DatabaseManager()

        # 準備數據庫格式的數據
        db_df = df.copy()

        # 確保必要的列存在並格式正確
        required_columns = ['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'funding_rate_a', 'funding_rate_b',
                            'diff_ab']
        for col in required_columns:
            if col not in db_df.columns:
                log_message(f"❌ 缺少必要列: {col}")
                return 0

        # 轉換時間戳格式
        db_df['timestamp_utc'] = pd.to_datetime(db_df['timestamp_utc'])

        # 記錄開始時間並使用舊版插入方法
        import time
        start_time = time.time()
        
        inserted_count = db.insert_funding_rate_diff_legacy(db_df)
        
        elapsed_time = time.time() - start_time
        log_message(f"✅ 舊版插入完成: {inserted_count} 條記錄")
        log_message(f"⏱️ 插入耗時: {elapsed_time:.2f} 秒")
        log_message(f"📊 插入速度: {inserted_count/elapsed_time:.0f} 條/秒")
        
        return inserted_count

    except Exception as e:
        log_message(f"❌ 保存差異數據到數據庫時出錯: {e}")
        return 0


# --------------------------------------
# 9. 檢查已存在的差異數據
# --------------------------------------
def check_existing_diff_data(symbol=None, start_date=None, end_date=None):
    """
    檢查數據庫中已存在的差異數據
    Args:
        symbol: 交易對符號
        start_date: 開始日期
        end_date: 結束日期
    Returns:
        set: 已處理的 (symbol, date) 組合集合
    """
    log_message("🔍 檢查數據庫中已存在的差異數據...")
    
    try:
        db = DatabaseManager()
        
        # 構建查詢條件
        where_conditions = []
        params = []
        
        if symbol:
            where_conditions.append("symbol = ?")
            params.append(symbol)
        
        if start_date:
            where_conditions.append("DATE(timestamp_utc) >= ?")
            params.append(start_date)
        
        if end_date:
            where_conditions.append("DATE(timestamp_utc) <= ?")
            params.append(end_date)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # 查詢數據庫中所有不重複的 (symbol, date) 組合
        query = f"""
            SELECT DISTINCT symbol, DATE(timestamp_utc) as date 
            FROM funding_rate_diff 
            {where_clause}
            ORDER BY symbol, date
        """
        
        with db.get_connection() as conn:
            result = pd.read_sql_query(query, conn, params=params)
        
        if result.empty:
            log_message("📊 數據庫中沒有差異數據")
            return set()
        
        existing_combinations = set(zip(result['symbol'], result['date']))
        
        log_message(f"📊 數據庫中找到 {len(existing_combinations)} 個已處理的 (symbol, date) 組合")
        if existing_combinations:
            unique_symbols = len(result['symbol'].unique())
            unique_dates = len(result['date'].unique())
            log_message(f"📅 涵蓋 {unique_symbols} 個交易對, {unique_dates} 個日期")
        
        return existing_combinations
        
    except Exception as e:
        log_message(f"⚠️ 檢查數據庫時出錯: {e}")
        return set()


# --------------------------------------
# 10. 主程式 (SQL優化版本)
# --------------------------------------
def main():
    log_message("🚀 資金費率差異計算程式啟動 (SQL優化版本 v2)")
    
    parser = argparse.ArgumentParser(description="計算交易所間資金費率差異並保存到數據庫 - SQL優化版本")
    parser.add_argument("--symbol", help="指定交易對符號 (可選)")
    parser.add_argument("--start-date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--exchanges", nargs='+', default=['binance', 'bybit'],
                        help="要比較的交易所列表")
    parser.add_argument("--use-legacy", action='store_true', help="使用舊版處理方式 (不推薦)")
    parser.add_argument("--method", choices=['v2', 'v1', 'legacy'], default='v2',
                        help="選擇插入方法: v2=批量+SQLite優化(默認), v1=向量化處理, legacy=舊版逐行")
    parser.add_argument("--check-existing", action='store_true', help="檢查已存在的數據，避免重複計算")

    args = parser.parse_args()

    log_message("=" * 60)
    log_message("📅 資金費率差異計算 v2 (SQL優化版本)")
    log_message("=" * 60)
    log_message(f"參數: symbol={args.symbol}, start_date={args.start_date}, end_date={args.end_date}")
    log_message(f"交易所: {args.exchanges}")

    try:
        # 生成交易所對組合
        exchange_pairs = []
        exchanges = args.exchanges
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange_pairs.append((exchanges[i], exchanges[j]))

        log_message(f"將計算以下交易所對的差異: {exchange_pairs}")

        if args.check_existing:
            # 檢查已存在的數據
            existing_data = check_existing_diff_data(args.symbol, args.start_date, args.end_date)
            log_message(f"📊 發現 {len(existing_data)} 個已存在的數據組合")

        if args.use_legacy:
            log_message("⚠️ 使用舊版處理方式 (性能較低)")
            log_message("💡 建議移除 --use-legacy 參數以使用SQL優化版本")
            
            # 舊版處理方式
            df = read_funding_rate_history_from_database(
                symbol=args.symbol,
                exchanges=args.exchanges,
                start_date=args.start_date,
                end_date=args.end_date
            )

            if df.empty:
                log_message("❌ 沒有可用的資金費率歷史數據，程序結束")
                return

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
        else:
            log_message("🚀 使用SQL優化版本 (推薦)")
            
            # SQL優化版本：一次性計算所有差異
            diff_df = calculate_funding_rate_differences_sql_optimized(
                symbol=args.symbol,
                exchanges=args.exchanges,
                start_date=args.start_date,
                end_date=args.end_date,
                exchange_pairs=exchange_pairs
            )

            if diff_df.empty:
                log_message("❌ 沒有計算出差異數據，程序結束")
                return

            # 保存到數據庫 (可選擇插入方法)
            success = save_differences_to_database_optimized(diff_df, args.method)

            if success:
                unique_symbols = diff_df['symbol'].nunique()
                # 修復時間戳轉換問題
                diff_df['timestamp_utc'] = pd.to_datetime(diff_df['timestamp_utc'])
                unique_dates = diff_df['timestamp_utc'].dt.date.nunique()
                log_message(f"✅ SQL優化版本完成！")
                log_message(f"📊 處理統計: {len(diff_df)} 條記錄, {unique_symbols} 個交易對, {unique_dates} 天")
            else:
                log_message("❌ 保存數據失敗")

        log_message("=" * 60)

    except Exception as e:
        log_message(f"❌ 程式執行出錯: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()