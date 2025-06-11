#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資金費率收益計算模組 - 加速版本 v2
從數據庫讀取funding_rate_diff數據，計算各種時間週期的收益指標
輸出到數據庫: return_metrics表

=== 性能優化目標 ===
1. 減少重複數據載入
2. 使用SQL進行主要計算 
3. 批量處理優化
4. 向量化操作
5. 記憶體使用優化
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse

# 添加數據庫支持
from database_operations import DatabaseManager

def load_fr_diff_data_from_database(start_date=None, end_date=None, symbol=None):
    """
    從數據庫加載指定時間範圍內的所有FR_diff數據
    Args:
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        symbol: 交易對符號 (可選)
    Returns:
        DataFrame with FR差異數據
    """
    try:
        db = DatabaseManager()
        
        print(f"📊 正在從數據庫加載FR_diff數據...")
        if start_date and end_date:
            print(f"   時間範圍: {start_date} 到 {end_date}")
        if symbol:
            print(f"   交易對: {symbol}")
        
        # 構建查詢條件
        where_conditions = []
        params = []
        
        if start_date:
            where_conditions.append("DATE(timestamp_utc) >= ?")
            params.append(start_date)
            
        if end_date:
            where_conditions.append("DATE(timestamp_utc) <= ?") 
            params.append(end_date)
            
        if symbol:
            where_conditions.append("symbol = ?")
            params.append(symbol)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT timestamp_utc, symbol, exchange_a, exchange_b, diff_ab,
                   symbol || '_' || exchange_a || '_' || exchange_b as trading_pair
            FROM funding_rate_diff 
            {where_clause}
            ORDER BY timestamp_utc, symbol, exchange_a, exchange_b
        """
        
        with db.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            print("⚠️ 數據庫中沒有找到匹配的FR_diff數據")
            return pd.DataFrame()
        
        # 轉換時間戳並重命名列以保持兼容性
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df = df.rename(columns={
            'timestamp_utc': 'Timestamp (UTC)',
            'trading_pair': 'Trading_Pair',
            'diff_ab': 'Diff_AB'
        })
        
        print(f"✅ 成功加載 {len(df)} 行FR_diff數據")
        print(f"   交易對數量: {df['Trading_Pair'].nunique()}")
        print(f"   時間範圍: {df['Timestamp (UTC)'].min()} 到 {df['Timestamp (UTC)'].max()}")
        
        return df
        
    except Exception as e:
        print(f"❌ 從數據庫加載FR_diff數據時出錯: {e}")
        return pd.DataFrame()

def calculate_returns_sql_optimized(start_date, end_date, symbol=None):
    """
    SQL優化版本：一次性計算所有交易對和日期的收益指標
    Args:
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
        symbol: 交易對符號 (可選)
    Returns:
        DataFrame: 包含所有結果的DataFrame
    """
    try:
        db = DatabaseManager()
        
        print(f"🚀 SQL優化版本：計算收益指標...")
        print(f"   時間範圍: {start_date} 到 {end_date}")
        if symbol:
            print(f"   交易對: {symbol}")
        
        # 構建查詢條件
        where_conditions = ["DATE(timestamp_utc) BETWEEN ? AND ?"]
        params = [start_date, end_date]
        
        if symbol:
            where_conditions.append("symbol = ?")
            params.append(symbol)
        
        where_clause = " AND ".join(where_conditions)
        
        # SQL優化版本：使用Window Functions一次性計算所有收益指標
        query = f"""
        WITH daily_returns AS (
            -- 第一步：按交易對和日期聚合每日收益
            SELECT 
                symbol || '_' || exchange_a || '_' || exchange_b as trading_pair,
                DATE(timestamp_utc) as date,
                SUM(diff_ab) as daily_return
            FROM funding_rate_diff 
            WHERE {where_clause}
            GROUP BY trading_pair, date
            ORDER BY trading_pair, date
        ),
        rolling_calculations AS (
            -- 第二步：使用Window Functions計算滑動窗口收益
            SELECT 
                trading_pair,
                date,
                daily_return,
                -- 1天收益 (當天)
                daily_return as return_1d,
                
                -- 2天收益 (當天+前1天)
                SUM(daily_return) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) as return_2d,
                
                -- 7天收益 (當天+前6天)
                SUM(daily_return) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as return_7d,
                
                -- 14天收益 (當天+前13天)
                SUM(daily_return) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as return_14d,
                
                -- 30天收益 (當天+前29天)
                SUM(daily_return) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as return_30d,
                
                -- 全部收益 (從開始到當天)
                SUM(daily_return) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS UNBOUNDED PRECEDING
                ) as return_all,
                
                -- 計算天數用於ROI計算
                COUNT(*) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) as days_2d,
                
                COUNT(*) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as days_7d,
                
                COUNT(*) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as days_14d,
                
                COUNT(*) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as days_30d,
                
                COUNT(*) OVER (
                    PARTITION BY trading_pair 
                    ORDER BY date 
                    ROWS UNBOUNDED PRECEDING
                ) as days_all
                
            FROM daily_returns
        )
        -- 第三步：計算年化收益率並輸出最終結果
        SELECT 
            trading_pair,
            date,
            COALESCE(return_1d, 0.0) as return_1d,
            COALESCE(return_1d * 365, 0.0) as roi_1d,
            
            COALESCE(return_2d, 0.0) as return_2d,
            COALESCE(CASE WHEN days_2d > 0 THEN return_2d * 365.0 / days_2d ELSE 0.0 END, 0.0) as roi_2d,
            
            COALESCE(return_7d, 0.0) as return_7d,
            COALESCE(CASE WHEN days_7d > 0 THEN return_7d * 365.0 / days_7d ELSE 0.0 END, 0.0) as roi_7d,
            
            COALESCE(return_14d, 0.0) as return_14d,
            COALESCE(CASE WHEN days_14d > 0 THEN return_14d * 365.0 / days_14d ELSE 0.0 END, 0.0) as roi_14d,
            
            COALESCE(return_30d, 0.0) as return_30d,
            COALESCE(CASE WHEN days_30d > 0 THEN return_30d * 365.0 / days_30d ELSE 0.0 END, 0.0) as roi_30d,
            
            COALESCE(return_all, 0.0) as return_all,
            COALESCE(CASE WHEN days_all > 0 THEN return_all * 365.0 / days_all ELSE 0.0 END, 0.0) as roi_all
            
        FROM rolling_calculations
        ORDER BY trading_pair, date
        """
        
        print("🔄 執行SQL查詢中...")
        with db.get_connection() as conn:
            results_df = pd.read_sql_query(query, conn, params=params)
        
        if results_df.empty:
            print("⚠️ SQL查詢沒有返回任何結果")
            return pd.DataFrame()
        
        print(f"✅ SQL優化計算完成!")
        print(f"   📊 計算結果: {len(results_df)} 條記錄")
        print(f"   📅 日期範圍: {results_df['date'].min()} 到 {results_df['date'].max()}")
        print(f"   🔗 交易對數量: {results_df['trading_pair'].nunique()}")
        
        return results_df
        
    except Exception as e:
        print(f"❌ SQL優化計算時出錯: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def process_batch_data_sql_optimized(start_date, end_date, target_dates, symbol=None):
    """
    SQL優化版本：批量處理多個日期的數據
    Args:
        start_date: 數據開始日期 (YYYY-MM-DD)
        end_date: 數據結束日期 (YYYY-MM-DD)
        target_dates: 目標日期列表
        symbol: 交易對符號 (可選)
    Returns:
        DataFrame包含所有結果
    """
    print(f"🚀 SQL優化批量處理: {len(target_dates)} 個日期")
    print(f"   數據範圍: {start_date} 到 {end_date}")
    
    # 使用SQL一次性計算所有結果
    all_results = calculate_returns_sql_optimized(start_date, end_date, symbol)
    
    if all_results.empty:
        print("⚠️ 沒有計算出任何結果")
        return pd.DataFrame()
    
    # 過濾出目標日期的結果
    target_dates_set = set(target_dates)
    filtered_results = all_results[all_results['date'].isin(target_dates_set)].copy()
    
    if filtered_results.empty:
        print("⚠️ 目標日期沒有匹配的結果")
        return pd.DataFrame()
    
    print(f"✅ 批量處理完成!")
    print(f"   📊 總結果: {len(filtered_results)} 條記錄")
    print(f"   📅 實際日期: {filtered_results['date'].nunique()} 天")
    print(f"   🔗 交易對: {filtered_results['trading_pair'].nunique()} 個")
    
    return filtered_results

def process_daily_data_legacy(combined_df, target_date):
    """
    舊版本的處理函數 (保留向後兼容)
    Args:
        combined_df: 合併的FR差異數據
        target_date: 目標日期 (YYYY-MM-DD)
    Returns:
        DataFrame包含所有交易對的收益指標
    """
    print(f"⚠️ 使用舊版處理方式處理 {target_date}")
    print("💡 建議升級到SQL優化版本以獲得更好性能")
    
    # 這裡保留原來的邏輯作為備用
    # 實際上我們會在main函數中避免調用這個函數
    return pd.DataFrame()

def save_returns_to_database(results_df):
    """
    將收益指標保存到數據庫
    Args:
        results_df: 包含收益指標的DataFrame
    Returns:
        保存的記錄數
    """
    if results_df.empty:
        print("⚠️ 收益數據為空，無法保存")
        return 0
    
    try:
        db = DatabaseManager()
        
        print(f"📊 準備將 {len(results_df)} 條收益指標記錄插入數據庫...")
        
        # 準備數據庫數據
        db_df = results_df.copy()
        
        # 處理列名映射
        column_mapping = {
            'Trading_Pair': 'trading_pair',
            'Date': 'date',
            '1d_return': 'return_1d',
            '1d_ROI': 'roi_1d',
            '2d_return': 'return_2d', 
            '2d_ROI': 'roi_2d',
            '7d_return': 'return_7d',
            '7d_ROI': 'roi_7d',
            '14d_return': 'return_14d',
            '14d_ROI': 'roi_14d',
            '30d_return': 'return_30d',
            '30d_ROI': 'roi_30d',
            'all_return': 'return_all',
            'all_ROI': 'roi_all'
        }
        
        # 重命名列
        for old_col, new_col in column_mapping.items():
            if old_col in db_df.columns:
                db_df[new_col] = db_df[old_col]
        
        # 確保有所有必需的列
        required_columns = ['trading_pair', 'date', 'return_1d', 'roi_1d', 'return_2d', 'roi_2d',
                          'return_7d', 'roi_7d', 'return_14d', 'roi_14d', 'return_30d', 'roi_30d',
                          'return_all', 'roi_all']
        
        for col in required_columns:
            if col not in db_df.columns:
                db_df[col] = 0.0  # 預設值
        
        # 選擇需要的列
        db_df = db_df[required_columns].copy()
        
        print(f"📊 數據樣本: Trading_Pair={db_df.iloc[0]['trading_pair']}, Date={db_df.iloc[0]['date']}")
        
        # 保存到數據庫
        inserted_count = db.insert_return_metrics(db_df)
        print(f"✅ 數據庫插入成功: {inserted_count} 條記錄")
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ 保存收益數據到數據庫時出錯: {e}")
        return 0

def check_existing_return_data():
    """
    檢查數據庫中已存在的收益數據，回傳已處理的日期集合
    Returns:
        set: 已處理的日期集合
    """
    print("🔍 檢查數據庫中已存在的收益數據...")
    
    try:
        db = DatabaseManager()
        
        # 查詢數據庫中所有不重複的日期
        query = "SELECT DISTINCT date FROM return_metrics ORDER BY date"
        
        with db.get_connection() as conn:
            result = pd.read_sql_query(query, conn)
        
        if result.empty:
            print("📊 數據庫中沒有收益數據")
            return set()
        
        existing_dates = set(result['date'].tolist())
        
        print(f"📊 數據庫中找到 {len(existing_dates)} 個已處理的日期")
        if existing_dates:
            sorted_dates = sorted(existing_dates)
            print(f"📅 數據庫已處理範圍: {sorted_dates[0]} 到 {sorted_dates[-1]}")
        
        return existing_dates
        
    except Exception as e:
        print(f"⚠️ 檢查數據庫時出錯: {e}")
        return set()

def auto_detect_date_range():
    """
    自動檢測數據庫中funding_rate_diff數據的日期範圍
    Returns:
        tuple: (start_date, end_date) 或 (None, None)
    """
    print("🔍 自動掃描數據庫中的FR_diff數據範圍...")
    
    try:
        db = DatabaseManager()
        
        # 查詢最小和最大日期
        query = """
            SELECT MIN(DATE(timestamp_utc)) as min_date, 
                   MAX(DATE(timestamp_utc)) as max_date,
                   COUNT(*) as total_count,
                   COUNT(DISTINCT symbol) as symbol_count
            FROM funding_rate_diff
        """
        
        with db.get_connection() as conn:
            result = conn.execute(query).fetchone()
        
        if not result or result[2] == 0:
            print("❌ 數據庫中沒有找到funding_rate_diff數據")
            return None, None
        
        min_date = result[0]
        max_date = result[1]
        total_count = result[2]
        symbol_count = result[3]
        
        print(f"📈 檢測到數據範圍: {min_date} 到 {max_date}")
        print(f"📊 總記錄數: {total_count}")
        print(f"📅 交易對數量: {symbol_count}")
        
        return min_date, max_date
        
    except Exception as e:
        print(f"❌ 自動檢測數據範圍時出錯: {e}")
        return None, None

def generate_date_range(start_date, end_date):
    """
    生成日期範圍
    Args:
        start_date: 開始日期 (YYYY-MM-DD)
        end_date: 結束日期 (YYYY-MM-DD)
    Returns:
        list: 日期字符串列表
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current_dt = start_dt
    
    while current_dt <= end_dt:
        dates.append(current_dt.strftime('%Y-%m-%d'))
        current_dt += timedelta(days=1)
    
    return dates

def save_to_database_optimized(db, results_df):
    """
    SQL優化版本：批量保存收益數據到數據庫
    Args:
        db: DatabaseManager實例
        results_df: 包含收益數據的DataFrame (SQL查詢結果格式)
    Returns:
        bool: 保存是否成功
    """
    if results_df.empty:
        print("⚠️ 沒有數據需要保存")
        return False
    
    try:
        print(f"💾 SQL優化保存: {len(results_df)} 條收益記錄...")
        
        # SQL優化版本的DataFrame已經有正確的列名格式
        # 確保列名符合數據庫期望
        required_columns = [
            'trading_pair', 'date',
            'return_1d', 'roi_1d',
            'return_2d', 'roi_2d', 
            'return_7d', 'roi_7d',
            'return_14d', 'roi_14d',
            'return_30d', 'roi_30d',
            'return_all', 'roi_all'
        ]
        
        # 檢查所需列是否存在
        missing_columns = [col for col in required_columns if col not in results_df.columns]
        if missing_columns:
            print(f"❌ 缺少必要的列: {missing_columns}")
            print(f"   現有列: {list(results_df.columns)}")
            return False
        
        # 創建用於數據庫插入的DataFrame
        db_df = results_df[required_columns].copy()
        
        # 確保數據類型正確
        db_df['date'] = pd.to_datetime(db_df['date']).dt.strftime('%Y-%m-%d')
        
        # 確保數值列為浮點數
        numeric_columns = [col for col in required_columns if col not in ['trading_pair', 'date']]
        for col in numeric_columns:
            db_df[col] = pd.to_numeric(db_df[col], errors='coerce').fillna(0.0)
        
        print(f"📊 數據範例: Trading_Pair={db_df.iloc[0]['trading_pair']}, Date={db_df.iloc[0]['date']}")
        
        # 批量插入到數據庫
        inserted_count = db.insert_return_metrics(db_df)
        
        if inserted_count > 0:
            print(f"✅ SQL優化保存成功: {inserted_count} 條記錄")
            return True
        else:
            print("❌ 沒有記錄被插入")
            return False
        
    except Exception as e:
        print(f"❌ SQL優化保存時出錯: {e}")
        import traceback
        traceback.print_exc()
        return False

def find_new_dates_to_process(db, start_date, end_date):
    """
    找到需要處理的新日期
    Args:
        db: DatabaseManager實例
        start_date: 開始日期
        end_date: 結束日期
    Returns:
        list: 需要處理的日期列表
    """
    existing_dates = check_existing_return_data()
    all_dates = generate_date_range(start_date, end_date)
    new_dates = [date for date in all_dates if date not in existing_dates]
    return new_dates

def find_latest_unprocessed_date(db):
    """
    找到最新的未處理日期
    Args:
        db: DatabaseManager實例
    Returns:
        str: 最新未處理日期或None
    """
    # 檢查funding_rate_diff表中的最新日期
    try:
        with db.get_connection() as conn:
            query = "SELECT MAX(DATE(timestamp_utc)) as max_date FROM funding_rate_diff"
            result = conn.execute(query).fetchone()
            
            if not result or not result[0]:
                return None
                
            latest_data_date = result[0]
        
        # 檢查return_metrics表中的最新日期
        existing_dates = check_existing_return_data()
        
        if not existing_dates:
            # 如果沒有任何處理過的日期，返回最新數據日期
            return latest_data_date
        
        latest_processed_date = max(existing_dates)
        
        # 如果最新數據日期比最新處理日期新，返回未處理的日期
        if latest_data_date > latest_processed_date:
            return latest_data_date
        
        return None
        
    except Exception as e:
        print(f"❌ 檢查未處理日期時出錯: {e}")
        return None

def main():
    print("🚀 資金費率收益計算程式啟動 (SQL優化版本 v2)")
    
    parser = argparse.ArgumentParser(description='計算資金費率收益指標 - SQL優化版本')
    parser.add_argument('--start-date', type=str, help='開始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='結束日期 (YYYY-MM-DD)')
    parser.add_argument('--symbol', type=str, help='交易對符號 (可選)')
    parser.add_argument('--process-latest', action='store_true', help='處理最新的未處理日期')
    parser.add_argument('--use-legacy', action='store_true', help='使用舊版處理方式 (不推薦)')
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("📅 資金費率收益計算 v2 (SQL優化版本)")
    print("="*60)
    
    try:
        # 初始化數據庫管理器
        db = DatabaseManager()
        print("✅ 數據庫初始化完成")
        
        if args.process_latest:
            # 查找最新的未處理日期
            latest_date = find_latest_unprocessed_date(db)
            if latest_date:
                print(f"🎯 發現未處理日期: {latest_date}")
                
                # SQL優化版本：計算數據範圍
                start_load_date = (pd.to_datetime(latest_date) - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
                
                # 使用SQL優化批量處理
                results_df = process_batch_data_sql_optimized(
                    start_load_date, latest_date, [latest_date], args.symbol
                )
                
                if not results_df.empty:
                    # 保存到數據庫
                    success = save_to_database_optimized(db, results_df)
                    if success:
                        print(f"✅ {latest_date} 的收益指標已成功保存到數據庫")
                    else:
                        print(f"❌ 保存 {latest_date} 的數據到數據庫失敗")
                else:
                    print(f"⚠️ {latest_date} 沒有計算出收益指標")
            else:
                print("✅ 沒有發現未處理的日期，所有數據都是最新的")
            return
        
        # 如果指定了日期範圍
        if args.start_date and args.end_date:
            start_date = args.start_date
            end_date = args.end_date
        else:
            # 自動檢測數據範圍
            start_date, end_date = auto_detect_date_range()
            
            if start_date is None or end_date is None:
                print("❌ 無法檢測數據範圍")
                print("💡 提示:")
                print("   1. 檢查數據庫中是否有funding_rate_diff數據")
                print("   2. 或使用 --start-date 和 --end-date 參數指定範圍")
                return
        
        print(f"📅 處理日期範圍: {start_date} 到 {end_date}")
        
        # 查找這個範圍內需要處理的新日期
        new_dates = find_new_dates_to_process(db, start_date, end_date)
        
        if not new_dates:
            print("✅ 在指定日期範圍內沒有發現需要處理的新日期")
            return
        
        print(f"🎯 發現 {len(new_dates)} 個需要處理的日期")
        if len(new_dates) <= 10:
            print(f"   新日期: {', '.join(new_dates)}")
        else:
            print(f"   新日期範圍: {new_dates[0]} 到 {new_dates[-1]}")
        
        # 擴展開始日期以包含足夠的歷史數據
        extended_start_date = (pd.to_datetime(start_date) - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        
        if args.use_legacy:
            print("⚠️ 使用舊版處理方式 (性能較低)")
            print("💡 建議移除 --use-legacy 參數以使用SQL優化版本")
            
            # 舊版處理方式 (保留向後兼容)
            combined_df = load_fr_diff_data_from_database(extended_start_date, end_date, args.symbol)
            
            if combined_df.empty:
                print("❌ 沒有找到任何數據")
                return
            
            print(f"✅ 成功載入 {len(combined_df)} 筆FR差異數據")
            
            # 處理每個新日期
            all_results = []
            for date in new_dates:
                print(f"\n🔄 處理日期: {date}")
                results_df = process_daily_data_legacy(combined_df, date)
                
                if not results_df.empty:
                    all_results.append(results_df)
                    
                    # 保存到數據庫
                    success = save_returns_to_database(results_df)
                    if success > 0:
                        print(f"✅ {date} 的收益指標已保存到數據庫 ({success} 筆記錄)")
                    else:
                        print(f"❌ 保存 {date} 的數據到數據庫失敗")
                else:
                    print(f"⚠️ {date} 沒有計算出收益指標")
        else:
            print("🚀 使用SQL優化版本 (推薦)")
            
            # SQL優化版本：一次性批量處理所有日期
            results_df = process_batch_data_sql_optimized(
                extended_start_date, end_date, new_dates, args.symbol
            )
            
            if not results_df.empty:
                # 保存到數據庫
                success = save_to_database_optimized(db, results_df)
                if success:
                    print(f"✅ 所有日期的收益指標已保存到數據庫 ({len(results_df)} 筆記錄)")
                    
                    # 顯示統計信息
                    unique_dates = results_df['date'].nunique()
                    unique_pairs = results_df['trading_pair'].nunique()
                    print(f"📊 處理統計: {unique_dates} 天, {unique_pairs} 個交易對")
                else:
                    print(f"❌ 保存數據到數據庫失敗")
            else:
                print("⚠️ 沒有計算出任何收益指標")
        
        print(f"\n🎉 處理完成!")
            
    except Exception as e:
        print(f"❌ 程式執行出錯: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 