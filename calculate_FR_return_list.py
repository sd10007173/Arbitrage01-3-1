#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資金費率收益計算模組
從數據庫讀取funding_rate_diff數據，計算各種時間週期的收益指標
輸出到數據庫: return_metrics表
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

def calculate_returns(df, trading_pair, target_date):
    """
    計算指定交易對在目標日期的各種收益指標
    Args:
        df: FR差異數據DataFrame
        trading_pair: 交易對名稱 (如 BTCUSDT_binance_bybit)
        target_date: 目標日期 (YYYY-MM-DD)
    Returns:
        字典包含各時間週期的收益指標
    """
    target_dt = pd.to_datetime(target_date)
    
    # 獲取目標日期及之前的所有數據
    historical_data = df[df['Timestamp (UTC)'] <= target_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)].copy()
    
    if historical_data.empty:
        return None
    
    historical_data = historical_data.sort_values('Timestamp (UTC)')
    
    if 'Diff_AB' not in historical_data.columns:
        print(f"警告: {trading_pair} 沒有 Diff_AB 列")
        return None
    
    # 添加日期列以便按日期分組
    historical_data['Date'] = historical_data['Timestamp (UTC)'].dt.date
    
    # 按日期計算每日收益：每日Diff_AB的總和
    daily_returns = historical_data.groupby('Date')['Diff_AB'].sum().reset_index()
    daily_returns = daily_returns.sort_values('Date')
    
    result = {'Trading_Pair': trading_pair, 'Date': target_date}
    
    # 獲取目標日期在daily_returns中的位置
    target_date_obj = pd.to_datetime(target_date).date()
    
    if target_date_obj not in daily_returns['Date'].values:
        # 如果目標日期沒有數據，返回0
        for period_name in ['1d', '2d', '7d', '14d', '30d', 'all']:
            result[f'{period_name}_return'] = 0.0
            result[f'{period_name}_ROI'] = 0.0
        return result
    
    # 找到目標日期的索引
    target_idx = daily_returns[daily_returns['Date'] == target_date_obj].index[0]
    
    # 計算各種時間週期的收益
    periods = {
        '1d': 1,
        '2d': 2, 
        '7d': 7,
        '14d': 14,
        '30d': 30,
        'all': len(daily_returns)
    }
    
    for period_name, days in periods.items():
        # 獲取從目標日期往前推days天的數據
        start_idx = max(0, target_idx - days + 1)
        end_idx = target_idx + 1
        
        period_data = daily_returns.iloc[start_idx:end_idx]
        
        if not period_data.empty:
            cumulative_return = period_data['Diff_AB'].sum()
            actual_days = len(period_data)
            
            # 年化收益率計算
            if actual_days > 0:
                annualized_roi = (cumulative_return / actual_days) * 365
            else:
                annualized_roi = 0.0
                
            result[f'{period_name}_return'] = cumulative_return
            result[f'{period_name}_ROI'] = annualized_roi
        else:
            result[f'{period_name}_return'] = 0.0
            result[f'{period_name}_ROI'] = 0.0
    
    return result

def process_daily_data(combined_df, target_date):
    """
    處理指定日期的數據，計算所有交易對的收益指標
    Args:
        combined_df: 合併的FR差異數據
        target_date: 目標日期 (YYYY-MM-DD)
    Returns:
        DataFrame包含所有交易對的收益指標
    """
    print(f"📊 正在處理 {target_date} 的數據...")
    
    trading_pairs = combined_df['Trading_Pair'].unique()
    print(f"   找到 {len(trading_pairs)} 個交易對")
    
    results = []
    
    for trading_pair in trading_pairs:
        pair_data = combined_df[combined_df['Trading_Pair'] == trading_pair].copy()
        result = calculate_returns(pair_data, trading_pair, target_date)
        
        if result:
            results.append(result)
    
    if results:
        results_df = pd.DataFrame(results)
        print(f"✅ 成功計算 {len(results_df)} 個交易對的收益指標")
        return results_df
    else:
        print("⚠️ 沒有計算出任何收益指標")
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

def main():
    parser = argparse.ArgumentParser(description="計算資金費率收益指標並保存到數據庫")
    parser.add_argument("--start_date", help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--symbol", help="指定交易對符號 (可選)")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("📅 資金費率收益計算 (數據庫版)")
    print("="*60)
    
    # 檢查已存在的收益數據
    existing_dates = check_existing_return_data()
    
    # 自動檢測或使用指定的日期範圍
    if args.start_date and args.end_date:
        start_time = args.start_date
        end_time = args.end_date
        print(f"📅 使用指定日期範圍: {start_time} 到 {end_time}")
    else:
        start_time, end_time = auto_detect_date_range()
        
        if start_time is None or end_time is None:
            print("❌ 無法檢測數據範圍")
            print("💡 提示:")
            print("   1. 檢查數據庫中是否有funding_rate_diff數據")
            print("   2. 或使用 --start_date 和 --end_date 參數指定範圍")
            return
    
    # 生成完整的日期範圍
    all_dates = generate_date_range(start_time, end_time)
    
    # 過濾出需要處理的新日期（排除已存在的）
    new_dates = [date for date in all_dates if date not in existing_dates]
    
    print(f"\n📊 數據分析結果:")
    print(f"   📅 完整日期範圍: {start_time} 到 {end_time} ({len(all_dates)} 天)")
    print(f"   ✅ 已處理日期: {len(existing_dates)} 天")
    print(f"   🆕 待處理日期: {len(new_dates)} 天")
    
    if not new_dates:
        print("\n🎉 所有日期都已處理完成，無需額外計算！")
        return
    
    print(f"\n🚀 開始處理...")
    if len(new_dates) <= 10:
        print(f"   新日期: {', '.join(new_dates)}")
    else:
        print(f"   新日期範圍: {new_dates[0]} 到 {new_dates[-1]}")
    
    # 從數據庫載入FR_diff資料（只載入需要的範圍）
    new_start_time = min(new_dates)
    new_end_time = max(new_dates)
    combined_df = load_fr_diff_data_from_database(new_start_time, new_end_time, args.symbol)
    
    if combined_df.empty:
        print("❌ 沒有找到有效的FR_diff數據")
        return
    
    processed_count = 0
    total_saved = 0
    
    for date in new_dates:
        print(f"\n📊 處理日期: {date}")
        
        daily_results = process_daily_data(combined_df, date)
        
        if not daily_results.empty:
            # 保存到數據庫
            saved_count = save_returns_to_database(daily_results)
            
            if saved_count > 0:
                total_saved += saved_count
                processed_count += 1
                print(f"✅ 成功處理日期 {date}: {saved_count} 條記錄")
            else:
                print(f"❌ 保存日期 {date} 失敗")
        else:
            print(f"⚠️ 日期 {date} 沒有有效數據")
    
    print(f"\n🎉 處理完成！")
    print(f"   📊 總待處理: {len(new_dates)} 個日期")
    print(f"   ✅ 成功處理: {processed_count} 個日期")
    print(f"   💾 總保存記錄: {total_saved} 條")

if __name__ == "__main__":
    main() 