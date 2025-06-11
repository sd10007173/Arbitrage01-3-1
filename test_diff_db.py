#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
from calculate_FR_diff_v1 import compute_diff_for_pair
from database_operations import DatabaseManager

def test_diff_calculation():
    """測試資金費率差異計算和數據庫插入"""
    print("🧪 測試資金費率差異計算和數據庫插入...")
    
    # 測試文件
    file_a = 'csv/FR_history/PNUTUSDT_binance_FR.csv'
    file_b = 'csv/FR_history/PNUTUSDT_bybit_FR.csv'
    
    if not (os.path.exists(file_a) and os.path.exists(file_b)):
        print("❌ 測試文件不存在")
        return
    
    print(f"📊 測試文件: {file_a} vs {file_b}")
    
    # 計算差異（只取前100行進行測試）
    try:
        # 讀取少量數據
        df_a = pd.read_csv(file_a).head(100)
        df_b = pd.read_csv(file_b).head(100)
        
        print(f"文件A: {len(df_a)} 行")
        print(f"文件B: {len(df_b)} 行")
        
        # 模擬差異計算
        merged_df = compute_diff_for_pair(file_a, file_b, "PNUTUSDT", "Binance", "Bybit")
        
        if merged_df.empty:
            print("❌ 差異計算結果為空")
            return
            
        print(f"✅ 差異計算成功: {len(merged_df)} 行")
        print("前5行數據:")
        print(merged_df.head())
        
        # 準備數據庫數據
        db_df = merged_df.copy()
        db_df['timestamp_utc'] = pd.to_datetime(db_df['Timestamp (UTC)'])
        db_df['symbol'] = db_df['Symbol']
        db_df['exchange_a'] = db_df['Exchange_A']
        db_df['exchange_b'] = db_df['Exchange_B']
        db_df['funding_rate_a'] = db_df['FundingRate_A']
        db_df['funding_rate_b'] = db_df['FundingRate_B']
        db_df['diff_ab'] = pd.to_numeric(db_df['Diff_AB'], errors='coerce')
        
        # 選擇需要的列
        db_df = db_df[['timestamp_utc', 'symbol', 'exchange_a', 'funding_rate_a', 
                       'exchange_b', 'funding_rate_b', 'diff_ab']].copy()
        
        print("準備插入數據庫的數據:")
        print(db_df.head())
        print(f"數據類型: {db_df.dtypes}")
        
        # 插入數據庫
        db = DatabaseManager()
        inserted_count = db.insert_funding_rate_diff(db_df)
        print(f"✅ 數據庫插入成功: {inserted_count} 條記錄")
        
        # 驗證插入
        result = db.get_funding_rate_diff(symbol="PNUTUSDT")
        print(f"✅ 數據庫查詢驗證: {len(result)} 條記錄")
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_diff_calculation() 