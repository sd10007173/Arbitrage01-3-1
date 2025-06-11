#!/usr/bin/env python3
"""
批量導入現有的 FR_return_list CSV 文件到數據庫
解決由於之前 database_operations bug 導致的數據缺失問題
"""

import os
import pandas as pd
from database_operations import DatabaseManager
from datetime import datetime
import glob

def batch_import_csv_to_database():
    """批量導入所有 FR_return_list CSV 文件到數據庫"""
    
    print("\n" + "="*50)
    print("📊 批量導入 FR_return_list 到數據庫")
    print("="*50)
    
    project_root = os.path.dirname(os.path.abspath(__file__))
    fr_return_list_folder = os.path.join(project_root, "csv", "FR_return_list")
    
    if not os.path.exists(fr_return_list_folder):
        print(f"❌ FR_return_list 資料夾不存在: {fr_return_list_folder}")
        return
    
    # 找到所有 CSV 文件
    csv_pattern = os.path.join(fr_return_list_folder, "FR_return_list_*.csv")
    csv_files = glob.glob(csv_pattern)
    csv_files.sort()  # 按日期排序
    
    if not csv_files:
        print("❌ 沒有找到 FR_return_list CSV 文件")
        return
    
    print(f"📁 找到 {len(csv_files)} 個 CSV 文件")
    
    # 初始化數據庫
    db = DatabaseManager()
    
    # 統計
    total_files = len(csv_files)
    processed_files = 0
    total_records = 0
    failed_files = []
    
    # 欄位映射
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
    
    print(f"\n🚀 開始批量導入...")
    
    for csv_file in csv_files:
        try:
            # 從文件名提取日期
            filename = os.path.basename(csv_file)
            date_str = filename.replace('FR_return_list_', '').replace('.csv', '')
            
            print(f"📊 處理 {date_str}...")
            
            # 讀取 CSV
            daily_results = pd.read_csv(csv_file)
            
            if daily_results.empty:
                print(f"⚠️  {date_str} CSV 文件為空，跳過")
                continue
            
            # 準備數據庫數據
            db_df = daily_results.copy()
            
            # 欄位映射
            for old_col, new_col in column_mapping.items():
                if old_col in db_df.columns:
                    db_df[new_col] = db_df[old_col]
            
            # 確保有所有必需的欄位
            required_columns = ['trading_pair', 'date', 'return_1d', 'roi_1d', 'return_2d', 'roi_2d',
                               'return_7d', 'roi_7d', 'return_14d', 'roi_14d', 'return_30d', 'roi_30d',
                               'return_all', 'roi_all']
            
            for col in required_columns:
                if col not in db_df.columns:
                    db_df[col] = 0.0  # 預設值
            
            # 選擇需要的欄位
            db_df = db_df[required_columns].copy()
            
            # 插入數據庫
            inserted_count = db.insert_return_metrics(db_df)
            
            if inserted_count > 0:
                processed_files += 1
                total_records += inserted_count
                print(f"  ✅ 成功插入: {inserted_count} 條記錄")
            else:
                print(f"  ⚠️  插入失敗: 0 條記錄")
                failed_files.append((date_str, "插入返回 0"))
            
        except Exception as e:
            print(f"  ❌ 處理失敗: {e}")
            failed_files.append((date_str, str(e)))
    
    # 總結報告
    print(f"\n🎉 批量導入完成！")
    print(f"   📊 總檔案數: {total_files}")
    print(f"   ✅ 成功處理: {processed_files}")
    print(f"   📝 總記錄數: {total_records}")
    
    if failed_files:
        print(f"   ❌ 失敗檔案: {len(failed_files)}")
        for date_str, error in failed_files[:5]:  # 顯示前5個錯誤
            print(f"      - {date_str}: {error}")
        if len(failed_files) > 5:
            print(f"      ... 還有 {len(failed_files) - 5} 個失敗")
    
    # 驗證結果
    print(f"\n🔍 數據庫驗證:")
    try:
        result = pd.read_sql_query('SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as total FROM return_metrics', db.get_connection())
        print(f"   📅 日期範圍: {result.iloc[0]['min_date']} 到 {result.iloc[0]['max_date']}")
        print(f"   📊 總記錄數: {result.iloc[0]['total']}")
        
        # 檢查幾個關鍵日期
        test_dates = ['2024-01-01', '2024-02-01', '2024-03-01']
        for test_date in test_dates:
            count_result = pd.read_sql_query(f'SELECT COUNT(*) as count FROM return_metrics WHERE date = "{test_date}"', db.get_connection())
            count = count_result.iloc[0]['count']
            print(f"   📊 {test_date}: {count} 條記錄")
    
    except Exception as e:
        print(f"   ❌ 驗證失敗: {e}")

if __name__ == "__main__":
    batch_import_csv_to_database() 