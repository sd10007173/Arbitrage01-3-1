#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🧪 測試修復後的數據庫插入功能
"""

import pandas as pd
import os
from database_operations import DatabaseManager

def test_database_commit():
    """測試數據庫事務提交"""
    print("🧪 測試數據庫事務提交修復...")
    print("="*50)
    
    # 1. 檢查當前數據庫狀態
    db = DatabaseManager()
    info = db.get_database_info()
    
    print("📊 當前數據庫狀態:")
    for table, count in info['tables'].items():
        status = "✅ 有數據" if count > 0 else "⚪ 空"
        print(f"{table:<25} {count:>10,} 條 {status}")
    
    # 2. 創建測試數據
    test_data = pd.DataFrame({
        'timestamp_utc': ['2025-01-01 00:00:00', '2025-01-01 01:00:00'],
        'symbol': ['TESTUSDT', 'TESTUSDT'],
        'exchange_a': ['binance', 'binance'],
        'funding_rate_a': ['0.001', '0.002'],
        'exchange_b': ['bybit', 'bybit'],
        'funding_rate_b': ['0.0015', '0.0025'],
        'diff_ab': [-0.0005, -0.0005]
    })
    
    print(f"\n🔧 插入測試數據: {len(test_data)} 條記錄")
    print("測試數據:")
    print(test_data)
    
    # 3. 插入測試數據
    try:
        inserted_count = db.insert_funding_rate_diff(test_data)
        print(f"✅ 插入成功: {inserted_count} 條記錄")
        
        # 4. 立即查詢驗證
        verify_data = db.get_funding_rate_diff(symbol='TESTUSDT')
        print(f"🔍 查詢驗證: 找到 {len(verify_data)} 條 TESTUSDT 記錄")
        
        if len(verify_data) > 0:
            print("✅ 數據庫事務提交成功！")
            print("查詢到的數據:")
            print(verify_data[['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'diff_ab']])
        else:
            print("❌ 數據庫事務提交失敗！查詢不到數據")
        
        # 5. 清理測試數據
        print("\n🧹 清理測試數據...")
        with db.get_connection() as conn:
            conn.execute("DELETE FROM funding_rate_diff WHERE symbol = 'TESTUSDT'")
            conn.commit()
        print("✅ 測試數據已清理")
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()

def check_existing_diff_data():
    """檢查現有的差異數據"""
    print("\n🔍 檢查現有的資金費率差異數據...")
    print("="*50)
    
    db = DatabaseManager()
    
    # 查詢所有數據
    all_diff_data = db.get_funding_rate_diff()
    print(f"📊 funding_rate_diff 表總記錄數: {len(all_diff_data)}")
    
    if len(all_diff_data) > 0:
        print("✅ 找到差異數據！前 5 條記錄:")
        display_df = all_diff_data.head()[['timestamp_utc', 'symbol', 'exchange_a', 'exchange_b', 'diff_ab']]
        print(display_df.to_string(index=False))
        
        # 統計各交易對數據量
        symbol_counts = all_diff_data['symbol'].value_counts()
        print(f"\n📈 各交易對數據統計 (前 10 個):")
        for symbol, count in symbol_counts.head(10).items():
            print(f"  {symbol:<15} {count:>8,} 條")
    else:
        print("❌ 沒有找到任何資金費率差異數據")

def main():
    """主函數"""
    print("🔧 測試數據庫修復效果")
    print("="*60)
    
    # 1. 測試數據庫事務提交
    test_database_commit()
    
    # 2. 檢查現有數據
    check_existing_diff_data()

if __name__ == "__main__":
    main() 