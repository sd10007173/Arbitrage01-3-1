"""
演示 SQLite 數據庫遷移功能
只遷移少量樣本數據來演示整個流程
"""

from csv_to_database_migration import CSVMigrator
import os
import pandas as pd
import glob

def demo_migration():
    """演示遷移流程"""
    print("🎯 SQLite 數據庫遷移演示")
    print("="*50)
    
    # 創建遷移器
    migrator = CSVMigrator()
    
    # 1. 遷移一個資金費率歷史文件
    print("\n📊 Step 1: 遷移資金費率歷史數據 (樣本)")
    test_fr_file = 'csv/FR_history/2025-06-09_backup/AAVEUSDT_binance_FR.csv'
    if os.path.exists(test_fr_file):
        df = pd.read_csv(test_fr_file)
        print(f"  - 文件: {test_fr_file}")
        print(f"  - 記錄數: {len(df)}")
        result = migrator.db.insert_funding_rate_history(df)
        print(f"  - 導入成功: {result} 條記錄")
    
    # 2. 遷移一個收益指標文件
    print("\n💰 Step 2: 遷移收益指標數據 (樣本)")
    test_return_file = 'csv/FR_return_list/2-25-06-09_backup/FR_return_list_2024-01-01.csv'
    if os.path.exists(test_return_file):
        df = pd.read_csv(test_return_file)
        print(f"  - 文件: {test_return_file}")
        print(f"  - 記錄數: {len(df)}")
        result = migrator.db.insert_return_metrics(df)
        print(f"  - 導入成功: {result} 條記錄")
    
    # 3. 遷移一個策略排行榜文件
    print("\n🏆 Step 3: 遷移策略排行榜數據 (樣本)")
    test_ranking_file = 'csv/strategy_ranking/original_ranking_2024-01-01.csv'
    if os.path.exists(test_ranking_file):
        df = pd.read_csv(test_ranking_file)
        print(f"  - 文件: {test_ranking_file}")
        print(f"  - 記錄數: {len(df)}")
        result = migrator.db.insert_strategy_ranking(df, 'original')
        print(f"  - 導入成功: {result} 條記錄")
    
    # 4. 顯示數據庫狀態
    print("\n📊 Step 4: 數據庫最終狀態")
    db_info = migrator.db.get_database_info()
    for table, count in db_info['tables'].items():
        print(f"  {table}: {count} 條記錄")
    
    # 5. 測試查詢功能
    print("\n🔍 Step 5: 測試查詢功能")
    
    # 查詢策略排行榜
    try:
        ranking = migrator.db.get_strategy_ranking('original', top_n=5)
        if not ranking.empty:
            print(f"  ✅ 策略排行榜查詢成功: 找到 {len(ranking)} 條記錄")
            print("  前5名交易對:")
            for i, row in ranking.iterrows():
                if i < 5:  # 只顯示前5名
                    print(f"    {row.get('rank_position', 'N/A')}. {row.get('trading_pair', 'N/A')} - 得分: {row.get('final_ranking_score', 'N/A'):.4f}")
        else:
            print("  ⚠️ 策略排行榜查詢結果為空")
    except Exception as e:
        print(f"  ❌ 策略排行榜查詢失敗: {e}")
    
    # 查詢資金費率歷史
    try:
        # 使用 pandas 直接查詢，因為我們的 get 方法可能需要參數
        with migrator.db.get_connection() as conn:
            history = pd.read_sql_query(
                "SELECT symbol, exchange, COUNT(*) as records FROM funding_rate_history GROUP BY symbol, exchange LIMIT 5", 
                conn
            )
            
        if not history.empty:
            print(f"  ✅ 資金費率歷史查詢成功:")
            for _, row in history.iterrows():
                print(f"    {row['symbol']} ({row['exchange']}): {row['records']} 條記錄")
        else:
            print("  ⚠️ 資金費率歷史查詢結果為空")
    except Exception as e:
        print(f"  ❌ 資金費率歷史查詢失敗: {e}")
    
    print("\n✅ 演示完成！")
    print("\n🎉 恭喜！您的 SQLite 數據庫遷移功能已成功實現！")
    
    # 顯示接下來的步驟
    print("\n📋 接下來您可以:")
    print("1. 運行 `python csv_to_database_migration.py` 遷移所有數據")
    print("2. 修改現有程式使用數據庫而不是 CSV")
    print("3. 享受更快的查詢速度和更好的數據管理！")

if __name__ == "__main__":
    demo_migration() 