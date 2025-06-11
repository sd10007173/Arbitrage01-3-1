"""
測試數據庫遷移功能
"""

from csv_to_database_migration import CSVMigrator
import os
import pandas as pd

def test_single_file_import():
    """測試單個文件導入"""
    print('🔍 測試導入單個資金費率文件...')
    
    # 創建測試遷移器
    migrator = CSVMigrator()
    
    # 找一個測試文件
    test_file = 'csv/FR_history/2025-06-09_backup/1INCHUSDT_binance_FR.csv'
    if os.path.exists(test_file):
        df = pd.read_csv(test_file)
        print(f'✅ 找到測試文件: {test_file}')
        print(f'📊 文件包含 {len(df)} 條記錄')
        print('📋 文件欄位:', df.columns.tolist())
        print('📄 文件前5行:')
        print(df.head())
        
        # 測試導入
        result = migrator.db.insert_funding_rate_history(df)
        print(f'✅ 成功導入 {result} 條記錄')
        
        # 驗證導入
        db_info = migrator.db.get_database_info()
        print('\n📊 數據庫狀態:')
        for table, count in db_info['tables'].items():
            print(f'  {table}: {count} 條記錄')
    else:
        print(f'❌ 測試文件不存在: {test_file}')

if __name__ == "__main__":
    test_single_file_import() 