"""
CSV 到 SQLite 數據庫遷移工具
將現有的 CSV 文件數據遷移到 SQLite 數據庫中
"""

import os
import pandas as pd
import glob
from database_operations import DatabaseManager
from datetime import datetime
import re
from pathlib import Path

class CSVMigrator:
    """CSV 數據遷移管理器"""
    
    def __init__(self, db_path="data/funding_rate.db"):
        self.db = DatabaseManager(db_path)
        self.csv_base_path = "csv"
        self.migration_log = []
    
    def migrate_all_data(self):
        """遷移所有 CSV 數據到數據庫"""
        print("🚀 開始 CSV 到 SQLite 數據庫遷移...")
        
        # 1. 遷移資金費率歷史數據
        self.migrate_funding_rate_history()
        
        # 2. 遷移資金費率差異數據
        self.migrate_funding_rate_diff()
        
        # 3. 遷移收益指標數據
        self.migrate_return_metrics()
        
        # 4. 遷移策略排行榜數據
        self.migrate_strategy_rankings()
        
        # 顯示遷移總結
        self.show_migration_summary()
    
    def migrate_funding_rate_history(self):
        """遷移資金費率歷史數據"""
        print("\n📊 遷移資金費率歷史數據...")
        
        fr_history_path = os.path.join(self.csv_base_path, "FR_history")
        if not os.path.exists(fr_history_path):
            print(f"⚠️ 目錄不存在: {fr_history_path}")
            return
        
        # 搜索所有子目錄中的資金費率文件
        csv_files = glob.glob(os.path.join(fr_history_path, "**", "*_FR.csv"), recursive=True)
        total_records = 0
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if not df.empty:
                    records_inserted = self.db.insert_funding_rate_history(df)
                    total_records += records_inserted
                    
                    self.migration_log.append({
                        'table': 'funding_rate_history',
                        'file': os.path.basename(csv_file),
                        'records': records_inserted,
                        'status': 'success'
                    })
                else:
                    print(f"⚠️ 空文件: {csv_file}")
                    
            except Exception as e:
                print(f"❌ 錯誤處理文件 {csv_file}: {e}")
                self.migration_log.append({
                    'table': 'funding_rate_history',
                    'file': os.path.basename(csv_file),
                    'records': 0,
                    'status': f'error: {e}'
                })
        
        print(f"✅ 資金費率歷史數據遷移完成: 總共 {total_records} 條記錄")
    
    def migrate_funding_rate_diff(self):
        """遷移資金費率差異數據"""
        print("\n📈 遷移資金費率差異數據...")
        
        fr_diff_path = os.path.join(self.csv_base_path, "FR_diff")
        if not os.path.exists(fr_diff_path):
            print(f"⚠️ 目錄不存在: {fr_diff_path}")
            return
        
        csv_files = glob.glob(os.path.join(fr_diff_path, "**", "*_FR_diff.csv"), recursive=True)
        total_records = 0
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if not df.empty:
                    records_inserted = self.db.insert_funding_rate_diff(df)
                    total_records += records_inserted
                    
                    self.migration_log.append({
                        'table': 'funding_rate_diff',
                        'file': os.path.basename(csv_file),
                        'records': records_inserted,
                        'status': 'success'
                    })
                    
            except Exception as e:
                print(f"❌ 錯誤處理文件 {csv_file}: {e}")
                self.migration_log.append({
                    'table': 'funding_rate_diff',
                    'file': os.path.basename(csv_file),
                    'records': 0,
                    'status': f'error: {e}'
                })
        
        print(f"✅ 資金費率差異數據遷移完成: 總共 {total_records} 條記錄")
    
    def migrate_return_metrics(self):
        """遷移收益指標數據"""
        print("\n💰 遷移收益指標數據...")
        
        fr_return_path = os.path.join(self.csv_base_path, "FR_return_list")
        if not os.path.exists(fr_return_path):
            print(f"⚠️ 目錄不存在: {fr_return_path}")
            return
        
        csv_files = glob.glob(os.path.join(fr_return_path, "*.csv"))
        total_records = 0
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if not df.empty:
                    records_inserted = self.db.insert_return_metrics(df)
                    total_records += records_inserted
                    
                    self.migration_log.append({
                        'table': 'return_metrics',
                        'file': os.path.basename(csv_file),
                        'records': records_inserted,
                        'status': 'success'
                    })
                    
            except Exception as e:
                print(f"❌ 錯誤處理文件 {csv_file}: {e}")
                self.migration_log.append({
                    'table': 'return_metrics',
                    'file': os.path.basename(csv_file),
                    'records': 0,
                    'status': f'error: {e}'
                })
        
        print(f"✅ 收益指標數據遷移完成: 總共 {total_records} 條記錄")
    
    def migrate_strategy_rankings(self):
        """遷移策略排行榜數據"""
        print("\n🏆 遷移策略排行榜數據...")
        
        strategy_ranking_path = os.path.join(self.csv_base_path, "strategy_ranking")
        if not os.path.exists(strategy_ranking_path):
            print(f"⚠️ 目錄不存在: {strategy_ranking_path}")
            return
        
        csv_files = glob.glob(os.path.join(strategy_ranking_path, "**", "*_ranking_*.csv"), recursive=True)
        total_records = 0
        
        for csv_file in csv_files:
            try:
                # 從文件名提取策略名稱
                filename = os.path.basename(csv_file)
                strategy_name = self.extract_strategy_name(filename)
                
                df = pd.read_csv(csv_file)
                if not df.empty:
                    records_inserted = self.db.insert_strategy_ranking(df, strategy_name)
                    total_records += records_inserted
                    
                    self.migration_log.append({
                        'table': 'strategy_ranking',
                        'file': filename,
                        'strategy': strategy_name,
                        'records': records_inserted,
                        'status': 'success'
                    })
                    
            except Exception as e:
                print(f"❌ 錯誤處理文件 {csv_file}: {e}")
                self.migration_log.append({
                    'table': 'strategy_ranking',
                    'file': os.path.basename(csv_file),
                    'records': 0,
                    'status': f'error: {e}'
                })
        
        print(f"✅ 策略排行榜數據遷移完成: 總共 {total_records} 條記錄")
    
    def extract_strategy_name(self, filename: str) -> str:
        """從文件名提取策略名稱"""
        # 文件名格式: {strategy_name}_ranking_{date}.csv
        # 例如: original_ranking_2025-06-07.csv
        
        if "_ranking_" in filename:
            return filename.split("_ranking_")[0]
        else:
            # 如果無法提取，返回默認值
            return "unknown_strategy"
    
    def show_migration_summary(self):
        """顯示遷移總結"""
        print("\n" + "="*60)
        print("📋 數據遷移總結報告")
        print("="*60)
        
        # 按表分組統計
        table_stats = {}
        for log in self.migration_log:
            table = log['table']
            if table not in table_stats:
                table_stats[table] = {'files': 0, 'records': 0, 'errors': 0}
            
            table_stats[table]['files'] += 1
            table_stats[table]['records'] += log['records']
            if 'error' in str(log['status']):
                table_stats[table]['errors'] += 1
        
        # 顯示統計結果
        for table, stats in table_stats.items():
            success_rate = ((stats['files'] - stats['errors']) / stats['files'] * 100) if stats['files'] > 0 else 0
            print(f"\n📊 {table}:")
            print(f"  - 處理文件: {stats['files']} 個")
            print(f"  - 成功記錄: {stats['records']} 條")
            print(f"  - 錯誤文件: {stats['errors']} 個")
            print(f"  - 成功率: {success_rate:.1f}%")
        
        # 顯示數據庫最終狀態
        print("\n📈 數據庫最終狀態:")
        db_info = self.db.get_database_info()
        for table, count in db_info['tables'].items():
            print(f"  {table}: {count} 條記錄")
        
        # 保存遷移日誌
        self.save_migration_log()
    
    def save_migration_log(self):
        """保存遷移日誌到文件"""
        log_df = pd.DataFrame(self.migration_log)
        log_filename = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        log_path = os.path.join("data", log_filename)
        
        # 確保目錄存在
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        log_df.to_csv(log_path, index=False)
        print(f"\n📝 遷移日誌已保存: {log_path}")
    
    def migrate_specific_table(self, table_type: str):
        """遷移特定類型的數據"""
        if table_type == "funding_history":
            self.migrate_funding_rate_history()
        elif table_type == "funding_diff":
            self.migrate_funding_rate_diff()
        elif table_type == "return_metrics":
            self.migrate_return_metrics()
        elif table_type == "strategy_ranking":
            self.migrate_strategy_rankings()
        else:
            print(f"❌ 未知的表類型: {table_type}")
    
    def verify_migration(self):
        """驗證遷移結果"""
        print("\n🔍 驗證遷移結果...")
        
        db_info = self.db.get_database_info()
        
        # 檢查每個表是否有數據
        empty_tables = []
        for table, count in db_info['tables'].items():
            if count == 0:
                empty_tables.append(table)
        
        if empty_tables:
            print("⚠️ 以下表為空:")
            for table in empty_tables:
                print(f"  - {table}")
        else:
            print("✅ 所有表都有數據")
        
        # 檢查數據一致性（示例）
        try:
            # 檢查是否有策略排行榜數據
            ranking_sample = self.db.get_strategy_ranking('original', top_n=5)
            if not ranking_sample.empty:
                print(f"✅ 策略排行榜數據檢查通過: 找到 {len(ranking_sample)} 條 original 策略記錄")
            else:
                print("⚠️ 未找到 original 策略排行榜數據")
                
        except Exception as e:
            print(f"⚠️ 數據驗證時出錯: {e}")

def main():
    """主函數：執行完整的遷移流程"""
    print("🎯 CSV 到 SQLite 數據庫遷移工具")
    print("="*50)
    
    # 創建遷移器
    migrator = CSVMigrator()
    
    # 選擇遷移模式
    print("\n請選擇遷移模式:")
    print("1. 遷移所有數據 (推薦)")
    print("2. 僅遷移資金費率歷史數據")
    print("3. 僅遷移資金費率差異數據")
    print("4. 僅遷移收益指標數據")
    print("5. 僅遷移策略排行榜數據")
    print("6. 驗證現有數據")
    
    try:
        choice = input("\n請輸入選項 (1-6): ").strip()
        
        if choice == "1":
            migrator.migrate_all_data()
            migrator.verify_migration()
        elif choice == "2":
            migrator.migrate_specific_table("funding_history")
        elif choice == "3":
            migrator.migrate_specific_table("funding_diff")
        elif choice == "4":
            migrator.migrate_specific_table("return_metrics")
        elif choice == "5":
            migrator.migrate_specific_table("strategy_ranking")
        elif choice == "6":
            migrator.verify_migration()
        else:
            print("❌ 無效選項")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ 遷移被用戶中斷")
    except Exception as e:
        print(f"\n❌ 遷移過程中出錯: {e}")

if __name__ == "__main__":
    main() 