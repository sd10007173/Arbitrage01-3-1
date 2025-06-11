from database_operations import DatabaseManager
import os
from datetime import datetime

def check_migration_status():
    print("🔍 檢查 CSV 遷移中斷後的狀態")
    print("=" * 60)
    
    db_path = "data/funding_rate.db"
    
    # 檢查數據庫文件
    if os.path.exists(db_path):
        file_size = os.path.getsize(db_path) / (1024**3)  # GB
        mod_time = datetime.fromtimestamp(os.path.getmtime(db_path))
        print(f"📊 數據庫文件: {file_size:.2f} GB")
        print(f"🕐 最後修改: {mod_time}")
    else:
        print("❌ 數據庫文件不存在")
        return
    
    # 檢查數據庫內容
    try:
        db = DatabaseManager()
        info = db.get_database_info()
        
        print(f"\n📋 數據庫內容統計:")
        print("-" * 40)
        
        total_records = 0
        for table, count in info['tables'].items():
            status = "✅ 有數據" if count > 0 else "⚪ 空"
            print(f"{table:<25} {count:>10,} 條 {status}")
            total_records += count
        
        print(f"\n📊 總記錄數: {total_records:,} 條")
        print(f"💾 數據庫大小: {info['db_size_mb']:.1f} MB")
        
        # 檢查最新數據的日期範圍
        print(f"\n🗓️  數據日期範圍:")
        print("-" * 40)
        
        # 檢查資金費率歷史
        if info['tables']['funding_rate_history'] > 0:
            with db.get_connection() as conn:
                date_range = conn.execute("""
                    SELECT MIN(date(timestamp)) as min_date, MAX(date(timestamp)) as max_date 
                    FROM funding_rate_history
                """).fetchone()
                print(f"資金費率歷史: {date_range[0]} 到 {date_range[1]}")
        
        # 檢查策略排行榜
        if info['tables']['strategy_ranking'] > 0:
            with db.get_connection() as conn:
                date_range = conn.execute("""
                    SELECT MIN(date) as min_date, MAX(date) as max_date 
                    FROM strategy_ranking
                """).fetchone()
                print(f"策略排行榜: {date_range[0]} 到 {date_range[1]}")
            
    except Exception as e:
        print(f"❌ 檢查數據庫時發生錯誤: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 建議的下一步行動:")
    print("1. 如果數據看起來正確 → 可以繼續使用")
    print("2. 如果數據不完整 → 可以重新遷移")
    print("3. 如果想要完整遷移 → 刪除數據庫重新開始")

if __name__ == "__main__":
    check_migration_status() 